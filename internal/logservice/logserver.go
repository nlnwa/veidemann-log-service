/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package logservice

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TableCrawlLog = "crawl_log"
	TablePageLog  = "page_log"
	TableResource = "resource"
)

type LogServer struct {
	logV1.UnimplementedLogServer

	// pool of prepared queries
	insertCrawlLog             *Pool
	insertPageLog              *Pool
	insertResource             *Pool
	listCrawlLogsByWarcId      *Pool
	listPageLogsByWarcId       *Pool
	listResourcesByPageId      *Pool
	listCrawlLogsByExecutionId *Pool
	listPageLogsByExecutionId  *Pool
}

func New(session gocqlx.Session, readPoolSize int, writePoolSize int, readConsistency gocql.Consistency) *LogServer {
	return &LogServer{
		insertCrawlLog: NewPool(writePoolSize, func() *gocqlx.Queryx {
			return qb.Insert(TableCrawlLog).Json().Query(session)
		}),
		insertPageLog: NewPool(writePoolSize, func() *gocqlx.Queryx {
			return qb.Insert(TablePageLog).Json().Query(session)
		}),
		insertResource: NewPool(writePoolSize, func() *gocqlx.Queryx {
			return qb.Insert(TableResource).Json().Query(session)
		}),
		listResourcesByPageId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select(TableResource).Where(qb.Eq("page_id")).Query(session).Consistency(readConsistency)
		}),
		listPageLogsByExecutionId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select(TablePageLog).Where(qb.Eq("execution_id")).Query(session).Consistency(readConsistency)
		}),
		listCrawlLogsByExecutionId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select(TableCrawlLog).Where(qb.Eq("execution_id")).Query(session).Consistency(readConsistency)
		}),
		listPageLogsByWarcId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select(TablePageLog).Where(qb.Eq("warc_id")).Query(session).Consistency(readConsistency)
		}),
		listCrawlLogsByWarcId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select(TableCrawlLog).Where(qb.Eq("warc_id")).Query(session).Consistency(readConsistency)
		}),
	}
}

// Close drains the query pools.
func (l *LogServer) Close() {
	l.insertCrawlLog.Drain()
	l.insertPageLog.Drain()
	l.insertResource.Drain()
	l.listResourcesByPageId.Drain()
	l.listCrawlLogsByWarcId.Drain()
	l.listPageLogsByWarcId.Drain()
	l.listCrawlLogsByExecutionId.Drain()
	l.listPageLogsByExecutionId.Drain()
}

func (l *LogServer) WriteCrawlLog(stream logV1.Log_WriteCrawlLogServer) error {
	q := l.insertCrawlLog.Borrow()
	defer l.insertCrawlLog.Return(q)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		crawlLog := req.GetCrawlLog()
		CollectCrawlLog(crawlLog)
		if err := writeCrawlLog(q, crawlLog); err != nil {
			return fmt.Errorf("error writing crawl log: %w", err)
		}
	}
}

func writeCrawlLog(query *gocqlx.Queryx, crawlLog *logV1.CrawlLog) error {
	// Generate timestamp with millisecond precision.
	// (ScyllaDB does not allow storing timestamps with better than millisecond precision)
	crawlLog.TimeStamp = timestamppb.New(time.Now().UTC().Truncate(time.Millisecond))
	// Convert FetchTimeStamp to millisecond precision
	crawlLog.FetchTimeStamp = timestamppb.New(crawlLog.FetchTimeStamp.AsTime().Truncate(time.Millisecond))

	cl, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(crawlLog)
	if err != nil {
		return err
	}

	err = query.Bind(cl).Exec()
	if err != nil {
		return err
	}
	return nil
}

func (l *LogServer) WritePageLog(stream logV1.Log_WritePageLogServer) error {
	q := l.insertPageLog.Borrow()
	defer l.insertPageLog.Return(q)
	r := l.insertResource.Borrow()
	defer l.insertResource.Return(r)
	pageLog := &logV1.PageLog{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			CollectPageLog(pageLog)
			resources := pageLog.GetResource()
			pageLog.Resource = nil
			if err := writePageLog(q, pageLog); err != nil {
				return fmt.Errorf("error writing page log: %w", err)
			}
			if err := writeResources(r, resources, pageLog.WarcId); err != nil {
				return fmt.Errorf("error writing resources: %w", err)
			}
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		switch req.Value.(type) {
		case *logV1.WritePageLogRequest_Outlink:
			pageLog.Outlink = append(pageLog.Outlink, req.GetOutlink())
		case *logV1.WritePageLogRequest_Resource:
			pageLog.Resource = append(pageLog.Resource, req.GetResource())
		case *logV1.WritePageLogRequest_CrawlLog:
			crawlLog := req.GetCrawlLog()
			pageLog.Uri = crawlLog.RequestedUri
			pageLog.ExecutionId = crawlLog.ExecutionId
			pageLog.Method = crawlLog.Method
			pageLog.CollectionFinalName = crawlLog.CollectionFinalName
			pageLog.Referrer = crawlLog.Referrer
			pageLog.JobExecutionId = crawlLog.JobExecutionId
			pageLog.WarcId = crawlLog.WarcId
		}
	}
}

func writePageLog(query *gocqlx.Queryx, pageLog *logV1.PageLog) error {
	pl, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(pageLog)
	if err != nil {
		return err
	}
	return query.Bind(pl).Exec()
}

func writeResources(q *gocqlx.Queryx, resources []*logV1.PageLog_Resource, pageId string) error {
	var firstErr error
	resource := &Resource{PageId: pageId}
	for _, r := range resources {
		if r.GetWarcId() == "" {
			// If the resource has an empty WarcId field we must generate one to conform with the schema
			r.WarcId = uuid.NewString()
		}
		j, err := json.Marshal(resource.fromProto(r))
		if err != nil {
			return err
		}
		if err := q.Bind(j).Exec(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *LogServer) ListPageLogs(req *logV1.PageLogListRequest, stream logV1.Log_ListPageLogsServer) error {
	r := l.listResourcesByPageId.Borrow()
	defer l.listResourcesByPageId.Return(r)

	if len(req.GetWarcId()) > 0 {
		q := l.listPageLogsByWarcId.Borrow()
		defer l.listPageLogsByWarcId.Return(q)
		return listPageLogsByWarcId(q.WithContext(stream.Context()), r.WithContext(stream.Context()), req, stream.Send)
	}
	if len(req.GetQueryTemplate().GetExecutionId()) > 0 {
		p := l.listPageLogsByExecutionId.Borrow()
		defer l.listPageLogsByExecutionId.Return(p)
		return listPageLogsByExecutionId(p.WithContext(stream.Context()), r.WithContext(stream.Context()), req, stream.Send)
	}
	return fmt.Errorf("request must provide warcId or executionId")
}

func listResources(q *gocqlx.Queryx, pageId string) ([]*logV1.PageLog_Resource, error) {
	var resources []*logV1.PageLog_Resource
	p := q.BindMap(qb.M{"page_id": pageId})
	iter := p.Iter()
	defer iter.Close()
	for resource := new(Resource); iter.StructScan(resource); resource = new(Resource) {
		resources = append(resources, resource.toProto())
	}
	return resources, nil
}

// listPageLogsByWarcId lists page logs by warcId
func listPageLogsByWarcId(q *gocqlx.Queryx, r *gocqlx.Queryx, req *logV1.PageLogListRequest, fn func(*logV1.PageLog) error) error {
	for _, warcId := range req.GetWarcId() {
		pageLog := new(PageLog)
		err := q.BindMap(qb.M{"warc_id": warcId}).Get(pageLog)
		if err != nil {
			return err
		}
		resources, err := listResources(r, warcId)
		if err != nil {
			return err
		}
		if err := fn(pageLog.toProto(resources)); err != nil {
			return err
		}
	}
	return nil
}

func listPageLogsByExecutionId(q *gocqlx.Queryx, r *gocqlx.Queryx, req *logV1.PageLogListRequest, fn func(*logV1.PageLog) error) error {
	offset := int(req.GetOffset())
	pageSize := int(req.GetPageSize())
	executionId := req.GetQueryTemplate().GetExecutionId()
	// count is the current number of rows processed by fn
	count := 0
	iter := q.BindMap(qb.M{"execution_id": executionId}).Iter()
	defer iter.Close()
	for pageLog := new(PageLog); iter.StructScan(pageLog); pageLog = new(PageLog) {
		if count < offset {
			count++
			continue
		}
		resources, err := listResources(r, pageLog.WarcId)
		if err != nil {
			return err
		}
		err = fn(pageLog.toProto(resources))
		if err != nil {
			return err
		}
		if offset+pageSize == 0 {
			continue
		}
		count++
		// stop when number of processed rows is what we requested
		if count >= offset+pageSize {
			return nil
		}
	}
	return iter.Close()
}

func (l *LogServer) ListCrawlLogs(req *logV1.CrawlLogListRequest, stream logV1.Log_ListCrawlLogsServer) error {
	if len(req.GetWarcId()) > 0 {
		q := l.listCrawlLogsByWarcId.Borrow()
		defer l.listCrawlLogsByWarcId.Return(q)
		return listCrawlLogsByWarcId(q.WithContext(stream.Context()), req, stream.Send)
	}
	if len(req.GetQueryTemplate().GetExecutionId()) > 0 {
		q := l.listCrawlLogsByExecutionId.Borrow()
		defer l.listCrawlLogsByExecutionId.Return(q)
		return listCrawlLogsByExecutionId(q.WithContext(stream.Context()), req, stream.Send)
	}
	return fmt.Errorf("request must provide warcId or executionId")
}

// listPageLogsByWarcId lists page logs by warcId
func listCrawlLogsByWarcId(query *gocqlx.Queryx, req *logV1.CrawlLogListRequest, fn func(*logV1.CrawlLog) error) error {
	for _, warcId := range req.GetWarcId() {
		crawlLog := new(CrawlLog)
		if err := query.BindMap(qb.M{"warc_id": warcId}).Get(crawlLog); err != nil {
			return err
		}
		if err := fn(crawlLog.toProto()); err != nil {
			return err
		}
	}
	return nil
}

func listCrawlLogsByExecutionId(query *gocqlx.Queryx, req *logV1.CrawlLogListRequest, fn func(*logV1.CrawlLog) error) error {
	offset := int(req.GetOffset())
	pageSize := int(req.GetPageSize())
	executionId := req.GetQueryTemplate().GetExecutionId()

	// count is the current number of rows processed by fn
	count := 0

	iter := query.BindMap(qb.M{"execution_id": executionId}).Iter()
	defer iter.Close()
	for crawlLog := new(CrawlLog); iter.StructScan(crawlLog); crawlLog = new(CrawlLog) {
		if count < offset {
			count++
			continue
		}
		err := fn(crawlLog.toProto())
		if err != nil {
			return err
		}
		if offset+pageSize == 0 {
			continue
		}
		count++
		// stop when number of processed rows equals what we requested
		if count >= offset+pageSize {
			return nil
		}
	}
	return iter.Close()
}
