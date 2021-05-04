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
	"fmt"
	"github.com/nlnwa/veidemann-api/go/commons/v1"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"time"
)

type ErrorUDT struct {
	gocqlx.UDT
	Code   int32  `cql:"code"`
	Msg    string `cql:"msg"`
	Detail string `cql:"detail"`
}

func (e *ErrorUDT) toProto() *commons.Error {
	if e != nil {
		return &commons.Error{
			Code:   e.Code,
			Msg:    e.Msg,
			Detail: e.Detail,
		}
	}
	return nil
}

type CrawlLog struct {
	WarcId              string    `cql:"warc_id"`
	TimeStamp           time.Time `cql:"time_stamp"`
	StatusCode          int32     `cql:"status_code"`
	Size                int64     `cql:"size"`
	RequestedUri        string    `cql:"requested_uri"`
	ResponseUri         string    `cql:"response_uri"`
	DiscoveryPath       string    `cql:"discovery_path"`
	Referrer            string    `cql:"referrer"`
	ContentType         string    `cql:"content_type"`
	FetchTimeStamp      time.Time `cql:"fetch_time_stamp"`
	FetchTimeMs         int64     `cql:"fetch_time_ms"`
	BlockDigest         string    `cql:"block_digest"`
	PayloadDigest       string    `cql:"payload_digest"`
	StorageRef          string    `cql:"storage_ref"`
	RecordType          string    `cql:"record_type"`
	WarcRefersTo        string    `cql:"warc_refers_to"`
	IpAddress           string    `cql:"ip_address"`
	ExecutionId         string    `cql:"execution_id"`
	Retries             int32     `cql:"retries"`
	Error               *ErrorUDT `cql:"error"`
	JobExecutionId      string    `cql:"job_execution_id"`
	CollectionFinalName string    `cql:"collection_final_name"`
	Method              string    `cql:"method"`
}

func (c *CrawlLog) toProto() *logV1.CrawlLog {
	return &logV1.CrawlLog{
		WarcId:              c.WarcId,
		TimeStamp:           timestamppb.New(c.TimeStamp),
		StatusCode:          c.StatusCode,
		Size:                c.Size,
		RequestedUri:        c.RequestedUri,
		ResponseUri:         c.ResponseUri,
		DiscoveryPath:       c.DiscoveryPath,
		Referrer:            c.Referrer,
		ContentType:         c.ContentType,
		FetchTimeStamp:      timestamppb.New(c.FetchTimeStamp),
		FetchTimeMs:         c.FetchTimeMs,
		BlockDigest:         c.BlockDigest,
		PayloadDigest:       c.PayloadDigest,
		StorageRef:          c.StorageRef,
		RecordType:          c.RecordType,
		WarcRefersTo:        c.WarcRefersTo,
		IpAddress:           c.IpAddress,
		ExecutionId:         c.ExecutionId,
		Retries:             c.Retries,
		Error:               c.Error.toProto(),
		JobExecutionId:      c.JobExecutionId,
		CollectionFinalName: c.CollectionFinalName,
		Method:              c.Method,
	}
}

type ResourceUDT struct {
	gocqlx.UDT
	Uri           string    `cql:"uri"`
	FromCache     bool      `cql:"from_cache"`
	Renderable    bool      `cql:"renderable"`
	ResourceType  string    `cql:"resource_type"`
	ContentType   string    `cql:"content_type"`
	StatusCode    int32     `cql:"status_code"`
	DiscoveryPath string    `cql:"discovery_path"`
	WarcId        string    `cql:"warc_id"`
	Referrer      string    `cql:"referrer"`
	Error         *ErrorUDT `cql:"error"`
	Method        string    `cql:"method"`
}

func (r *ResourceUDT) toProto() *logV1.PageLog_Resource {
	return &logV1.PageLog_Resource{
		Uri:           r.Uri,
		FromCache:     r.FromCache,
		Renderable:    r.Renderable,
		ResourceType:  r.ResourceType,
		ContentType:   r.ContentType,
		StatusCode:    r.StatusCode,
		DiscoveryPath: r.DiscoveryPath,
		WarcId:        r.WarcId,
		Referrer:      r.Referrer,
		Error:         r.Error.toProto(),
		Method:        r.Method,
	}
}

type PageLog struct {
	WarcId              string         `cql:"warc_id"`
	Uri                 string         `cql:"uri"`
	ExecutionId         string         `cql:"execution_id"`
	Referrer            string         `cql:"referrer"`
	JobExecutionId      string         `cql:"job_execution_id"`
	CollectionFinalName string         `cql:"collection_final_name"`
	Method              string         `cql:"method"`
	Resource            []*ResourceUDT `cql:"resource"`
	Outlink             []string       `cql:"outlink"`
}

func (p *PageLog) toProto() *logV1.PageLog {
	var resource []*logV1.PageLog_Resource
	for _, r := range p.Resource {
		resource = append(resource, r.toProto())
	}
	return &logV1.PageLog{
		WarcId:              p.WarcId,
		Uri:                 p.Uri,
		ExecutionId:         p.ExecutionId,
		Referrer:            p.Referrer,
		JobExecutionId:      p.JobExecutionId,
		CollectionFinalName: p.CollectionFinalName,
		Method:              p.Method,
		Resource:            resource,
		Outlink:             p.Outlink,
	}
}

type LogServer struct {
	logV1.UnimplementedLogServer

	// pool of prepared queries
	insertCrawlLog             *Pool
	insertPageLog              *Pool
	listCrawlLogsByWarcId      *Pool
	listPageLogsByWarcId       *Pool
	listCrawlLogsByExecutionId *Pool
	listPageLogsByExecutionId  *Pool
}

func New(session gocqlx.Session, readPoolSize int, writePoolSize int) *LogServer {
	return &LogServer{
		insertCrawlLog: NewPool(writePoolSize, func() *gocqlx.Queryx {
			return qb.Insert("crawl_log").Json().Query(session)
		}),
		insertPageLog: NewPool(writePoolSize, func() *gocqlx.Queryx {
			return qb.Insert("page_log").Json().Query(session)
		}),
		listPageLogsByExecutionId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select("page_log").Where(qb.Eq("execution_id")).Query(session)
		}),
		listCrawlLogsByExecutionId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select("crawl_log").Where(qb.Eq("execution_id")).Query(session)
		}),
		listPageLogsByWarcId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select("page_log").Where(qb.Eq("warc_id")).Query(session)
		}),
		listCrawlLogsByWarcId: NewPool(readPoolSize, func() *gocqlx.Queryx {
			return qb.Select("crawl_log").Where(qb.Eq("warc_id")).Query(session)
		}),
	}
}

// Close drains the query pools.
func (l *LogServer) Close() {
	l.insertCrawlLog.Drain()
	l.insertPageLog.Drain()
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
	pageLog := &logV1.PageLog{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			CollectPageLog(pageLog)
			if err := writePageLog(q, pageLog); err != nil {
				return fmt.Errorf("error writing page log: %w", err)
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

func (l *LogServer) ListPageLogs(req *logV1.PageLogListRequest, stream logV1.Log_ListPageLogsServer) error {
	if len(req.GetWarcId()) > 0 {
		q := l.listPageLogsByWarcId.Borrow()
		defer l.listPageLogsByWarcId.Return(q)
		return listPageLogsByWarcId(q.WithContext(stream.Context()), req, stream.Send)
	}
	if len(req.GetQueryTemplate().GetExecutionId()) > 0 {
		q := l.listPageLogsByExecutionId.Borrow()
		defer l.listPageLogsByExecutionId.Return(q)
		return listPageLogsByExecutionId(q.WithContext(stream.Context()), req, stream.Send)
	}
	return fmt.Errorf("request must provide warcId or executionId")
}

// listPageLogsByWarcId lists page logs by warcId
func listPageLogsByWarcId(query *gocqlx.Queryx, req *logV1.PageLogListRequest, fn func(*logV1.PageLog) error) error {
	for _, warcId := range req.GetWarcId() {
		pageLog := new(PageLog)
		if err := query.BindMap(qb.M{"warc_id": warcId}).Get(pageLog); err != nil {
			return err
		}
		if err := fn(pageLog.toProto()); err != nil {
			return err
		}
	}
	return nil
}

func listPageLogsByExecutionId(query *gocqlx.Queryx, req *logV1.PageLogListRequest, fn func(*logV1.PageLog) error) error {
	offset := int(req.GetOffset())
	pageSize := int(req.GetPageSize())
	executionId := req.GetQueryTemplate().GetExecutionId()

	// count is the current number of rows processed by fn
	count := 0
	iter := query.BindMap(qb.M{"execution_id": executionId}).Iter()
	for pageLog := new(PageLog); iter.StructScan(pageLog); pageLog = new(PageLog) {
		if count < offset {
			count++
			continue
		}
		err := fn(pageLog.toProto())
		if err != nil {
			_ = iter.Close()
			return err
		}
		if offset+pageSize == 0 {
			continue
		}
		count++
		// stop when number of processed rows is what we requested
		if count >= offset+pageSize {
			_ = iter.Close()
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
	for crawlLog := new(CrawlLog); iter.StructScan(crawlLog); crawlLog = new(CrawlLog) {
		if count < offset {
			count++
			continue
		}
		err := fn(crawlLog.toProto())
		if err != nil {
			_ = iter.Close()
			return err
		}
		if offset+pageSize == 0 {
			continue
		}
		count++
		// stop when number of processed rows equals what we requested
		if count >= offset+pageSize {
			_ = iter.Close()
			return nil
		}
	}
	return iter.Close()
}
