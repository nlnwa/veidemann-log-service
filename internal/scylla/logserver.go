package scylla

import (
	"fmt"
	"github.com/gocql/gocql"
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

type logServer struct {
	// logServer is a scylladb client
	*Client
	logV1.UnimplementedLogServer

	// metric channels
	crawlLogMetric chan *logV1.CrawlLog
	pageLogMetric  chan *logV1.PageLog

	// prepared queries
	writeCrawlLog              *gocqlx.Queryx
	writePageLog               *gocqlx.Queryx
	listCrawlLogsByWarcId      *gocqlx.Queryx
	listPageLogsByWarcId       *gocqlx.Queryx
	listCrawlLogsByExecutionId *gocqlx.Queryx
	listPageLogsByExecutionId  *gocqlx.Queryx
}

// New creates a new client with the specified address and apiKey.
func New(options Options) *logServer {
	crawlLogMetric := make(chan *logV1.CrawlLog, 100)
	pageLogMetric := make(chan *logV1.PageLog, 100)

	go func() {
		for crawlLog := range crawlLogMetric {
			CollectCrawlLog(crawlLog)
		}
	}()
	go func() {
		for pageLog := range pageLogMetric {
			CollectPageLog(pageLog)
		}
	}()

	return &logServer{
		Client: &Client{
			config: createCluster(gocql.LocalQuorum, options.Keyspace, options.Hosts...),
		},
		crawlLogMetric: crawlLogMetric,
		pageLogMetric:  pageLogMetric,
	}
}

// Connect connects to a scylladb cluster.
func (l *logServer) Connect() error {
	err := l.Client.Connect()
	if err != nil {
		return err
	}

	// setup prepared queries
	l.writeCrawlLog = qb.Insert("crawl_log").Json().Query(l.session)
	l.writePageLog = qb.Insert("page_log").Json().Query(l.session)
	l.listPageLogsByExecutionId = qb.Select("page_log").Where(qb.Eq("execution_id")).Query(l.session)
	l.listCrawlLogsByExecutionId = qb.Select("crawl_log").Where(qb.Eq("execution_id")).Query(l.session)
	l.listCrawlLogsByWarcId = qb.Select("crawl_log").Where(qb.Eq("warc_id")).Query(l.session)
	l.listPageLogsByWarcId = qb.Select("page_log").Where(qb.Eq("warc_id")).Query(l.session)

	return nil
}

// Close closes the connection to database session
func (l *logServer) Close() {
	l.writeCrawlLog.Release()
	l.writePageLog.Release()
	l.listCrawlLogsByWarcId.Release()
	l.listPageLogsByWarcId.Release()
	l.listCrawlLogsByExecutionId.Release()
	l.listPageLogsByExecutionId.Release()
	l.session.Close()

	close(l.crawlLogMetric)
	close(l.pageLogMetric)
}

func (l *logServer) WriteCrawlLog(stream logV1.Log_WriteCrawlLogServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		cl := req.GetCrawlLog()
		l.crawlLogMetric <- cl
		if err := writeCrawlLog(l.writeCrawlLog.WithContext(stream.Context()), cl); err != nil {
			return err
		}
	}
}

func writeCrawlLog(query *gocqlx.Queryx, crawlLog *logV1.CrawlLog) error {
	// Generate nanosecond timestamp with millisecond precision.
	// (ScyllaDB does not allow storing timestamps with better than millisecond precision)
	ns := (time.Now().UnixNano() / 1e6) * 1e6
	crawlLog.TimeStamp = timestamppb.New(time.Unix(0, ns))
	// Convert fetchstimestamp to have millisecond precision
	crawlLog.FetchTimeStamp.Nanos = crawlLog.FetchTimeStamp.Nanos - (crawlLog.FetchTimeStamp.Nanos % 1e6)

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

func (l *logServer) WritePageLog(stream logV1.Log_WritePageLogServer) error {
	pageLog := &logV1.PageLog{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if err := writePageLog(l.writePageLog.WithContext(stream.Context()), pageLog); err != nil {
				return err
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

func (l *logServer) ListPageLogs(req *logV1.PageLogListRequest, stream logV1.Log_ListPageLogsServer) error {
	if len(req.GetWarcId()) > 0 {
		return listPageLogsByWarcId(l.listPageLogsByWarcId.WithContext(stream.Context()), req, stream.Send)
	}
	if len(req.GetQueryTemplate().GetExecutionId()) > 0 {
		return listPageLogsByExecutionId(l.listPageLogsByExecutionId.WithContext(stream.Context()), req, stream.Send)
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

func (l *logServer) ListCrawlLogs(req *logV1.CrawlLogListRequest, stream logV1.Log_ListCrawlLogsServer) error {
	if len(req.GetWarcId()) > 0 {
		return listCrawlLogsByWarcId(l.listCrawlLogsByWarcId.WithContext(stream.Context()), req, stream.Send)
	}
	if len(req.GetQueryTemplate().GetExecutionId()) > 0 {
		return listCrawlLogsByExecutionId(l.listCrawlLogsByExecutionId.WithContext(stream.Context()), req, stream.Send)
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
