package scylla

import (
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/ptypes"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/nlnwa/veidemann-log-service/internal/metrics"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"time"
)

type logServer struct {
	// logServer is a scylladb client
	*Client
	logV1.UnimplementedLogServer
	crawlLogMetric chan *logV1.CrawlLog
	pageLogMetric  chan *logV1.PageLog
	insertCrawlLog *gocqlx.Queryx
	insertPageLog  *gocqlx.Queryx
	selectCrawlLog *gocqlx.Queryx
	selectPageLog  *gocqlx.Queryx
}

// New creates a new client with the specified address and apiKey.
func New(options Options) *logServer {
	crawlLogMetric := make(chan *logV1.CrawlLog, 100)
	pageLogMetric := make(chan *logV1.PageLog, 100)

	go func() {
		for crawlLog := range crawlLogMetric {
			metrics.CollectCrawlLog(crawlLog)
		}
	}()
	go func() {
		for pageLog := range pageLogMetric {
			metrics.CollectPageLog(pageLog)
		}
	}()

	return &logServer{
		Client: &Client{
			config: createCluster(gocql.Quorum, options.Keyspace, options.Hosts...),
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
	l.insertCrawlLog = qb.Insert("crawl_log").Query(l.session)
	l.insertPageLog = qb.Insert("page_log").Json().Query(l.session)
	l.selectCrawlLog = qb.Select("crawl_log").Where(qb.Eq("execution_id")).Json().Query(l.session)
	l.selectPageLog = qb.Select("crawl_log").Where(qb.Eq("execution_id")).Json().Query(l.session)

	return nil
}

// Close closes the connection to database session
func (l *logServer) Close() {
	l.insertCrawlLog.Release()
	l.insertPageLog.Release()
	l.selectCrawlLog.Release()
	l.selectPageLog.Release()

	l.session.Close()

	close(l.crawlLogMetric)
	close(l.pageLogMetric)
}

func (l *logServer) WriteCrawlLog(stream logV1.Log_WriteCrawlLogServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		cl := req.GetCrawlLog()
		l.crawlLogMetric <- cl

		// Generate nanosecond timestamp with millisecond precision.
		// (ScyllaDB does not allow storing timestamps with better than millisecond precision)
		ns := (time.Now().UnixNano() / 1e6) * 1e6
		timeStamp, err := ptypes.TimestampProto(time.Unix(0, ns))
		if err != nil {
			return err
		}
		cl.TimeStamp = timeStamp

		cl.FetchTimeStamp.Nanos = cl.FetchTimeStamp.Nanos - (cl.FetchTimeStamp.Nanos % 1e6)
		crawlLog, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(cl)
		if err != nil {
			return err
		}

		err = l.insertCrawlLog.WithContext(stream.Context()).Bind(crawlLog).Exec()
		if err != nil {
			return err
		}
	}
}

func (l *logServer) WritePageLog(stream logV1.Log_WritePageLogServer) error {
	pageLog := &logV1.PageLog{}
	insert := l.insertPageLog.WithContext(stream.Context())
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			l.pageLogMetric <- pageLog
			pl, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(pageLog)
			if err != nil {
				return err
			}
			return insert.Bind(pl).Exec()
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

func (l *logServer) ListPageLogs(req *logV1.PageLogListRequest, stream logV1.Log_ListPageLogsServer) error {
	q := l.selectPageLog.
		WithContext(stream.Context()).
		BindMap(qb.M{"execution_id": req.GetQueryTemplate().GetExecutionId()})
	if req.GetPageSize() > 0 {
		q = q.PageSize(int(req.PageSize))
	}
	defer q.Release()

	iter := q.Iter()
	defer func() { _ = iter.Close() }()

	var pageLogJSON string
	for iter.Scan(pageLogJSON) {
		var pageLog *logV1.PageLog
		if err := protojson.Unmarshal([]byte(pageLogJSON), pageLog); err != nil {
			return err
		}
		err := stream.Send(pageLog)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *logServer) ListCrawlLogs(req *logV1.CrawlLogListRequest, stream logV1.Log_ListCrawlLogsServer) error {
	q := l.selectCrawlLog.
		WithContext(stream.Context()).
		BindMap(qb.M{"execution_id": req.GetQueryTemplate().GetExecutionId()})
	if req.GetPageSize() > 0 {
		q = q.PageSize(int(req.PageSize))
	}
	defer q.Release()

	iter := q.Iter()
	defer func() { _ = iter.Close() }()

	var jsonCrawlLog string
	for iter.Scan(jsonCrawlLog) {
		var crawlLog *logV1.CrawlLog
		if err := protojson.Unmarshal([]byte(jsonCrawlLog), crawlLog); err != nil {
			return err
		}
		if err := stream.Send(crawlLog); err != nil {
			return err
		}
	}
	return nil
}
