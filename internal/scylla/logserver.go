package scylla

import (
	"github.com/gocql/gocql"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	l.insertCrawlLog = qb.Insert("crawl_log").Json().Query(l.session)
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
		cl.TimeStamp = timestamppb.New(time.Unix(0, ns))
		// Convert fetchstimestamp to have millisecond precision
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
	offset := int(req.GetOffset())
	pageSize := int(req.GetPageSize())
	executionId := req.GetQueryTemplate().GetExecutionId()

	// count is the current number of rows sent to the client
	count := 0

	var pageState []byte
	for {
		q := l.selectPageLog.WithContext(stream.Context()).BindMap(qb.M{"execution_id": executionId})

		// if the requested page does not overlap or is less than default pageSize (5000)
		// set pageSize such that we fetch no more rows then needed for the request
		if pageSize+offset <= 5000 {
			q.PageSize(offset + pageSize)
		}
		iter := q.PageState(pageState).Iter()
		nextPageState := iter.PageState()
		scanner := iter.Scanner()
	out:
		for scanner.Next() {
			if count < offset {
				continue
			}
			var pageLogJSON string
			if err := scanner.Scan(&pageLogJSON); err != nil {
				_ = iter.Close()
				return err
			}
			var pageLog *logV1.PageLog
			if err := protojson.Unmarshal([]byte(pageLogJSON), pageLog); err != nil {
				_ = iter.Close()
				return err
			}
			err := stream.Send(pageLog)
			if err != nil {
				_ = iter.Close()
				return err
			}
			count++
			// break if we already sent the number of requested rows
			if count >= offset+pageSize {
				_ = iter.Close()
				break out
			}
		}
		if len(nextPageState) == 0 {
			break
		}
		pageState = nextPageState
	}
	return nil
}

func (l *logServer) ListCrawlLogs(req *logV1.CrawlLogListRequest, stream logV1.Log_ListCrawlLogsServer) error {
	offset := int(req.GetOffset())
	pageSize := int(req.GetPageSize())
	executionId := req.GetQueryTemplate().GetExecutionId()

	// count is the current number of rows sent to the client
	count := 0

	var pageState []byte
	for {
		q := l.selectCrawlLog.WithContext(stream.Context()).BindMap(qb.M{"execution_id": executionId})

		// if the requested page does not overlap or is less than default pageSize (5000)
		// set pageSize such that we fetch no more rows then needed for the request
		if pageSize+offset <= 5000 {
			q.PageSize(offset + pageSize)
		}
		iter := q.PageState(pageState).Iter()
		nextPageState := iter.PageState()
		scanner := iter.Scanner()
	out:
		for scanner.Next() {
			if count < offset {
				continue
			}
			var crawlLogJson string
			if err := scanner.Scan(&crawlLogJson); err != nil {
				_ = iter.Close()
				return err
			}
			var crawlLog *logV1.CrawlLog
			if err := protojson.Unmarshal([]byte(crawlLogJson), crawlLog); err != nil {
				_ = iter.Close()
				return err
			}
			err := stream.Send(crawlLog)
			if err != nil {
				_ = iter.Close()
				return err
			}
			count++
			// break if we already sent the number of requested rows
			if count >= offset+pageSize {
				_ = iter.Close()
				break out
			}
		}
		if len(nextPageState) == 0 {
			break
		}
		pageState = nextPageState
	}
	return nil
}
