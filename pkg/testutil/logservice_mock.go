package testutil

import (
	"fmt"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"net"
	"sync"
)

// LogServiceMock is used to implement ContentWriterServer.
type LogServiceMock struct {
	logV1.UnimplementedLogServer
	*grpc.Server

	CrawlLogs []*logV1.CrawlLog
	PageLogs  []*logV1.PageLog
	cl        sync.Mutex
	pl        sync.Mutex
}

func (s *LogServiceMock) WriteCrawlLog(stream logV1.Log_WriteCrawlLogServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		s.cl.Lock()
		s.CrawlLogs = append(s.CrawlLogs, req.GetCrawlLog())
		s.cl.Unlock()
	}
	return nil
}

func (s *LogServiceMock) WritePageLog(stream logV1.Log_WritePageLogServer) error {
	pageLog := &logV1.PageLog{}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			s.pl.Lock()
			s.PageLogs = append(s.PageLogs, pageLog)
			s.pl.Unlock()
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
			pageLog.WarcId = crawlLog.WarcId
			pageLog.ExecutionId = crawlLog.ExecutionId
			pageLog.JobExecutionId = crawlLog.JobExecutionId
			pageLog.Uri = crawlLog.RequestedUri
			pageLog.Method = crawlLog.Method
			pageLog.CollectionFinalName = crawlLog.CollectionFinalName
			pageLog.Referrer = crawlLog.Referrer
		}
	}
}

func (s *LogServiceMock) Reset() {
	s.cl.Lock()
	s.pl.Lock()
	defer s.pl.Unlock()
	defer s.cl.Unlock()
	s.PageLogs = []*logV1.PageLog{}
	s.CrawlLogs = []*logV1.CrawlLog{}
}

func (s *LogServiceMock) Close() {
	s.GracefulStop()
}

// NewLogServiceMock creates a new LogServiceMock
func NewLogServiceMock(port int) *LogServiceMock {
	addr := fmt.Sprintf("localhost:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Errorf("failed to listen: %w", err))
	}
	logService := &LogServiceMock{
		Server: grpc.NewServer(),
	}
	logV1.RegisterLogServer(logService, logService)
	reflection.Register(logService.Server)
	go func() {
		_ = logService.Serve(lis)
	}()

	return logService
}
