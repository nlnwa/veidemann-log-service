package logservice

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/nlnwa/veidemann-api/go/commons/v1"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/nlnwa/veidemann-log-service/internal/scylla"
	"github.com/nlnwa/veidemann-log-service/pkg/logservice"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

var (
	session   gocqlx.Session
	logServer *LogServer
	logWriter logservice.LogWriter
)

func TestMain(m *testing.M) {
	networkName := "test"
	ctx := context.Background()

	newNetwork, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:           networkName,
			CheckDuplicate: true,
		},
	})
	if err != nil {
		panic(err)
	}

	scyllaC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "scylladb/scylla:4.4.4",
			ExposedPorts: []string{"9042/tcp", "19042/tcp"},
			Networks:     []string{networkName},
			NetworkAliases: map[string][]string{
				networkName: {"scylla"},
			},
			WaitingFor: wait.ForListeningPort("9042/tcp"),
		},
		Started: true,
	})
	if err != nil {
		panic(err)
	}

	if _, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      "ghcr.io/nlnwa/veidemann-log-schema:2.0.0",
			AutoRemove: true,
			Networks:   []string{networkName},
			Env:        map[string]string{
				"CQLSH_HOST": "scylla",
			},
			WaitingFor: wait.ForLog("Schema initialized"),
		},
		Started: true,
	}); err != nil {
		panic(err)
	}

	// wait for index to be ready
	time.Sleep(5 * time.Second)

	cqlshHost, err := scyllaC.Host(ctx)
	if err != nil {
		panic(err)
	}
	cqlshPort, err := scyllaC.MappedPort(ctx, "9042/tcp")
	if err != nil {
		panic(err)
	}

	cfg := scylla.CreateCluster(gocql.LocalQuorum, "v7n_v2_dc1", cqlshHost)
	cfg.Port = cqlshPort.Int()

	session, err = scylla.Connect(cfg)
	if err != nil {
		panic(err)
	}

	logServer = New(session, 2, 5)

	srv := grpc.NewServer()
	logV1.RegisterLogServer(srv, logServer)

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	srvPort := listener.Addr().(*net.TCPAddr).Port

	go func() {
		if err := srv.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", srvPort), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	logWriter = logservice.LogWriter{
		LogClient: logV1.NewLogClient(conn),
	}

	code := m.Run()

	_ = conn.Close()
	srv.GracefulStop()
	_ = listener.Close()
	logServer.Close()
	_ = scyllaC.Terminate(ctx)
	_ = newNetwork.Remove(ctx)

	os.Exit(code)
}

func TestWriteCrawlLogs(t *testing.T) {
	if err := truncate(CrawlLogTable); err != nil {
		t.Fatal(err)
	}
	templates := []*logV1.CrawlLog{crawlLog1, crawlLog2}
	concurrency := 10
	n := 100

	testdata := make([][]*logV1.CrawlLog, concurrency)
	for i := 0; i < concurrency; i++ {
		for j := 0; j < n; j++ {
			crawlLog := *templates[i%len(templates)]
			crawlLog.WarcId = uuid.NewString()
			testdata[i] = append(testdata[i], &crawlLog)
		}
	}
	for i, crawlLogs := range testdata {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			if err := logWriter.WriteCrawlLogs(context.Background(), crawlLogs); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestWritePageLogs(t *testing.T) {
	if err := truncate(PageLogTable); err != nil {
		t.Fatal(err)
	}
	templates := []*logV1.PageLog{pageLog1, pageLog2}
	concurrency := 50

	testdata := make([]*logV1.PageLog, concurrency)
	for i := 0; i < concurrency; i++ {
		pageLog := *templates[i%len(templates)]
		pageLog.WarcId = uuid.NewString()
		testdata = append(testdata, &pageLog)
	}
	for i, pageLog := range testdata {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			if err := logWriter.WritePageLog(context.Background(), pageLog); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestListCrawlLogs(t *testing.T) {
	if err := truncate(CrawlLogTable); err != nil {
		t.Fatal(err)
	}

	crawlLogs := []*logV1.CrawlLog{crawlLog1, crawlLog2}

	if err := logWriter.WriteCrawlLogs(context.Background(), crawlLogs); err != nil {
		t.Error(err)
	}

	count := 0

	// read and assert
	if err := listCrawlLogsByExecutionId(
		logServer.listCrawlLogsByExecutionId.Borrow(),
		&logV1.CrawlLogListRequest{
			QueryTemplate: &logV1.CrawlLog{
				ExecutionId: crawlLog1.ExecutionId,
			},
		},
		func(got *logV1.CrawlLog) error {
			count++
			var expected *logV1.CrawlLog
			for _, crawlLog := range crawlLogs {
				if crawlLog.WarcId == got.WarcId {
					expected = crawlLog
					break
				}
			}
			if expected == nil {
				t.Fatalf("Found no crawlLog matching warcId: %s", got.GetWarcId())
			}
			assertEqualCrawlLogs(t, expected, got)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if count != len(crawlLogs) {
		t.Errorf("Expected %d, got %d", len(crawlLogs), count)
	}
}

func TestListPageLog(t *testing.T) {
	if err := truncate(PageLogTable); err != nil {
		t.Fatal(err)
	}

	pageLogs := []*logV1.PageLog{pageLog1, pageLog2}

	// insert
	for _, pageLog := range pageLogs {
		if err := logWriter.WritePageLog(context.Background(), pageLog); err != nil {
			t.Error(err)
		}
	}

	// try to list a page log that does not exist
	bogus := uuid.NewString()
	if err := listPageLogsByWarcId(
		logServer.listPageLogsByWarcId.Borrow(),
		&logV1.PageLogListRequest{
			QueryTemplate: &logV1.PageLog{
				WarcId: bogus,
			},
		},
		func(got *logV1.PageLog) error {
			t.Fatalf("Expected no callback to be made for bogus warcId: %s", bogus)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	// expect to find page log with known warcId
	if err := listPageLogsByWarcId(
		logServer.listPageLogsByWarcId.Borrow(),
		&logV1.PageLogListRequest{
			QueryTemplate: &logV1.PageLog{
				WarcId: pageLog1.WarcId,
			},
		},
		func(got *logV1.PageLog) error {
			expected := pageLog1
			assertEqualPageLogs(t, expected, got)
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	count := 0

	// expect to list pagelogs with known execution id
	if err := listPageLogsByExecutionId(
		logServer.listPageLogsByExecutionId.Borrow(),
		&logV1.PageLogListRequest{
			QueryTemplate: &logV1.PageLog{
				ExecutionId: pageLog1.ExecutionId,
			},
		},
		func(got *logV1.PageLog) error {
			count++
			var expected *logV1.PageLog

			// find corresponding original pagelog
			for _, pageLog := range pageLogs {
				if pageLog.WarcId == got.WarcId {
					expected = pageLog
					break
				}
			}
			if expected == nil {
				t.Fatalf("Found no pagelog matching warcId: %s", got.GetWarcId())
			}
			assertEqualPageLogs(t, expected, got)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if count != len(pageLogs) {
		t.Errorf("Expected %d, got %d", len(pageLogs), count)
	}
}

func assertEqualPageLogs(t *testing.T, expected *logV1.PageLog, got *logV1.PageLog) {
	// convert to json for comparison
	a, err := protojson.Marshal(expected)
	if err != nil {
		t.Error(err)
	}
	b, err := protojson.Marshal(got)
	if err != nil {
		t.Error(err)
	}
	if string(a) != string(b) {
		t.Errorf("Expected:\n%s, got:\n%s", a, b)
	}
}

func assertEqualCrawlLogs(t *testing.T, expected *logV1.CrawlLog, got *logV1.CrawlLog) {
	got.TimeStamp = nil
	// convert to json for comparison
	a, err := protojson.Marshal(expected)
	if err != nil {
		t.Error(err)
	}
	b, err := protojson.Marshal(got)
	if err != nil {
		t.Error(err)
	}
	if string(a) != string(b) {
		t.Errorf("Expected:\n%s, got:\n%s", a, b)
	}
}

// truncate truncates the contents of given table and waits for it to take effect.
func truncate(table string) error {
	if err := session.ExecStmt(fmt.Sprintf("TRUNCATE %s", table)); err != nil {
		return err
	}
	for {
		count := 100
		if err := qb.Select(table).CountAll().Query(session).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

func timestamp() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().UTC().Truncate(time.Millisecond))
}

var (
	crawlLog1 = &logV1.CrawlLog{
		ExecutionId:         uuid.NewString(),
		JobExecutionId:      uuid.NewString(),
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamp(),
		BlockDigest:         "sha1:f054ed8f9fd5893d6b70dc336a68e8092782723c",
		CollectionFinalName: "Collection_2021",
		ContentType:         "text/dns",
		DiscoveryPath:       "P",
		FetchTimeMs:         46,
		IpAddress:           "8.8.8.8:53",
		RecordType:          "response",
		RequestedUri:        "dns:www.example.com",
		Size:                50,
		Error: &commons.Error{
			Code:   -1,
			Msg:    "Error",
			Detail: "Everything went wrong",
		},
		StatusCode: 1,
		StorageRef: "warcfile:Collection_2021-20210415110455-veidemann_contentwriter_775ffd88bc_5ljbb-00000.warc.gz:667",
	}
	crawlLog2 = &logV1.CrawlLog{
		ExecutionId:         crawlLog1.ExecutionId,
		JobExecutionId:      crawlLog1.JobExecutionId,
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamp(),
		BlockDigest:         "sha1:f054ed8f9fd5893d6b70dc336a68e8092782723c",
		CollectionFinalName: "Collection_2021",
		ContentType:         "text/dns",
		DiscoveryPath:       "P",
		FetchTimeMs:         46,
		IpAddress:           "8.8.8.8:53",
		RecordType:          "response",
		RequestedUri:        "dns:www.example.com",
		Size:                50,
		StatusCode:          200,
		StorageRef:          "warcfile:Collection_2021-20210415110455-veidemann_contentwriter_775ffd88bc_5ljbb-00000.warc.gz:667",
	}
	pageLog1 = &logV1.PageLog{
		ExecutionId:         uuid.NewString(),
		WarcId:              uuid.NewString(),
		JobExecutionId:      uuid.NewString(),
		Uri:                 "https://www.nb.no/samlinger",
		Referrer:            "https://www.nb.no/",
		CollectionFinalName: "Veidemann_2021",
		Method:              "GET",
		Resource: []*logV1.PageLog_Resource{
			{
				Uri:           "https://www.nb.no/samlinger",
				FromCache:     false,
				Renderable:    false,
				ResourceType:  "t",
				ContentType:   "text/html",
				StatusCode:    200,
				DiscoveryPath: "L",
				WarcId:        uuid.NewString(),
				Referrer:      "https://www.nb.no/",
				Error:         nil,
				Method:        "GET",
			},
		},
		Outlink: []string{
			"https://www.nb.no/whatever",
		},
	}
	pageLog2 = &logV1.PageLog{
		WarcId:              uuid.NewString(),
		ExecutionId:         pageLog1.ExecutionId,
		JobExecutionId:      pageLog1.JobExecutionId,
		Uri:                 "https://www.nb.no/presse",
		Referrer:            "https://www.nb.no/",
		CollectionFinalName: "Veidemann_2021",
		Method:              "GET",
		Resource: []*logV1.PageLog_Resource{
			{
				Uri:           "https://www.nb.no/presse",
				FromCache:     false,
				Renderable:    false,
				ResourceType:  "t",
				ContentType:   "text/html",
				StatusCode:    200,
				DiscoveryPath: "L",
				WarcId:        uuid.NewString(),
				Referrer:      "https://www.nb.no/",
				Error:         nil,
				Method:        "GET",
			},
		},
		Outlink: []string{
			"https://www.nb.no/foobar",
		},
	}
)

const (
	CrawlLogTable = "crawl_log"
	PageLogTable  = "page_log"
)
