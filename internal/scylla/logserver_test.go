package scylla

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/nlnwa/veidemann-api/go/commons/v1"
	logV1 "github.com/nlnwa/veidemann-api/go/log/v1"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"testing"
	"time"
)

var session gocqlx.Session

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
			Image:        "scylladb/scylla:4.4.1",
			ExposedPorts: []string{"9042/tcp"},
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
			Image:      "norsknettarkiv/veidemann-log-schema:v1.0.0",
			AutoRemove: true,
			Networks:   []string{networkName},
			WaitingFor: wait.ForLog("Schema initialized"),
		},
		Started: true,
	}); err != nil {
		panic(err)
	}

	cqlshPort, err := scyllaC.MappedPort(ctx, "9042/tcp")
	if err != nil {
		panic(err)
	}

	cfg := createCluster(gocql.Quorum, "v7n_v1_dc1", "localhost")
	cfg.Port = cqlshPort.Int()
	client := &Client{config: cfg}

	if err := client.Connect(); err != nil {
		panic(err)
	}

	session = client.session

	// wait for index to be ready before running tests
	time.Sleep(5 * time.Second)

	code := m.Run()

	client.Close()
	_ = scyllaC.Terminate(ctx)
	_ = newNetwork.Remove(ctx)

	os.Exit(code)
}

func TestListCrawlLog(t *testing.T) {
	crawlLog1 := &logV1.CrawlLog{
		ExecutionId:         uuid.NewString(),
		JobExecutionId:      uuid.NewString(),
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamppb.New(time.Now().UTC()),
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
		TimeStamp:  timestamppb.New(time.Now().UTC()),
	}

	crawlLog2 := &logV1.CrawlLog{
		ExecutionId:         crawlLog1.ExecutionId,
		JobExecutionId:      crawlLog1.JobExecutionId,
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamppb.New(time.Now().UTC()),
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
		TimeStamp:           timestamppb.New(time.Now().UTC()),
	}

	crawlLogs := []*logV1.CrawlLog{crawlLog1, crawlLog2}
	count := 0

	// write
	for _, crawlLog := range crawlLogs {
		if err := writeCrawlLog(
			qb.Insert("crawl_log").Json().Query(session),
			crawlLog,
		); err != nil {
			t.Error(err)
		}
		count++
	}
	if count != len(crawlLogs) {
		t.Errorf("Expected %d crawl logs was inserted, was %d", len(crawlLogs), count)
	}

	// read and assert
	if err := listCrawlLogsByExecutionId(
		qb.Select("crawl_log").Where(qb.Eq("execution_id")).Query(session),
		&logV1.CrawlLogListRequest{
			QueryTemplate: &logV1.CrawlLog{
				ExecutionId: crawlLog1.ExecutionId,
			},
		},
		func(got *logV1.CrawlLog) error {
			var expected *logV1.CrawlLog
			for _, crawlLog := range crawlLogs {
				if crawlLog.WarcId == got.WarcId {
					expected = crawlLog
				}
			}
			if expected == nil {
				t.Errorf("Found no crawlLog with warcId: %s", got.GetWarcId())
			}
			assertEqualCrawlLogs(t, expected, got)
			count--
			return nil
		},
	); err != nil {
		t.Error(err)
	}
}

func TestListPageLog(t *testing.T) {
	pageLog1 := &logV1.PageLog{
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
	pageLog2 := &logV1.PageLog{
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
	pageLogs := []*logV1.PageLog{pageLog1, pageLog2}

	count := 0
	// insert
	for _, pageLog := range pageLogs {
		if err := writePageLog(
			qb.Insert("page_log").Json().Query(session),
			pageLog,
		); err != nil {
			t.Error(err)
		}
		count++
	}

	if count != len(pageLogs) {
		t.Errorf("Expected %d page logs to have been inserted, was %d", len(pageLogs), count)
	}

	// try to list a page log that does not exist
	bogus := uuid.NewString()
	if err := listPageLogs(
		qb.Select("page_log").Where(qb.Eq("warc_id")).Query(session),
		&logV1.PageLogListRequest{
			QueryTemplate: &logV1.PageLog{
				WarcId: bogus,
			},
		},
		func(got *logV1.PageLog) error {
			t.Errorf("Expected no callback to be made for bogus warcId: %s", bogus)
			return nil
		},
	); err != nil {
		t.Error(err)
	}

	// Expect to find page log with known warcId
	if err := listPageLogs(
		qb.Select("page_log").Where(qb.Eq("warc_id")).Query(session),
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
		t.Error(err)
	}

	// Expect to list all pagelogs with known execution id
	if err := listPageLogsByExecutionId(
		qb.Select("page_log").Where(qb.Eq("execution_id")).Query(session),
		&logV1.PageLogListRequest{
			QueryTemplate: &logV1.PageLog{
				ExecutionId: pageLog1.ExecutionId,
			},
		},
		func(got *logV1.PageLog) error {
			var expected *logV1.PageLog

			// find corresponding original pagelog
			for _, pageLog := range pageLogs {
				if pageLog.WarcId == got.WarcId {
					expected = pageLog
				}
			}
			if expected == nil {
				t.Errorf("Found no pagelog matching warcId: %s", got.GetWarcId())
			}
			assertEqualPageLogs(t, expected, got)
			count--
			return nil
		},
	); err != nil {
		t.Error(err)
	}
	if count != 0 {
		t.Errorf("Expected to list %d page logs, got %d", len(pageLogs), len(pageLogs)-count)
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
