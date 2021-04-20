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
			Image:      "norsknettarkiv/veidemann-log-schema:v0.1.0",
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

	code := m.Run()

	client.Close()
	_ = scyllaC.Terminate(ctx)
	_ = newNetwork.Remove(ctx)

	os.Exit(code)
}

func TestListCrawlLog(t *testing.T) {
	crawlLog := &logV1.CrawlLog{
		ExecutionId:         uuid.NewString(),
		WarcId:              uuid.NewString(),
		FetchTimeStamp:      timestamppb.Now(),
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
		TimeStamp:  timestamppb.Now(),
	}

	// write
	if err := writeCrawlLog(
		qb.Insert("crawl_log").Json().Query(session),
		crawlLog,
	); err != nil {
		t.Error(err)
	}

	// read and assert
	if err := listCrawlLogs(
		qb.Select("crawl_log").Where(qb.Eq("execution_id")).Query(session),
		&logV1.CrawlLogListRequest{
			QueryTemplate: &logV1.CrawlLog{
				ExecutionId: crawlLog.ExecutionId,
			},
		},
		func(gotCL *logV1.CrawlLog) error {
			// convert to json for comparison
			expected, err := protojson.Marshal(crawlLog)
			if err != nil {
				t.Error(err)
			}
			got, err := protojson.Marshal(gotCL)
			if err != nil {
				t.Error(err)
			}
			if string(expected) != string(got) {
				t.Errorf("Expected: %s, got: %s", expected, got)
			}
			return nil
		},
	); err != nil {
		t.Error(err)
	}
}

func TestListPageLog(t *testing.T) {
	pageLog := &logV1.PageLog{
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
				WarcId:        "7f035395-e03f-4ed9-88b1-cc43e90f81dc",
				Referrer:      "https://www.nb.no/",
				Error:         nil,
				Method:        "GET",
			},
		},
		Outlink: []string{
			"https://www.nb.no/whatever",
		},
	}

	// write
	if err := writePageLog(
		qb.Insert("page_log").Json().Query(session),
		pageLog,
	); err != nil {
		t.Error(err)
	}

	// read and assert
	if err := listPageLogs(
		qb.Select("page_log").Where(qb.Eq("execution_id")).Query(session),
		&logV1.PageLogListRequest{
			QueryTemplate: &logV1.PageLog{
				ExecutionId: pageLog.ExecutionId,
			},
		},
		func(gotPL *logV1.PageLog) error {
			// convert to json for comparison
			expected, err := protojson.Marshal(pageLog)
			if err != nil {
				t.Error(err)
			}
			got, err := protojson.Marshal(gotPL)
			if err != nil {
				t.Error(err)
			}
			if string(expected) != string(got) {
				t.Errorf("Expected: %s, got: %s", expected, got)
			}
			return nil
		},
	); err != nil {
		t.Error(err)
	}
}
