package scylla

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"time"
)

func createCluster(consistency gocql.Consistency, keyspace string, hosts ...string) *gocql.ClusterConfig {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Timeout = 5 * time.Second
	cluster.RetryPolicy = retryPolicy
	cluster.Consistency = consistency
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	return cluster
}

type Options struct {
	Hosts    []string
	Keyspace string
}

// Client is a scylla client.
type Client struct {
	config  *gocql.ClusterConfig
	session gocqlx.Session
}

// Connect establishes a connection to a ScyllaDB cluster.
func (c *Client) Connect() error {
	session, err := gocqlx.WrapSession(gocql.NewSession(*c.config))
	if err != nil {
		return fmt.Errorf("failed to connect to scylla: %w", err)
	}
	c.session = session

	return nil
}

// Close closes the database session
func (c *Client) Close() {
	c.session.Close()
}
