// Package scylla implements a server that talks to a scylla cluster.
package scylla

// About paging:
//
// Paging with scylla used to be stateless but is now stateful:
// https://www.scylladb.com/2018/07/13/efficient-query-paging/,
// see https://github.com/scylladb/gocql/blob/master/example_paging_test.go for examples.
