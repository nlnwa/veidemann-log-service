package connection

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	*grpc.Server
	opts    []grpc.ServerOption
	address string
}

func NewGrpcServer(host string, port int, opts ...grpc.ServerOption) *Server {
	return &Server{
		address: fmt.Sprintf("%s:%d", host, port),
		opts: opts,
	}
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.address, err)
	}

	s.Server = grpc.NewServer(s.opts...)

	return s.Server.Serve(lis)
}

func (s *Server) Shutdown() {
	s.Server.GracefulStop()
}
