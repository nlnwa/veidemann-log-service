package connection

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	*grpc.Server
	address string
}

func NewGrpcServer(host string, port int, opts ...grpc.ServerOption) *Server {
	return &Server{
		address: fmt.Sprintf("%s:%d", host, port),
		Server: grpc.NewServer(opts...),
	}
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.address, err)
	}
	return s.Server.Serve(lis)
}

func (s *Server) Shutdown() {
	s.Server.GracefulStop()
}
