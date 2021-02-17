package connection

import (
	"fmt"
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	address string
	*grpc.Server
}

func NewGrpcServer(host string, port int) *Server {
	return &Server{
		address: fmt.Sprintf("%s:%d", host, port),
	}
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.address, err)
	}

	//tracer := opentracing.GlobalTracer()
	//var opts = []grpc.ServerOption{
	//	grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)),
	//	grpc.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(tracer)),
	//}
	var opts []grpc.ServerOption
	s.Server = grpc.NewServer(opts...)

	return s.Server.Serve(lis)
}

func (s *Server) Shutdown() {
	s.Server.GracefulStop()
}
