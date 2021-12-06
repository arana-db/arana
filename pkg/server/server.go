package server

import (
	"github.com/dubbogo/kylin/pkg/proto"
)

type Server struct {
	listeners []proto.Listener
}

func NewServer() *Server {
	return &Server{
		listeners: make([]proto.Listener, 0),
	}
}

func (srv *Server) AddListener(listener proto.Listener) {
	srv.listeners = append(srv.listeners, listener)
}

func (srv *Server) Start() {
	for _, l := range srv.listeners {
		l.Listen()
	}
}
