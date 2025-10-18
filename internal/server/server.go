package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"brm/pkg/blocking"
	"brm/pkg/config"
)

// Server represents the HTTP server
type Server struct {
	httpServer        *http.Server
	logger            *slog.Logger
	port              int
	blockingSimulator *blocking.Simulator
	responseFormat    string // "json" or "text"
}

// New creates a new server instance from server-specific configuration
func New(serverCfg *config.Config, rootCfg *config.Config, logger *slog.Logger) *Server {
	port := serverCfg.GetIntWithDefault("port", 8080)
	readTimeout := serverCfg.GetIntWithDefault("readTimeout", 15)
	writeTimeout := serverCfg.GetIntWithDefault("writeTimeout", 15)
	idleTimeout := serverCfg.GetIntWithDefault("idleTimeout", 60)

	mux := http.NewServeMux()
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  time.Duration(readTimeout) * time.Second,
		WriteTimeout: time.Duration(writeTimeout) * time.Second,
		IdleTimeout:  time.Duration(idleTimeout) * time.Second,
	}

	// Get blocking configuration from root config
	blockingCfg := rootCfg.GetSubConfig("blocking")
	responseFormat := blockingCfg.GetStringWithDefault("response-format", "json")

	// Create blocking simulator
	blockingSimulator := blocking.New(blockingCfg, logger)

	srv := &Server{
		httpServer:        httpServer,
		logger:            logger,
		port:              port,
		blockingSimulator: blockingSimulator,
		responseFormat:    responseFormat,
	}

	// Setup routes using server methods
	srv.setupRoutes()

	return srv
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	mux := s.httpServer.Handler.(*http.ServeMux)
	mux.HandleFunc("/status", s.statusHandler)
	mux.HandleFunc("/rest/blocking", s.blockingHandler)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.Info("Starting BRM server", "port", s.port)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down server gracefully")
	return s.httpServer.Shutdown(ctx)
}

// Port returns the server port
func (s *Server) Port() int {
	return s.port
}
