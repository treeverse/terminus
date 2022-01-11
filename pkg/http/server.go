// Package http holds the HTTP server used by Terminus.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	http_pprof "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/treeverse/terminus/pkg/store"
)

const (
	forceShutdownTime = 10 * time.Second
	JSONContentType   = "application/json"
)

type Server struct {
	Store store.Store
}

// Serve serves all HTTP traffic on ctx, until that is cancelled.
func (s *Server) Serve(ctx context.Context, listenAddress string) {
	router := chi.NewRouter()
	router.Mount("/_health", ServeHealth())
	router.Mount("/internal/_pprof/", ServePPRof())
	// Internal service, respond only on a designated "internal" endpoint.
	router.Mount("/internal/api/v1", s.ServeREST())
	server := &http.Server{
		Addr:    listenAddress,
		Handler: router,
	}

	// termCtx is the context for stopping the server.
	termCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed to listen on %s: %v\n", listenAddress, err)
		}
	}()

	go func() {
		done := termCtx.Done()
		if done == nil {
			return // Never cancelled
		}
		<-done
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("server failed to shut down: %v", err)
			time.Sleep(forceShutdownTime)
			os.Exit(1)
		}
		os.Exit(0)
	}()
}

func ServeHealth() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "alive!")
	})
}

func (s *Server) ServeREST() http.Handler {
	router := chi.NewRouter()
	router.Get("/quota/exceeded", func(w http.ResponseWriter, r *http.Request) {
		exceeded, err := s.Store.GetExceeded(r.Context())
		if err != nil {
			log.Printf("[ERROR] Get keys exceeding quota: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			_, err = fmt.Fprintf(w, "Get keys exceeding quota: %v", err)
			if err != nil {
				log.Printf("[ERROR]   Write error message: %v", err)
			}
			return
		}
		h := w.Header()
		h["Content-Type"] = []string{JSONContentType}
		body := struct{ Records []store.Record }{exceeded}
		encodedBody, err := json.Marshal(body)
		if err != nil {
			log.Printf("[ERROR] [I] %s while encoding %v", err, body)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, err = fmt.Fprint(w, string(encodedBody))
		if err != nil {
			log.Printf("[ERROR] %s while writing %d-byte response", err, len(encodedBody))
			return
		}
	})
	return router
}

func ServePPRof() http.Handler {
	router := chi.NewRouter()
	router.Get("/", http_pprof.Index)
	for _, profile := range pprof.Profiles() {
		name := profile.Name()
		handler := http_pprof.Handler(name)
		// BUG(ariels): Also handles non-GET operations.
		router.Handle("/"+name, handler)
	}
	router.Get("/cmdline", http_pprof.Cmdline)
	router.Get("/profile", http_pprof.Profile)
	router.Get("/symbol", http_pprof.Symbol)
	router.Get("/trace", http_pprof.Trace)

	return router
}
