package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/danielloader/oci-pull-through/internal/cache"
	"github.com/danielloader/oci-pull-through/internal/config"
	"github.com/danielloader/oci-pull-through/internal/proxy"
	"github.com/danielloader/oci-pull-through/internal/tlsgen"
)

func main() {
	// Self-contained healthcheck for scratch containers (no curl/wget available).
	// Usage: oci-pull-through -healthcheck
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		resp, err := http.Get("http://127.0.0.1:8080/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	cfg := config.Load()

	if cfg.UpstreamRegistry == "" {
		fmt.Fprintln(os.Stderr, "UPSTREAM_REGISTRY is required (e.g. https://ghcr.io, https://registry-1.docker.io)")
		os.Exit(1)
	}
	upstreamURL, err := url.Parse(cfg.UpstreamRegistry)
	if err != nil || upstreamURL.Host == "" {
		fmt.Fprintf(os.Stderr, "UPSTREAM_REGISTRY %q is not a valid URL (expected https://host or http://host)\n", cfg.UpstreamRegistry)
		os.Exit(1)
	}
	if upstreamURL.Scheme != "https" && upstreamURL.Scheme != "http" {
		fmt.Fprintf(os.Stderr, "UPSTREAM_REGISTRY scheme must be http or https, got %q\n", upstreamURL.Scheme)
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := newStore(ctx, cfg)
	if err != nil {
		slog.Error("failed to create store", "backend", cfg.StorageBackend, "error", err)
		os.Exit(1)
	}

	if err := store.Init(ctx); err != nil {
		slog.Error("failed to initialise store", "backend", cfg.StorageBackend, "error", err)
		os.Exit(1)
	}

	upstreamClient := proxy.NewUpstreamClient()
	upstreamClient.Scheme = upstreamURL.Scheme

	handler := &proxy.Handler{
		Registry:          upstreamURL.Host,
		Cache:             store,
		Upstream:          upstreamClient,
		CacheTagManifests: cfg.CacheTagManifests,
		CacheLatestTag:    cfg.CacheLatestTag,
	}

	logged := proxy.LoggingMiddleware(handler)

	var server *http.Server

	if cfg.GenerateSelfSignedTLS {
		cert, err := tlsgen.SelfSignedCert()
		if err != nil {
			slog.Error("failed to generate self-signed certificate", "error", err)
			os.Exit(1)
		}
		slog.Info("generated self-signed TLS certificate")

		server = &http.Server{
			Addr:    cfg.ListenAddr,
			Handler: logged,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
			},
		}
		// http2 is configured automatically by ListenAndServeTLS
	} else {
		// Wrap with h2c for cleartext HTTP/2 support alongside HTTP/1.1
		h2s := &http2.Server{}
		server = &http.Server{
			Addr:    cfg.ListenAddr,
			Handler: h2c.NewHandler(logged, h2s),
		}
	}

	go func() {
		slog.Info("starting server", "addr", cfg.ListenAddr, "upstream", cfg.UpstreamRegistry, "tls", cfg.GenerateSelfSignedTLS, "backend", cfg.StorageBackend)
		var err error
		if cfg.GenerateSelfSignedTLS {
			err = server.ListenAndServeTLS("", "")
		} else {
			err = server.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
		os.Exit(1)
	}
	slog.Info("shutdown complete")
}

func newStore(ctx context.Context, cfg config.Config) (cache.Store, error) {
	switch cfg.StorageBackend {
	case "s3":
		return cache.NewS3Store(ctx, cfg.S3Bucket, cfg.S3ForcePathStyle, cfg.S3LifecycleDays)
	case "fs":
		return cache.NewFSStore(cfg.FSRoot), nil
	default:
		return nil, fmt.Errorf("unknown storage backend: %q", cfg.StorageBackend)
	}
}
