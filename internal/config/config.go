package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// AWS SDK environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
// AWS_REGION, AWS_ENDPOINT_URL) are read directly by the SDK's default
// credential chain and do not appear in this struct.

type Config struct {
	UpstreamRegistry      string
	StorageBackend        string
	FSRoot                string
	ListenAddr            string
	S3Bucket              string
	S3Prefix              string
	S3ForcePathStyle      bool
	CacheTagManifests     bool
	CacheLatestTag        bool
	S3LifecycleDays       int
	GenerateSelfSignedTLS bool
	LogLevel              slog.Level
	ProxyMode             string // "transparent" or "authenticated"
}

func Load() (Config, error) {
	selfSigned := envOr("GENERATE_SELF_SIGNED_TLS", "false") == "true"
	defaultAddr := ":8080"
	if selfSigned {
		defaultAddr = ":8443"
	}

	lifecycleDays, _ := strconv.Atoi(envOr("S3_LIFECYCLE_DAYS", "28"))

	proxyMode := os.Getenv("PROXY_MODE")
	if proxyMode != "transparent" && proxyMode != "authenticated" {
		return Config{}, fmt.Errorf("PROXY_MODE is required and must be \"transparent\" or \"authenticated\" (got %q)", proxyMode)
	}

	return Config{
		UpstreamRegistry:      os.Getenv("UPSTREAM_REGISTRY"),
		StorageBackend:        envOr("STORAGE_BACKEND", "s3"),
		FSRoot:                envOr("FS_ROOT", "/data/oci-cache"),
		ListenAddr:            envOr("LISTEN_ADDR", defaultAddr),
		S3Bucket:              envOr("S3_BUCKET", "oci-cache"),
		S3Prefix:              os.Getenv("S3_PREFIX"),
		S3ForcePathStyle:      envOr("S3_FORCE_PATH_STYLE", "true") == "true",
		S3LifecycleDays:       lifecycleDays,
		CacheTagManifests:     envOr("CACHE_TAG_MANIFESTS", "true") == "true",
		CacheLatestTag:        envOr("CACHE_LATEST_TAG", "false") == "true",
		GenerateSelfSignedTLS: selfSigned,
		LogLevel:              parseLogLevel(envOr("LOG_LEVEL", "info")),
		ProxyMode:             proxyMode,
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
