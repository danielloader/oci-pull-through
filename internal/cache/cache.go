package cache

import (
	"context"
	"io"
	"strings"
)

// Store is the interface for OCI object storage backends.
type Store interface {
	Init(ctx context.Context) error
	Head(ctx context.Context, key string) (ObjectMeta, error)
	GetWithMeta(ctx context.Context, key string) (*GetResult, error)
	Put(ctx context.Context, key string, body io.Reader, meta ObjectMeta) error
}

// ObjectMeta holds metadata for cached objects.
type ObjectMeta struct {
	ContentType         string
	DockerContentDigest string
	ContentLength       int64
}

// GetResult holds the body and metadata from a single get call.
type GetResult struct {
	Body io.ReadCloser
	Meta ObjectMeta
}

// NormalizeDigest ensures a digest uses the standard "algorithm:hex" format.
// SeaweedFS mangles colons to hyphens in S3 metadata values, so
// "sha256:abc..." becomes "sha256-abc..." on read-back. This restores the colon.
func NormalizeDigest(s string) string {
	if strings.Contains(s, ":") {
		return s
	}
	for _, alg := range []string{"sha256", "sha512"} {
		if strings.HasPrefix(s, alg+"-") {
			return alg + ":" + s[len(alg)+1:]
		}
	}
	return s
}
