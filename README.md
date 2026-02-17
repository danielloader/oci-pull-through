# oci-pull-through

A pull-through cache for OCI container registries. It sits between
your container runtime and upstream registries, transparently caching
image layers and manifests on first pull. Subsequent pulls for the
same content are served from the cache without contacting the
upstream registry.

This exists because pulling the same images repeatedly across a
fleet of machines is wasteful. Rate limits, network latency, and
registry outages compound the problem. A pull-through cache
eliminates redundant transfers and provides a degree of resilience
against upstream unavailability for previously-cached content.

## How it works

The proxy implements the [OCI Distribution Spec][oci-spec] read
path. The upstream registry hostname is encoded in the request path:

[oci-spec]: https://github.com/opencontainers/distribution-spec/blob/main/spec.md

```text
GET /v2/{registry}/{image}/manifests/{reference}
GET /v2/{registry}/{image}/blobs/{digest}
```

For example, pulling `ghcr.io/org/app:v1.2.3` through the proxy
running on `cache.internal:8080`:

```shell
docker pull cache.internal:8080/ghcr.io/org/app:v1.2.3
```

On a cache miss, the proxy fetches from the upstream registry and
simultaneously streams the response to the client and the cache
store. The client is never blocked by cache writes -- if the cache
store is slow or fails, the client stream continues uninterrupted.

On a cache hit, the response is served directly from the cache
store. No upstream request is made. All upstream response headers
(excluding hop-by-hop headers) are stored alongside the cached
object and replayed on cache hits, making the proxy transparent to
clients that depend on headers like `ETag` or `Accept-Ranges`.

## Caching behaviour

Content-addressed objects (blobs and manifests resolved by digest)
are immutable. They are always cached and served with
`Cache-Control: public, max-age=31536000, immutable`.

Tag references are mutable -- a tag can point to a different digest
at any time. Caching of tag manifests is therefore optional and
controlled by configuration:

| Scenario | Cached | Condition |
| --- | --- | --- |
| Blob (`/blobs/sha256:...`) | Always | Immutable |
| Manifest by digest | Always | Immutable |
| Manifest by tag | Configurable | `CACHE_TAG_MANIFESTS=true` |
| Manifest by `latest` | Configurable | Both tag and latest flags |

When tag manifests are cached, they are served with
`Cache-Control: public, max-age=60`.

Non-2xx upstream responses are forwarded to the client as-is and
are never cached.

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `STORAGE_BACKEND` | `s3` | Storage backend. `s3` or `fs`. |
| `LISTEN_ADDR` | `:8080` (`:8443` with TLS) | Listen address. |
| `GENERATE_SELF_SIGNED_TLS` | `false` | Generate a self-signed TLS certificate on startup. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |
| `CACHE_TAG_MANIFESTS` | `true` | Cache manifests resolved by tag. |
| `CACHE_LATEST_TAG` | `false` | Cache the `latest` tag. |

### S3 backend

| Variable | Default | Description |
| --- | --- | --- |
| `S3_BUCKET` | `oci-cache` | Bucket name. Auto-created. |
| `S3_FORCE_PATH_STYLE` | `true` | Path-style S3 URLs. |
| `AWS_ACCESS_KEY_ID` | -- | Standard SDK credential chain. |
| `AWS_SECRET_ACCESS_KEY` | -- | Standard SDK credential chain. |
| `AWS_REGION` | -- | Standard SDK credential chain. |
| `AWS_ENDPOINT_URL` | -- | S3-compatible endpoint override. |

Credentials, region, and endpoint are resolved through the standard
AWS SDK default credential chain. IAM instance profiles, ECS task
roles, and `~/.aws/credentials` all work as expected.

### Filesystem backend

| Variable | Default | Description |
| --- | --- | --- |
| `FS_ROOT` | `/data/oci-cache` | Root directory for cache. |

Objects are stored as files with `.meta.json` sidecar files
containing content metadata and the full set of upstream response
headers. Writes are atomic (temp file + rename). The S3 backend
uses the same `.meta.json` sidecar pattern (stored as a separate
S3 object alongside the data object) for parity between backends.

## Running

### Docker Compose (development)

The included `docker-compose.yml` runs the proxy with
[SeaweedFS][seaweedfs] as an S3-compatible backend:

[seaweedfs]: https://github.com/seaweedfs/seaweedfs

```shell
docker compose up
```

The proxy is available on `localhost:8080`. SeaweedFS provides S3
on port 8333.

### Container image

```dockerfile
FROM golang:1.26-alpine AS builder
# ...
FROM scratch
```

The image is built from `scratch`. It contains the static binary,
CA certificates, and nothing else. It runs as `nobody`.

Build and run:

```shell
docker build -t oci-pull-through .
docker run -p 8080:8080 \
  -e STORAGE_BACKEND=s3 \
  -e AWS_ENDPOINT_URL=http://your-s3:9000 \
  -e AWS_ACCESS_KEY_ID=access \
  -e AWS_SECRET_ACCESS_KEY=secret \
  oci-pull-through
```

### Binary

```shell
go build -o oci-pull-through ./cmd/oci-pull-through
STORAGE_BACKEND=fs FS_ROOT=/var/cache/oci ./oci-pull-through
```

## Health check

`GET /healthz` returns `200 OK` when the server is accepting
connections.

For scratch containers (no shell, no curl), the binary includes
a built-in health check client:

```shell
oci-pull-through -healthcheck
```

This is what the Docker Compose healthcheck uses. Exit code 0 on
success, 1 on failure.

## API endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/healthz` | Health check. |
| `GET` | `/v2/` | OCI version check. |
| `GET`, `HEAD` | `/v2/{reg}/{name}/manifests/{ref}` | Manifest. |
| `GET`, `HEAD` | `/v2/{reg}/{name}/blobs/{digest}` | Blob. |

The proxy supports multi-segment image names
(e.g., `/v2/ghcr.io/org/sub/image/manifests/latest`).

`docker.io` is automatically resolved to `registry-1.docker.io`
for upstream requests.

## Protocol

By default the proxy serves both HTTP/1.1 and cleartext HTTP/2
(h2c) on the same port. TLS termination is expected to be handled
by a reverse proxy or load balancer in front of this service.

### Self-signed TLS

Setting `GENERATE_SELF_SIGNED_TLS=true` generates an in-memory
ECDSA P-256 self-signed certificate on startup (valid for 10 years,
with SANs for `localhost`, `127.0.0.1`, and `::1`). The server
switches to HTTPS with HTTP/2 and the default listen address
changes to `:8443`.

This is useful for local development where the Docker daemon
requires HTTPS to pull from a registry. No certificate files are
written to disk.

Authorization headers from the client are forwarded to the upstream
registry as-is. The proxy does not perform authentication or token
exchange. If your upstream registry requires authentication, the
client must provide valid credentials.

## Signals

The process handles `SIGINT` and `SIGTERM` for graceful shutdown
with a 30-second drain timeout.
