package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
)

// S3Store provides S3-backed caching for OCI objects.
type S3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store creates a new S3 cache store.
// Credentials, region, and endpoint are resolved via the standard AWS SDK
// default credential chain (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
// AWS_REGION, AWS_ENDPOINT_URL, instance profiles, etc.).
func NewS3Store(ctx context.Context, bucket string, forcePathStyle bool) (*S3Store, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = forcePathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	return &S3Store{
		client: client,
		bucket: bucket,
	}, nil
}

// Init creates the S3 bucket if it doesn't already exist.
func (s *S3Store) Init(ctx context.Context) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		// Ignore "bucket already exists" errors
		var baoby *types.BucketAlreadyOwnedByYou
		var bae *types.BucketAlreadyExists
		if isError(err, &baoby) || isError(err, &bae) {
			slog.Debug("bucket already exists", "bucket", s.bucket)
			return nil
		}
		return fmt.Errorf("creating bucket: %w", err)
	}
	slog.Debug("bucket created", "bucket", s.bucket)
	return nil
}

// Head checks if an object exists and returns its metadata.
func (s *S3Store) Head(ctx context.Context, key string) (ObjectMeta, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return ObjectMeta{}, err
	}

	meta := ObjectMeta{
		ContentLength: aws.ToInt64(out.ContentLength),
	}
	if out.ContentType != nil {
		meta.ContentType = *out.ContentType
	}
	if v, ok := out.Metadata["docker-content-digest"]; ok {
		meta.DockerContentDigest = NormalizeDigest(v)
	}
	return meta, nil
}

// GetWithMeta retrieves an object's body and metadata in a single GetObject call,
// eliminating the HEAD+GET race window.
func (s *S3Store) GetWithMeta(ctx context.Context, key string) (*GetResult, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	meta := ObjectMeta{
		ContentLength: aws.ToInt64(out.ContentLength),
	}
	if out.ContentType != nil {
		meta.ContentType = *out.ContentType
	}
	if v, ok := out.Metadata["docker-content-digest"]; ok {
		meta.DockerContentDigest = NormalizeDigest(v)
	}

	return &GetResult{Body: out.Body, Meta: meta}, nil
}

// Put writes an object to S3 using a single PutObject call.
// Race conditions are benign: blobs are content-addressed (identical content)
// and manifest overwrites are harmless. The proxy handler already does a HEAD
// check before fetching from upstream, so duplicate writes are unlikely.
func (s *S3Store) Put(ctx context.Context, key string, body io.Reader, meta ObjectMeta) error {
	input := &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     body,
		Metadata: map[string]string{},
	}

	if meta.ContentLength > 0 {
		input.ContentLength = aws.Int64(meta.ContentLength)
	}
	if meta.ContentType != "" {
		input.ContentType = aws.String(meta.ContentType)
	}
	if meta.DockerContentDigest != "" {
		input.Metadata["docker-content-digest"] = meta.DockerContentDigest
	}

	_, err := s.client.PutObject(ctx, input,
		s3.WithAPIOptions(func(stack *middleware.Stack) error {
			return v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware(stack)
		}),
		func(o *s3.Options) {
			o.RetryMaxAttempts = 1
		},
	)
	if err != nil {
		return fmt.Errorf("putting to S3: %w", err)
	}
	return nil
}

// isError checks if an error matches a target type using string matching,
// since different S3 implementations may return errors differently.
func isError[T error](err error, target *T) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	switch any(*target).(type) {
	case *types.BucketAlreadyOwnedByYou:
		return strings.Contains(errMsg, "BucketAlreadyOwnedByYou")
	case *types.BucketAlreadyExists:
		return strings.Contains(errMsg, "BucketAlreadyExists")
	}
	return false
}
