package cache

import (
	"bytes"
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

// metaKey returns the S3 key for the metadata sidecar object.
func metaKey(key string) string {
	return key + ".meta.json"
}

// Head checks if an object exists and returns its metadata from the sidecar.
func (s *S3Store) Head(ctx context.Context, key string) (ObjectMeta, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(metaKey(key)),
	})
	if err != nil {
		return ObjectMeta{}, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return ObjectMeta{}, fmt.Errorf("reading meta sidecar: %w", err)
	}

	meta, err := UnmarshalMeta(data)
	if err != nil {
		return ObjectMeta{}, fmt.Errorf("parsing meta sidecar: %w", err)
	}
	return meta, nil
}

// GetWithMeta retrieves an object's body and metadata.
// It reads the sidecar .meta.json first, then opens the data object.
func (s *S3Store) GetWithMeta(ctx context.Context, key string) (*GetResult, error) {
	// Read metadata sidecar
	metaOut, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(metaKey(key)),
	})
	if err != nil {
		return nil, err
	}
	defer metaOut.Body.Close()

	data, err := io.ReadAll(metaOut.Body)
	if err != nil {
		return nil, fmt.Errorf("reading meta sidecar: %w", err)
	}

	meta, err := UnmarshalMeta(data)
	if err != nil {
		return nil, fmt.Errorf("parsing meta sidecar: %w", err)
	}

	// Read data object
	dataOut, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return &GetResult{Body: dataOut.Body, Meta: meta}, nil
}

// Put writes an object and its metadata sidecar to S3.
// Race conditions are benign: blobs are content-addressed (identical content)
// and manifest overwrites are harmless. The proxy handler already does a HEAD
// check before fetching from upstream, so duplicate writes are unlikely.
func (s *S3Store) Put(ctx context.Context, key string, body io.Reader, meta ObjectMeta) error {
	// Write data object
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   body,
	}

	if meta.ContentLength > 0 {
		input.ContentLength = aws.Int64(meta.ContentLength)
	}
	if meta.ContentType != "" {
		input.ContentType = aws.String(meta.ContentType)
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
		return fmt.Errorf("putting data to S3: %w", err)
	}

	// Write metadata sidecar
	metaJSON, err := MarshalMeta(meta)
	if err != nil {
		return fmt.Errorf("marshalling metadata: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(metaKey(key)),
		Body:        bytes.NewReader(metaJSON),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("putting meta sidecar to S3: %w", err)
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
