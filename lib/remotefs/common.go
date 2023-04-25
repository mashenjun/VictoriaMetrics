package remotefs

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/fscommon"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"io"
	"net/http"
	pathlib "path"
	"strings"
)

var remoteFS *RemoteFS

var remoteFSNotInitErr = errors.New("remote fs is not init")

type RemoteFS struct {
	bucket string
	client *minio.Client
	prefix string
}

type BuketConfig struct {
	Bucket             string            `yaml:"bucket"`
	Endpoint           string            `yaml:"endpoint"`
	Region             string            `yaml:"region"`
	Dir                string            `yaml:"dir"`
	AWSSDKAuth         bool              `yaml:"aws_sdk_auth"`
	AccessKey          string            `yaml:"access_key"`
	Insecure           bool              `yaml:"insecure"`
	SignatureV2        bool              `yaml:"signature_version2"`
	SecretKey          string            `yaml:"secret_key"`
	PutUserMetadata    map[string]string `yaml:"put_user_metadata"`
	ListObjectsVersion string            `yaml:"list_objects_version"`
}

type overrideSignerType struct {
	credentials.Provider
	signerType credentials.SignatureType
}

func InitRemoteFSBySchemaPath(path string) error {
	if len(path) == 0 {
		return fmt.Errorf("remotePath cannot be empty")
	}
	n := strings.Index(path, "://")
	if n < 0 {
		return fmt.Errorf("missing scheme in remotePath %q. Supported schemes: `gs://`, `s3://`, `fs://`", path)
	}
	scheme := path[:n]
	dir := path[n+len("://"):]
	switch scheme {
	case "s3":
		n := strings.Index(dir, "/")
		if n < 0 {
			return fmt.Errorf("missing directory on the s3 bucket %q", dir)
		}
		bucket := dir[:n]
		dir = dir[n:]
		ctx := context.Background()
		opts := session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}
		sess, err := session.NewSessionWithOptions(opts)
		if err != nil {
			return fmt.Errorf("cannot create S3 session: %w", err)
		}
		region, err := s3manager.GetBucketRegion(ctx, sess, bucket, "us-west-2")
		endpoint := fmt.Sprintf("s3.%s.amazonaws.com", region)
		return InitRemoteFS(&BuketConfig{
			Bucket:   bucket,
			Endpoint: endpoint,
			Region:   region,
			Dir:      strings.TrimPrefix(dir, "/"),
		})
	default:
		return fmt.Errorf("unsupported scheme %q", scheme)
	}
}

func InitRemoteFS(config *BuketConfig) error {
	var chain []credentials.Provider
	wrapCredentialsProvider := func(p credentials.Provider) credentials.Provider { return p }
	if config.SignatureV2 {
		wrapCredentialsProvider = func(p credentials.Provider) credentials.Provider {
			return &overrideSignerType{Provider: p, signerType: credentials.SignatureV2}
		}
	}

	chain = []credentials.Provider{
		wrapCredentialsProvider(&credentials.EnvAWS{}),
		wrapCredentialsProvider(&credentials.FileAWSCredentials{}),
		wrapCredentialsProvider(&credentials.IAM{
			Client: &http.Client{
				Transport: http.DefaultTransport,
			},
		}),
	}
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:        credentials.NewChainCredentials(chain),
		Secure:       !config.Insecure,
		Region:       config.Region,
		Transport:    http.DefaultTransport,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return errors.Wrap(err, "initialize s3 client")
	}
	remoteFS = &RemoteFS{
		bucket: config.Bucket,
		prefix: config.Dir,
		client: client,
	}
	return nil
}

func ListRemoteDir(path string) ([]string, error) {
	if remoteFS == nil {
		return nil, remoteFSNotInitErr
	}
	dir := pathlib.Join(remoteFS.prefix, path)
	options := &minio.ListObjectsOptions{
		Prefix:    dir,
		Recursive: true,
	}
	names := make([]string, 0)
	filter := make(map[string]struct{})
	objCh := remoteFS.client.ListObjects(context.Background(), remoteFS.bucket, *options)
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		file := obj.Key
		if !strings.HasPrefix(file, dir) {
			return nil, fmt.Errorf("unexpected prefix for s3 key %q; want %q", file, dir)
		}
		if fscommon.IgnorePath(file) {
			continue
		}
		name := strings.TrimPrefix(file, dir)
		if name == "" {
			continue
		}
		segments := strings.Split(strings.TrimPrefix(name, "/"), "/")
		if _, found := filter[segments[0]]; found {
			continue
		}
		filter[segments[0]] = struct{}{}
		names = append(names, segments[0])
	}
	return names, nil

}

// only used by reading metadata.json which has only one part
func ReadRemoteFile(path string) ([]byte, error) {
	if remoteFS == nil {
		return nil, remoteFSNotInitErr
	}
	ctx := context.Background()
	dir := pathlib.Join(remoteFS.prefix, path)
	options := &minio.ListObjectsOptions{
		Prefix:    dir,
		Recursive: true,
		MaxKeys:   1,
	}
	var partPath string
	objCh := remoteFS.client.ListObjects(ctx, remoteFS.bucket, *options)
	for obj := range objCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		file := obj.Key
		if !strings.HasPrefix(file, dir) {
			return nil, fmt.Errorf("unexpected prefix for s3 key %q; want %q", file, dir)
		}
		partPath = obj.Key
	}
	if len(partPath) == 0 {
		return nil, fmt.Errorf("%q does not contain remote parts", path)
	}
	part, err := remoteFS.client.GetObject(ctx, remoteFS.bucket, partPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object for s3 key %q failed: %v", partPath, err)
	}
	return io.ReadAll(part)

}
