package remotefs

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/fscommon"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
	pathlib "path"
	"strings"
)

var bufPool bytesutil.ByteBufferPool

// does not support read with offset
type RemoteReader struct {
	remoteFS *RemoteFS

	currOffset int
	fileSize   uint64

	buf       *bytesutil.ByteBuffer
	bufReader filestream.ReadCloser
	//parts     []common.Part
}

func (rr *RemoteReader) Read(p []byte) (int, error) {
	return rr.bufReader.Read(p)
}
func (rr *RemoteReader) MustClose() {
	rr.bufReader.MustClose()
	bufPool.Put(rr.buf)
}
func (rr *RemoteReader) FileSize() uint64 {
	return rr.fileSize
}

func OpenRemoteReader(path string) (*RemoteReader, error) {
	if remoteFS == nil {
		return nil, errors.New("bkt reader is not init")
	}
	// remotePath to the *.bin dir
	dir := pathlib.Join(remoteFS.prefix, path)
	option := minio.ListObjectsOptions{
		Prefix:    dir,
		Recursive: true,
	}
	var errOuter error
	var parts []common.Part
	objCh := remoteFS.client.ListObjects(context.Background(), remoteFS.bucket, option)
	for obj := range objCh {
		if obj.Err != nil {
			errOuter = obj.Err
			break
		}
		file := obj.Key
		if !strings.HasPrefix(file, dir) {
			errOuter = fmt.Errorf("unexpected prefix for s3 key %q; want %q", file, dir)
			continue
		}
		if fscommon.IgnorePath(file) {
			continue
		}
		var p common.Part
		if !p.ParseFromRemotePath(file[len(remoteFS.prefix):]) {
			logger.Infof("skipping unknown object %q", file)
			continue
		}
		p.ActualSize = uint64(obj.Size)
		if p.Size != p.ActualSize {
			errOuter = errors.Errorf("skipping broken object %q", file)
			break
		}
		parts = append(parts, p)
	}
	if errOuter != nil {
		return nil, fmt.Errorf("error when listing s3 objects inside dir %q: %w", dir, errOuter)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("file is empty")
	}
	// read all content form part at once
	fileSize := parts[0].FileSize
	buf := bufPool.Get()
	common.SortParts(parts)
	objs := make([]*minio.Object, 0, len(parts))
	defer func() {
		for _, obj := range objs {
			_ = obj.Close()
		}
	}()
	// we need two loops to make sure objs are closed when error occurs
	for _, part := range parts {
		obj, err := remoteFS.client.GetObject(context.Background(), remoteFS.bucket, part.RemotePath(remoteFS.prefix), minio.GetObjectOptions{})
		if err != nil {
			bufPool.Put(buf)
			logger.Errorf("request get object failed, err:", err)
			return nil, err
		}
		objs = append(objs, obj)
	}
	for i, part := range parts {
		obj := objs[i]
		n, err := buf.ReadFrom(obj)
		if err != nil {
			bufPool.Put(buf)
			logger.Errorf("cannot read object %q: %s", part.RemotePath(remoteFS.prefix), err)
			return nil, err
		}
		if uint64(n) != part.ActualSize {
			logger.Errorf("unexpected number of bytes read; got %d; want %d", n, part.ActualSize)
			bufPool.Put(buf)
			return nil, err
		}
	}
	rr := &RemoteReader{
		//parts:     parts,
		remoteFS:  remoteFS,
		fileSize:  fileSize,
		buf:       buf,
		bufReader: buf.NewReader(),
	}
	return rr, nil
}
