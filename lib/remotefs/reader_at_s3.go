package remotefs

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/fscommon"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
	"os"
	pathlib "path"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var ReadDurationMillSecond = &atomic.Int64{}

// remotePart wraps `Part` and `Object` for performance tuning
type remotePart struct {
	*common.Part
	//buf *bufio.Reader
	obj *minio.Object
}

func (rp *remotePart) Close() error {
	return rp.obj.Close()
}

type localPart struct {
	*common.Part
	f *os.File
}

func (rp *localPart) Close() error {
	return rp.f.Close()
}

// RemoteReaderAt support read with offset
type RemoteReaderAt struct {
	remoteFS *RemoteFS

	fileSize uint64
	//parts       []common.Part
	remoteParts []*remotePart
	localParts  []*localPart

	syncToLocal *atomic.Bool

	// just for debug
	remotePath string
	localPath  string
}

func (rra *RemoteReaderAt) SyncToLocal(localDir string) error {
	errG, ctx := errgroup.WithContext(context.Background())
	errG.SetLimit(len(rra.remoteParts))
	startTime := time.Now()
	for _, rp := range rra.remoteParts {
		key := rp.RemotePath(remoteFS.prefix)
		localPath := rp.RemotePath(localDir)
		logger.Infof("DEBUG: SyncToLocal remotePath: %q; file size: %v MB", rra.remotePath, rra.fileSize/1024/1024)
		errG.Go(func() error {
			if err := remoteFS.client.FGetObject(ctx, remoteFS.bucket, key, localPath, minio.GetObjectOptions{}); err != nil {
				logger.Errorf("fget object: %v to path: %v failed, err: %w", key, localPath, err)
				return err
			}
			return nil
		})
	}
	if err := errG.Wait(); err != nil {
		return err
	}
	logger.Infof("DEBUG: SyncToLocal remotePath: %q in %.3f seconds; file size: %v MB", rra.remotePath, time.Since(startTime).Seconds(), rra.fileSize/1024/1024)
	var errOuter error
	localParts := make([]*localPart, 0, len(rra.remoteParts))

	for _, rp := range rra.remoteParts {
		f, err := os.Open(rp.RemotePath(localDir))
		if err != nil {
			logger.Errorf("open local part err: %w", err)
			errOuter = err
			break
		}
		localParts = append(localParts, &localPart{
			Part: rp.Part,
			f:    f,
		})
	}
	if errOuter != nil {
		for _, lp := range localParts {
			_ = lp.f.Close()
		}
		return errOuter
	}
	rra.localParts = localParts
	rra.syncToLocal.Store(true)
	return nil
}

func (rra *RemoteReaderAt) FileSize() uint64 {
	return rra.fileSize
}

// implement 1
func (rra *RemoteReaderAt) MustReadAt(p []byte, off int64) {
	// may be not panic?

	if len(p) == 0 {
		return
	}
	if len(rra.remoteParts) == 0 {
		// do nothing
		return
	}

	if off < 0 {
		logger.Panicf("off=%d cannot be negative", off)
	}
	if rra.syncToLocal.Load() {
		var targetPart *localPart
		for _, part := range rra.localParts {
			var inPart bool
			if uint64(off) >= part.Offset && uint64(off) < part.Offset+part.Size {
				inPart = true
			}
			if inPart {
				targetPart = part
				break
			}
		}
		if targetPart == nil {
			logger.Errorf("BUG: can not local offset %v in parts, local path %v", off, rra.remotePath)
			return
		}
		startTime := time.Now()
		n, err := targetPart.f.ReadAt(p, off-int64(targetPart.Offset))
		if err != nil {
			logger.Panicf("FATAL: cannot read %d bytes at offset %d of file %q: %s", len(p), off, targetPart.f.Name(), err)
		}
		if n != len(p) {
			logger.Panicf("FATAL: unexpected number of bytes read; got %d; want %d", n, len(p))
		}
		dur := time.Since(startTime)
		ReadDurationMillSecond.Add(dur.Milliseconds())
		logger.Infof("DEBUG: MustReadAt from local path: %q; len(p): %v; real off: %v in %.3f seconds; file size: %v MB", rra.remotePath, len(p), off-int64(targetPart.Offset), dur.Seconds(), rra.fileSize/1024/1024)
		return
	}
	// read from parts a block must not cross parts
	var targetPart *remotePart
	for _, part := range rra.remoteParts {
		var inPart bool
		if uint64(off) >= part.Offset && uint64(off) < part.Offset+part.Size {
			inPart = true
		}
		if inPart {
			targetPart = part
			break
		}
	}
	if targetPart == nil {
		logger.Errorf("BUG: can not local offset %v in parts, remotePath in s3 %v", off, rra.remotePath)
		return
	}
	startTime := time.Now()
	n, err := targetPart.obj.ReadAt(p, off-int64(targetPart.Offset))
	if err != nil {
		logger.Panicf("FATAL: cannot read %d bytes at offset %d of object %q: %s", len(p), off, targetPart.RemotePath(remoteFS.prefix), err)
	}
	if n != len(p) {
		logger.Panicf("FATAL: unexpected number of bytes read; got %d; want %d", n, len(p))
	}
	dur := time.Since(startTime)
	ReadDurationMillSecond.Add(dur.Milliseconds())
	logger.Infof("DEBUG: MustReadAt from remote path: %q; len(p): %v; real off: %v in %.3f seconds; file size: %v MB", rra.remotePath, len(p), off-int64(targetPart.Offset), dur.Seconds(), rra.fileSize/1024/1024)
	//logger.Infof("DEBUG: MustReadAt remotePath: %q; len(p): %v; off: %v in %.3f seconds", rra.remotePath, len(p), off, time.Since(startTime).Seconds())
}

// implement 2
//func (rra *RemoteReaderAt) MustReadAt(p []byte, off int64) {
//	if len(p) == 0 || len(rra.parts) == 0 {
//		return
//	}
//	// may be not panic?
//	if off < 0 {
//		logger.Panicf("off=%d cannot be negative", off)
//	}
//	// read from parts
//	// a block must not cross parts
//	var targetPart *common.Part
//	for _, part := range rra.parts {
//		var inPart bool
//		if uint64(off) >= part.Offset && uint64(off) < part.Offset+part.Size {
//			inPart = true
//		}
//		if inPart {
//			targetPart = &part
//			break
//		}
//	}
//	if targetPart == nil {
//		logger.Errorf("BUG: can not local offset %v in parts, remotePath in s3 %v", off, rra.remotePath)
//		return
//	}
//	startTime := time.Now()
//	options := &minio.GetObjectOptions{}
//	start := off - int64(targetPart.Offset)
//	end := start + int64(len(p)) - 1
//	if err := options.SetRange(start, end); err != nil {
//		logger.Errorf("BUG: set get object range start: %v, end: %v failed, err:", start, end, err)
//		return
//	}
//	key := targetPart.RemotePath(remoteFS.prefix)
//	obj, err := rra.remoteFS.client.GetObject(context.Background(), rra.remoteFS.bucket, key, *options)
//	if err != nil {
//		logger.Errorf("request get object failed, err: %w", err)
//		return
//	}
//	defer obj.Close()
//	n, err := obj.Read(p)
//	if err != nil && err != io.EOF {
//		logger.Panicf("FATAL: cannot read %d bytes at offset %d of object %q: %s", len(p), off, key, err)
//	}
//	if n != len(p) {
//		logger.Panicf("FATAL: unexpected number of bytes read; got %d; want %d", n, len(p))
//	}
//	logger.Infof("DEBUG: MustReadAt remotePath: %q; len(p): %v; off: %v in %.3f seconds", rra.remotePath, len(p), off, time.Since(startTime).Seconds())
//}

// MustClose must close the reader.
func (rra *RemoteReaderAt) MustClose() {
	// do nothing
	for _, rp := range rra.remoteParts {
		_ = rp.Close()
	}
	for _, lp := range rra.localParts {
		_ = lp.Close()
	}
}

// consider parts for one file
func OpenRemoteReaderAt(path string) (*RemoteReaderAt, error) {
	if remoteFS == nil {
		return nil, remoteFSNotInitErr
	}
	// remotePath to the *.bin dir
	dir := pathlib.Join(remoteFS.prefix, path)
	options := &minio.ListObjectsOptions{
		Prefix:    dir,
		Recursive: true,
	}
	var errOuter error
	var remoteParts []*remotePart
	ctx := context.Background()
	infoCh := remoteFS.client.ListObjects(ctx, remoteFS.bucket, *options)
	for info := range infoCh {
		if info.Err != nil {
			errOuter = info.Err
			break
		}
		file := info.Key
		if !strings.HasPrefix(file, dir) {
			logger.Infof("unexpected prefix for s3 key %q; want %q", file, dir)
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
		p.ActualSize = uint64(info.Size)
		if p.Size != p.ActualSize {
			logger.Infof("skipping broken object %q", file)
			continue
		}
		obj, err := remoteFS.client.GetObject(ctx, remoteFS.bucket, file, minio.GetObjectOptions{})
		if err != nil {
			errOuter = fmt.Errorf("get object: %v failed, err: %w", file, err)
			logger.Errorf("get object: %v failed, err: %w", file, err)
			continue
		}
		rp := &remotePart{
			Part: &p,
			//buf:  bufio.NewReader(obj),
			obj: obj,
		}
		remoteParts = append(remoteParts, rp)
	}
	if errOuter != nil {
		for _, rp := range remoteParts {
			_ = rp.Close()
		}
		return nil, fmt.Errorf("error when listing s3 objects inside dir %q: %w", dir, errOuter)
	}
	if len(remoteParts) == 0 {
		return nil, fmt.Errorf("file is empty")
	}
	sortRemoteParts(remoteParts)
	ora := &RemoteReaderAt{
		remoteParts: remoteParts,
		remoteFS:    remoteFS,
		fileSize:    remoteParts[0].FileSize,
		syncToLocal: &atomic.Bool{},

		remotePath: dir,
	}
	return ora, nil
}

// SortParts sorts parts by (Path, Offset)
func sortRemoteParts(parts []*remotePart) {
	sort.Slice(parts, func(i, j int) bool {
		a := parts[i]
		b := parts[j]
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Offset < b.Offset
	})
}
