package mergeset

import (
	"fmt"
	"path/filepath"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/remotefs"
)

var idxbCache = blockcache.NewCache(getMaxIndexBlocksCacheSize)
var ibCache = blockcache.NewCache(getMaxInmemoryBlocksCacheSize)

// SetIndexBlocksCacheSize overrides the default size of indexdb/indexBlock cache
func SetIndexBlocksCacheSize(size int) {
	maxIndexBlockCacheSize = size
}

func getMaxIndexBlocksCacheSize() int {
	maxIndexBlockCacheSizeOnce.Do(func() {
		if maxIndexBlockCacheSize <= 0 {
			maxIndexBlockCacheSize = int(0.10 * float64(memory.Allowed()))
		}
	})
	return maxIndexBlockCacheSize
}

var (
	maxIndexBlockCacheSize     int
	maxIndexBlockCacheSizeOnce sync.Once
)

// SetDataBlocksCacheSize overrides the default size of indexdb/dataBlocks cache
func SetDataBlocksCacheSize(size int) {
	maxInmemoryBlockCacheSize = size
}

func getMaxInmemoryBlocksCacheSize() int {
	maxInmemoryBlockCacheSizeOnce.Do(func() {
		if maxInmemoryBlockCacheSize <= 0 {
			maxInmemoryBlockCacheSize = int(0.25 * float64(memory.Allowed()))
		}
	})
	return maxInmemoryBlockCacheSize
}

var (
	maxInmemoryBlockCacheSize     int
	maxInmemoryBlockCacheSizeOnce sync.Once
)

type part struct {
	ph partHeader

	path string

	size uint64

	mrs []metaindexRow

	indexFile fs.MustReadAtCloser
	itemsFile fs.MustReadAtCloser
	lensFile  fs.MustReadAtCloser
}

func openFilePart(path string) (*part, error) {
	path = filepath.Clean(path)

	var ph partHeader
	if err := ph.ParseFromPath(path); err != nil {
		return nil, fmt.Errorf("cannot parse path to part: %w", err)
	}

	metaindexPath := path + "/metaindex.bin"
	metaindexFile, err := filestream.Open(metaindexPath, true)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", metaindexPath, err)
	}
	metaindexSize := fs.MustFileSize(metaindexPath)

	indexPath := path + "/index.bin"
	indexFile := fs.MustOpenReaderAt(indexPath)
	indexSize := fs.MustFileSize(indexPath)

	itemsPath := path + "/items.bin"
	itemsFile := fs.MustOpenReaderAt(itemsPath)
	itemsSize := fs.MustFileSize(itemsPath)

	lensPath := path + "/lens.bin"
	lensFile := fs.MustOpenReaderAt(lensPath)
	lensSize := fs.MustFileSize(lensPath)

	size := metaindexSize + indexSize + itemsSize + lensSize
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func openRemotePart(path string) (*part, error) {
	path = filepath.Clean(path)
	// todo
	var ph partHeader
	if err := ph.ParseFromRemotePath(path); err != nil {
		return nil, fmt.Errorf("cannot parse path to part: %w", err)
	}

	metaindexPath := path + "/metaindex.bin"
	metaindexFile, err := remotefs.OpenRemoteReader(metaindexPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", metaindexPath, err)
	}
	metaindexSize := metaindexFile.FileSize()

	indexPath := path + "/index.bin"
	indexFile, err := remotefs.OpenRemoteReaderAt(indexPath)
	if err != nil {
		return nil, err
	}
	indexSize := indexFile.FileSize()

	itemsPath := path + "/items.bin"
	itemsFile, err := remotefs.OpenRemoteReaderAt(itemsPath)
	if err != nil {
		return nil, err
	}
	itemsSize := itemsFile.FileSize()

	lensPath := path + "/lens.bin"
	lensFile, err := remotefs.OpenRemoteReaderAt(lensPath)
	if err != nil {
		return nil, err
	}
	lensSize := lensFile.FileSize()

	size := metaindexSize + indexSize + itemsSize + lensSize
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func syncRemotePart(remotePath string, localDir string) (*part, error) {
	path := filepath.Clean(remotePath)
	// todo
	var ph partHeader
	if err := ph.ParseFromRemotePath(path); err != nil {
		return nil, fmt.Errorf("cannot parse path to part: %w", err)
	}

	metaindexPath := path + "/metaindex.bin"
	metaindexFile, err := remotefs.OpenRemoteReader(metaindexPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", metaindexPath, err)
	}
	metaindexSize := metaindexFile.FileSize()

	indexPath := path + "/index.bin"
	indexFile, err := remotefs.OpenRemoteReaderAt(indexPath)
	if err != nil {
		return nil, err
	}
	indexSize := indexFile.FileSize()
	if indexSize <= 1024*1024*1024 {
		if err := indexFile.SyncToLocal(localDir); err != nil {
			return nil, err
		}
	}

	itemsPath := path + "/items.bin"
	itemsFile, err := remotefs.OpenRemoteReaderAt(itemsPath)
	if err != nil {
		return nil, err
	}
	itemsSize := itemsFile.FileSize()
	if itemsSize <= 1024*1024*1024 {
		if err := itemsFile.SyncToLocal(localDir); err != nil {
			return nil, err
		}
	}

	lensPath := path + "/lens.bin"
	lensFile, err := remotefs.OpenRemoteReaderAt(lensPath)
	if err != nil {
		return nil, err
	}
	lensSize := lensFile.FileSize()
	if lensSize <= 1024*1024*1024 {
		if err := lensFile.SyncToLocal(localDir); err != nil {
			return nil, err
		}
	}

	size := metaindexSize + indexSize + itemsSize + lensSize
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)

}

func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, indexFile, itemsFile, lensFile fs.MustReadAtCloser) (*part, error) {
	var errors []error
	mrs, err := unmarshalMetaindexRows(nil, metaindexReader)
	if err != nil {
		errors = append(errors, fmt.Errorf("cannot unmarshal metaindexRows: %w", err))
	}
	metaindexReader.MustClose()

	var p part
	p.path = path
	p.size = size
	p.mrs = mrs

	p.indexFile = indexFile
	p.itemsFile = itemsFile
	p.lensFile = lensFile

	p.ph.CopyFrom(ph)
	if len(errors) > 0 {
		// Return only the first error, since it has no sense in returning all errors.
		err := fmt.Errorf("error opening part %s: %w", p.path, errors[0])
		p.MustClose()
		return nil, err
	}
	return &p, nil
}

func (p *part) MustClose() {
	p.indexFile.MustClose()
	p.itemsFile.MustClose()
	p.lensFile.MustClose()

	idxbCache.RemoveBlocksForPart(p)
	ibCache.RemoveBlocksForPart(p)
}

type indexBlock struct {
	bhs []blockHeader
}

func (idxb *indexBlock) SizeBytes() int {
	bhs := idxb.bhs[:cap(idxb.bhs)]
	n := int(unsafe.Sizeof(*idxb))
	for i := range bhs {
		n += bhs[i].SizeBytes()
	}
	return n
}
