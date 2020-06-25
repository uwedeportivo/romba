// Copyright (c) 2013 Uwe Hoffmann. All rights reserved.

/*
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package archive

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/klauspost/compress/gzip"
	"github.com/uwedeportivo/romba/worker"
	"github.com/willf/bloom"

	"github.com/dgraph-io/ristretto"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/util"
)

type Depot struct {
	roots []*depotRoot
	RomDB db.RomDB
	lock  *sync.Mutex
	cache *ristretto.Cache
	// where in the depot to reserve the next space
	// when archiving
	start int
}

type cacheValue struct {
	hh        *Hashes
	rootIndex int
}

func NewDepot(roots []string, maxSize []int64, romDB db.RomDB) (*Depot, error) {
	glog.Info("Depot init")

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}

	depot := new(Depot)
	depot.roots = make([]*depotRoot, len(roots))
	depot.cache = cache

	for k, root := range roots {
		glog.Infof("establishing size of %s", root)
		size, err := establishSize(root)
		if err != nil {
			return nil, err
		}

		glog.Infof("initialize bloomfilter for %s", root)

		bf := bloom.NewWithEstimates(20000000, 0.1)
		err = loadBloomFilter(root, bf)
		if err != nil {
			return nil, err
		}
		depot.roots[k] = &depotRoot{
			path:       root,
			size:       size,
			maxSize:    maxSize[k],
			bf:         bf,
			bloomReady: bf != nil,
		}
	}

	glog.Info("Initializing Depot with the following roots")

	for _, dr := range depot.roots {
		glog.Infof("root = %s, maxSize = %s, size = %s", dr.path,
			humanize.IBytes(uint64(dr.maxSize)), humanize.IBytes(uint64(dr.size)))
	}

	depot.RomDB = romDB
	depot.lock = new(sync.Mutex)
	glog.Info("Depot init finished")
	return depot, nil
}

func (depot *Depot) RomInDepot(sha1Hex string) (bool, string, error) {
	v, hit := depot.cache.Get(sha1Hex)
	if hit {
		cv := v.(*cacheValue)
		return true, pathFromSha1HexEncoding(depot.roots[cv.rootIndex].path,
			hex.EncodeToString(cv.hh.Sha1), gzipSuffix), nil
	}
	for _, dr := range depot.roots {
		dr.Lock()
		if dr.bloomReady && !dr.bf.Test([]byte(sha1Hex)) {
			dr.Unlock()
			continue
		}
		dr.Unlock()

		rompath := pathFromSha1HexEncoding(dr.path, sha1Hex, gzipSuffix)
		exists, err := PathExists(rompath)
		if err != nil {
			return false, "", err
		}

		if exists {
			return true, rompath, nil
		}
	}
	return false, "", nil
}

func (depot *Depot) SHA1InDepot(sha1Hex string) (bool, *Hashes, string, int64, error) {
	v, hit := depot.cache.Get(sha1Hex)
	if hit {
		cv := v.(*cacheValue)
		return true, cv.hh, pathFromSha1HexEncoding(depot.roots[cv.rootIndex].path,
			hex.EncodeToString(cv.hh.Sha1), gzipSuffix), cv.hh.Size, nil
	}
	for idx, dr := range depot.roots {
		dr.Lock()
		if dr.bloomReady && !dr.bf.Test([]byte(sha1Hex)) {
			dr.Unlock()
			return false, nil, "", 0, nil
		}
		dr.Unlock()

		rompath := pathFromSha1HexEncoding(dr.path, sha1Hex, gzipSuffix)
		exists, err := PathExists(rompath)
		if err != nil {
			return false, nil, "", 0, err
		}

		var size int64

		if exists {
			hh := new(Hashes)
			sha1Bytes, err := hex.DecodeString(sha1Hex)
			if err != nil {
				return false, nil, "", 0, err
			}
			hh.Sha1 = sha1Bytes

			romGZ, err := os.Open(rompath)
			if err != nil {
				return false, nil, "", 0, err
			}
			defer romGZ.Close()

			gzr, err := gzip.NewReader(romGZ)
			if err != nil {
				return false, nil, "", 0, err
			}
			defer gzr.Close()

			md5crcBuffer := gzr.Header.Extra

			if len(md5crcBuffer) == md5.Size+crc32.Size+8 {
				hh.Md5 = make([]byte, md5.Size)
				copy(hh.Md5, md5crcBuffer[:md5.Size])
				hh.Crc = make([]byte, crc32.Size)
				copy(hh.Crc, md5crcBuffer[md5.Size:md5.Size+crc32.Size])
				size = util.BytesToInt64(md5crcBuffer[md5.Size+crc32.Size:])
			} else {
				glog.Warningf("rom %s has missing gzip md5 or crc header", rompath)
			}

			depot.cache.Set(sha1Hex, &cacheValue{
				hh:        hh,
				rootIndex: idx,
			}, 1)

			return true, hh, rompath, size, nil
		}
	}
	return false, nil, "", 0, nil
}

type zeroLengthReadCloser struct{}

func (zlrc *zeroLengthReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (zlrc *zeroLengthReadCloser) Close() error {
	return nil
}

func (depot *Depot) OpenRomGZ(rom *types.Rom) (io.ReadCloser, error) {
	if rom.Size == 0 {
		return new(zeroLengthReadCloser), nil
	}

	if rom.Sha1 == nil {
		return nil, fmt.Errorf("cannot open rom %s because SHA1 is missing", rom.Name)
	}

	sha1Hex := hex.EncodeToString(rom.Sha1)

	for _, root := range depot.roots {
		rompath := pathFromSha1HexEncoding(root.path, sha1Hex, gzipSuffix)
		exists, err := PathExists(rompath)
		if err != nil {
			return nil, err
		}

		if exists {
			return os.Open(rompath)
		}
	}
	return nil, nil
}

func (depot *Depot) Paths() []string {
	ps := make([]string, 0, len(depot.roots))

	for _, dr := range depot.roots {
		ps = append(ps, dr.path)
	}
	return ps
}

func (depot *Depot) PopulateBloom(path string) {
	parts := strings.Split(path, string(filepath.Separator))

	if len(parts) < 5 {
		glog.Errorf("failed to populate bloom filter for path %s: not enough dir parts", path)
		return
	}
	n := len(parts) - 5
	depotPath := string(filepath.Separator) + filepath.Join(parts[:n]...)

	for _, dr := range depot.roots {
		if depotPath == dr.path {
			fn := parts[len(parts)-1]
			sha1Hex := strings.TrimSuffix(fn, ".gz")
			if len(sha1Hex) != 40 {
				glog.Errorf("failed to populate bloom filter for path %s: not enough dir parts", path)
				return
			}
			dr.Lock()
			dr.bf.Add([]byte(sha1Hex))
			dr.numBfAdded++
			if dr.numBfAdded == 10000 {
				oldResumes, err := filepath.Glob(filepath.Join(dr.path, "resumebloom-*"))
				if err != nil {
					glog.Errorf("failed to clean up old resume files in %s: %v", dr.path, err)
				}

				for _, oldResume := range oldResumes {
					err := os.Remove(oldResume)
					if err != nil {
						glog.Errorf("failed to clean old resume file %s: %v", oldResume, err)
					}
				}
				resumePath := filepath.Join(dr.path, "resumebloom-"+sha1Hex)
				err = writeBloomFilter(resumePath, dr.bf)
				if err != nil {
					glog.Errorf("failed to write resume path %s for populating bloom filter: %v", resumePath, err)
				}
			}
			dr.Unlock()
		}
	}
}

func (depot *Depot) ClearBloomFilters() error {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	for _, dr := range depot.roots {
		dr.Lock()
		dr.bloomReady = false
		dr.bf.ClearAll()
		dr.numBfAdded = 0
		dr.Unlock()
		bfFilepath := filepath.Join(dr.path, bloomFilterFilename)
		bfFileExists, err := PathExists(bfFilepath)
		if err != nil {
			return err
		}
		if bfFileExists {
			err := os.Remove(bfFilepath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (depot *Depot) ResumePopBloomPaths() ([]worker.ResumePath, error) {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	rps := make([]worker.ResumePath, 0, len(depot.roots))

	for _, dr := range depot.roots {
		files, err := filepath.Glob(filepath.Join(dr.path, "resumebloom-*"))
		if err != nil {
			return nil, err
		}

		if len(files) > 1 {
			return nil, fmt.Errorf("more than one resumebloom files found in %s", dr.path)
		}

		if len(files) == 0 {
			rps = append(rps, worker.ResumePath{Path: dr.path})
			continue
		}

		_, filename := filepath.Split(files[0])

		parts := strings.Split(filename, "-")

		if len(parts) != 2 || len(parts[1]) != 40 {
			return nil, fmt.Errorf("resumebloom file with unexpected name %s", files[0])
		}

		sha1Hex := parts[1]
		resumeLine := pathFromSha1HexEncoding(dr.path, sha1Hex, gzipSuffix)

		dr.Lock()
		err = loadBloomFilter(files[0], dr.bf)
		dr.Unlock()
		if err != nil {
			return nil, err
		}

		rps = append(rps, worker.ResumePath{Path: dr.path, ResumeLine: resumeLine})
	}

	return rps, nil
}

func (depot *Depot) SaveBloomFilters() error {
	for _, dr := range depot.roots {
		dr.Lock()
		oldResumes, err := filepath.Glob(filepath.Join(dr.path, "resumebloom-*"))
		if err != nil {
			glog.Errorf("failed to clean up old resume files in %s: %v", dr.path, err)
		}

		for _, oldResume := range oldResumes {
			err := os.Remove(oldResume)
			if err != nil {
				glog.Errorf("failed to clean old resume file %s: %v", oldResume, err)
			}
		}

		err = writeBloomFilterWithBackup(dr.path, dr.bf)
		if err != nil {
			dr.Unlock()
			return err
		}
		dr.bloomReady = true
		dr.Unlock()
	}
	return nil
}
