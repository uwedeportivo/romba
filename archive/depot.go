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
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/klauspost/compress/gzip"

	"github.com/dgraph-io/ristretto"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/util"
	"github.com/willf/bloom"
)

type Depot struct {
	roots        []string
	bloomReady   []bool
	bloomfilters []*bloom.BloomFilter
	sizes        []int64
	maxSizes     []int64
	touched      []bool
	rootLocks    []*sync.Mutex
	RomDB        db.RomDB
	lock         *sync.Mutex
	cache        *ristretto.Cache
	// where in the depot to reserve the next space
	// when archiving
	start int
}

type cacheValue struct {
	hh        *Hashes
	rootIndex int
}

func loadBloomFilter(root string) (*bloom.BloomFilter, error) {
	bfp := filepath.Join(root, bloomFilterFilename)
	exists, err := PathExists(bfp)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, nil
	}

	bf := bloom.NewWithEstimates(20000000, 0.1)
	file, err := os.Open(bfp)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = bf.ReadFrom(file)
	if err != nil {
		return nil, err
	}
	return bf, nil
}

func writeBloomFilter(root string, bf *bloom.BloomFilter) error {
	bfFilePath := filepath.Join(root, bloomFilterFilename)

	exists, err := PathExists(bfFilePath)
	if err != nil {
		return err
	}

	if exists {
		backupBfFilePath := filepath.Join(root, backupBloomFilterFilename)

		err := os.Rename(bfFilePath, backupBfFilePath)
		if err != nil {
			return err
		}
	}

	file, err := os.Create(bfFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = bf.WriteTo(file)
	return err
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
	depot.roots = make([]string, len(roots))
	depot.rootLocks = make([]*sync.Mutex, len(roots))
	depot.bloomfilters = make([]*bloom.BloomFilter, len(roots))
	depot.bloomReady = make([]bool, len(roots))
	depot.sizes = make([]int64, len(roots))
	depot.maxSizes = make([]int64, len(roots))
	depot.touched = make([]bool, len(roots))
	depot.cache = cache

	copy(depot.roots, roots)
	copy(depot.maxSizes, maxSize)

	for k, root := range depot.roots {
		glog.Infof("establishing size of %s", root)
		size, err := establishSize(root)
		if err != nil {
			return nil, err
		}
		depot.sizes[k] = size
		depot.rootLocks[k] = new(sync.Mutex)

		glog.Infof("initialize bloomfilter for %s", root)

		bf, err := loadBloomFilter(root)
		if err != nil {
			return nil, err
		}
		depot.bloomfilters[k] = bf
		depot.bloomReady[k] = bf != nil
	}

	glog.Info("Initializing Depot with the following roots")

	for k, root := range depot.roots {
		glog.Infof("root = %s, maxSize = %s, size = %s", root,
			humanize.IBytes(uint64(depot.maxSizes[k])), humanize.IBytes(uint64(depot.sizes[k])))
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
		return true, pathFromSha1HexEncoding(depot.roots[cv.rootIndex],
			hex.EncodeToString(cv.hh.Sha1), gzipSuffix), nil
	}
	for idx, root := range depot.roots {
		if depot.bloomReady[idx] && !depot.bloomfilters[idx].Test([]byte(sha1Hex)) {
			return false, "", nil
		}

		rompath := pathFromSha1HexEncoding(root, sha1Hex, gzipSuffix)
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
		return true, cv.hh, pathFromSha1HexEncoding(depot.roots[cv.rootIndex],
			hex.EncodeToString(cv.hh.Sha1), gzipSuffix), cv.hh.Size, nil
	}
	for idx, root := range depot.roots {
		if depot.bloomReady[idx] && !depot.bloomfilters[idx].Test([]byte(sha1Hex)) {
			return false, nil, "", 0, nil
		}

		rompath := pathFromSha1HexEncoding(root, sha1Hex, gzipSuffix)
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
		rompath := pathFromSha1HexEncoding(root, sha1Hex, gzipSuffix)
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

func (depot *Depot) writeSizes() {
	for k, root := range depot.roots {
		depot.rootLocks[k].Lock()
		if depot.touched[k] {
			err := writeSizeFile(root, depot.sizes[k])
			if err != nil {
				glog.Errorf("failed to write size file into %s: %v\n", root, err)
			} else {
				depot.touched[k] = false
			}

			if depot.bloomReady[k] {
				err = writeBloomFilter(root, depot.bloomfilters[k])
				if err != nil {
					depot.touched[k] = true
					glog.Errorf("failed to write bloomfilter into %s: %v\n", root, err)
				}
			}
		}
		depot.rootLocks[k].Unlock()
	}
}

func (depot *Depot) adjustSize(index int, delta int64, sha1Hex string) {
	depot.rootLocks[index].Lock()
	defer depot.rootLocks[index].Unlock()

	depot.sizes[index] += delta

	if depot.sizes[index] < 0 {
		depot.sizes[index] = 0
	}

	if sha1Hex != "" && depot.bloomReady[index] {
		depot.bloomfilters[index].Add([]byte(sha1Hex))
	}

	depot.touched[index] = true
}
