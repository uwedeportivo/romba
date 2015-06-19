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
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/uwedeportivo/torrentzip/cgzip"

	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/util"
)

type Depot struct {
	roots    []string
	sizes    []int64
	maxSizes []int64
	touched  []bool
	romDB    db.RomDB
	lock     *sync.Mutex
	// where in the depot to reserve the next space
	// when archiving
	start int
}

func NewDepot(roots []string, maxSize []int64, romDB db.RomDB) (*Depot, error) {
	glog.Info("Depot init")
	depot := new(Depot)
	depot.roots = make([]string, len(roots))
	depot.sizes = make([]int64, len(roots))
	depot.maxSizes = make([]int64, len(roots))
	depot.touched = make([]bool, len(roots))

	copy(depot.roots, roots)
	copy(depot.maxSizes, maxSize)

	for k, root := range depot.roots {
		glog.Infof("establishing size of %s", root)
		size, err := establishSize(root)
		if err != nil {
			return nil, err
		}
		depot.sizes[k] = size
	}

	glog.Info("Initializing Depot with the following roots")

	for k, root := range depot.roots {
		glog.Infof("root = %s, maxSize = %s, size = %s", root,
			humanize.IBytes(uint64(depot.maxSizes[k])), humanize.IBytes(uint64(depot.sizes[k])))
	}

	depot.romDB = romDB
	depot.lock = new(sync.Mutex)
	glog.Info("Depot init finished")
	return depot, nil
}

func (depot *Depot) RomInDepot(sha1Hex string) (bool, string, error) {
	for _, root := range depot.roots {
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
	for _, root := range depot.roots {
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

			gzr, err := cgzip.NewReader(romGZ)
			if err != nil {
				return false, nil, "", 0, err
			}
			defer gzr.Close()

			md5crcBuffer := make([]byte, md5.Size+crc32.Size+8)
			err = gzr.RequestExtraHeader(md5crcBuffer)
			if err != nil {
				return false, nil, "", 0, err
			}

			gzbuf := make([]byte, 1024)
			gzr.Read(gzbuf)

			md5crcBuffer = gzr.GetExtraHeader()

			if len(md5crcBuffer) == md5.Size+crc32.Size+8 {
				hh.Md5 = make([]byte, md5.Size)
				copy(hh.Md5, md5crcBuffer[:md5.Size])
				hh.Crc = make([]byte, crc32.Size)
				copy(hh.Crc, md5crcBuffer[md5.Size:md5.Size+crc32.Size])
				size = util.BytesToInt64(md5crcBuffer[md5.Size+crc32.Size:])
			} else {
				glog.Warningf("rom %s has missing gzip md5 or crc header", rompath)
			}

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
	depot.lock.Lock()
	defer depot.lock.Unlock()

	for k, root := range depot.roots {
		if depot.touched[k] {
			err := writeSizeFile(root, depot.sizes[k])
			if err != nil {
				glog.Errorf("failed to write size file into %s: %v\n", root, err)
			} else {
				depot.touched[k] = false
			}
		}
	}
}

func (depot *Depot) adjustSize(index int, delta int64) {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	depot.sizes[index] += delta

	if depot.sizes[index] < 0 {
		depot.sizes[index] = 0
	}

	depot.touched[index] = true
}
