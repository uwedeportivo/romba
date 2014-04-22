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
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/golang/glog"

	"github.com/dustin/go-humanize"

	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
)

type Depot struct {
	roots    []string
	sizes    []int64
	maxSizes []int64
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
			humanize.Bytes(uint64(depot.maxSizes[k])), humanize.Bytes(uint64(depot.sizes[k])))
	}

	depot.romDB = romDB
	depot.lock = new(sync.Mutex)
	glog.Info("Depot init finished")
	return depot, nil
}

func (depot *Depot) SHA1InDepot(sha1Hex string) (bool, error) {
	for _, root := range depot.roots {
		rompath := pathFromSha1HexEncoding(root, sha1Hex, gzipSuffix)
		exists, err := PathExists(rompath)
		if err != nil {
			return false, err
		}

		if exists {
			return true, nil
		}
	}
	return false, nil
}

func (depot *Depot) OpenRomGZ(rom *types.Rom) (io.ReadCloser, error) {
	if rom.Sha1 == nil {
		return nil, fmt.Errorf("cannot open rom %s because SHA1 is missing", rom.Name)
	}

	if len(rom.Sha1) == sha1.Size {
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
	} else {
		if glog.V(2) {
			glog.Infof("searching for the right file for rom %s because of hash collisions", rom.Name)
		}
		for i := 0; i < len(rom.Sha1); i += sha1.Size {
			sha1Hex := hex.EncodeToString(rom.Sha1[i : i+sha1.Size])

			if glog.V(3) {
				glog.Infof("trying SHA1 %s", sha1Hex)
			}

			for _, root := range depot.roots {
				rompath := pathFromSha1HexEncoding(root, sha1Hex, gzipSuffix)
				exists, err := PathExists(rompath)
				if err != nil {
					return nil, err
				}

				if exists {
					// double check that it matches crc or md5
					if rom.Crc != nil || rom.Md5 != nil {
						hh, err := HashesForGZFile(rompath)
						if err != nil {
							return nil, err
						}

						if rom.Md5 != nil && bytes.Equal(rom.Md5, hh.Md5) {
							return os.Open(rompath)
						}

						if rom.Crc != nil && bytes.Equal(rom.Crc, hh.Crc) {
							return os.Open(rompath)
						}

					} else {
						if glog.V(2) {
							glog.Warningf("rom %s with collision SHA1 and no other hash to disambigue", rom.Name)
						}
						return os.Open(rompath)
					}
				}
			}
		}
	}

	return nil, nil
}

func (depot *Depot) writeSizes() {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	for k, root := range depot.roots {
		err := writeSizeFile(root, depot.sizes[k])
		if err != nil {
			glog.Errorf("failed to write size file into %s: %v\n", root, err)
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
}
