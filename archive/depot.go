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
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
	"github.com/uwedeportivo/torrentzip/czip"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Depot struct {
	roots      []string
	sizes      []int64
	maxSizes   []int64
	resumePath string
	numWorkers int
	romDB      db.RomDB
	soFar      chan *completed
	lock       *sync.Mutex
	start      int
}

type completed struct {
	path        string
	workerIndex int
}

type slave struct {
	depot *Depot
	hh    *hashes
	index int
}

func NewDepot(roots []string, maxSize []int64, romDB db.RomDB, numWorkers int) (*Depot, error) {
	depot := new(Depot)
	depot.roots = make([]string, len(roots))
	depot.sizes = make([]int64, len(roots))
	depot.maxSizes = make([]int64, len(roots))

	copy(depot.roots, roots)
	copy(depot.maxSizes, maxSize)

	for k, root := range depot.roots {
		size, err := establishSize(root)
		if err != nil {
			return nil, err
		}
		depot.sizes[k] = size
	}
	depot.soFar = make(chan *completed)
	depot.lock = new(sync.Mutex)
	depot.romDB = romDB
	depot.numWorkers = numWorkers
	return depot, nil
}

func (depot *Depot) Archive(paths []string, resumePath string, resumeLog *log.Logger, archiveLog *log.Logger) error {
	depot.resumePath = resumePath

	go depot.loopObserver(resumeLog)

	err := worker.Work("archive roms", paths, depot, archiveLog)

	depot.soFar <- &completed{
		workerIndex: -1,
	}

	if err != nil {
		return err
	}

	for k, root := range depot.roots {
		err := writeSizeFile(root, depot.sizes[k])
		if err != nil {
			return err
		}
	}
	return nil
}

func (depot *Depot) Accept(path string) bool {
	if depot.resumePath != "" {
		return path > depot.resumePath
	}
	return true
}

func (depot *Depot) NewWorker(index int) worker.Worker {
	return &slave{
		depot: depot,
		hh:    newHashes(),
		index: index,
	}
}

func (depot *Depot) NumWorkers() int {
	return depot.numWorkers
}

func (depot *Depot) reserveRoot(size int64) (int, error) {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	for i := depot.start; i < len(depot.roots); i++ {
		if depot.sizes[i]+size < depot.maxSizes[i] {
			depot.sizes[i] += size
			return i, nil
		} else if depot.sizes[i] >= depot.maxSizes[i] {
			depot.start = i
		}
	}
	return -1, fmt.Errorf("out of disk space")
}

func (depot *Depot) adjustSize(index int, delta int64) {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	depot.sizes[index] += delta
}

func (w *slave) Process(path string, size int64, logger *log.Logger) error {
	var err error

	if filepath.Ext(path) == zipSuffix {
		_, err = w.archiveZip(path, size, false)
	} else {
		_, err = w.archiveRom(path, size)
	}

	if err != nil {
		return err
	}

	rom := new(types.Rom)
	rom.Crc = make([]byte, 0, crc32.Size)
	rom.Md5 = make([]byte, 0, md5.Size)
	rom.Sha1 = make([]byte, 0, sha1.Size)
	copy(rom.Crc, w.hh.crc)
	copy(rom.Md5, w.hh.md5)
	copy(rom.Sha1, w.hh.sha1)
	rom.Name = filepath.Base(path)
	rom.Size = size
	rom.Path = path

	err = w.depot.romDB.IndexRom(rom)
	if err != nil {
		return err
	}

	w.depot.soFar <- &completed{
		path:        path,
		workerIndex: w.index,
	}
	return nil
}

func (w *slave) Close() error {
	return nil
}

type readerOpener func() (io.ReadCloser, error)

func (w *slave) archive(ro readerOpener, size int64) (int64, error) {
	r, err := ro()
	if err != nil {
		return 0, err
	}

	br := bufio.NewReader(r)

	err = w.hh.forReader(br)
	if err != nil {
		r.Close()
		return 0, err
	}
	err = r.Close()
	if err != nil {
		return 0, err
	}

	sha1Hex := hex.EncodeToString(w.hh.sha1)

	root, err := w.depot.reserveRoot(size)
	if err != nil {
		return 0, err
	}

	outpath := pathFromSha1HexEncoding(w.depot.roots[root], sha1Hex, gzipSuffix)

	exists, err := pathExists(outpath)
	if err != nil {
		return 0, err
	}

	if exists {
		return 0, nil
	}

	r, err = ro()
	if err != nil {
		return 0, err
	}
	defer r.Close()

	compressedSize, err := archive(outpath, r)
	if err != nil {
		return 0, err
	}

	w.depot.adjustSize(root, compressedSize-size)
	return compressedSize, nil
}

func (w *slave) archiveZip(inpath string, size int64, addZipItself bool) (int64, error) {
	zr, err := czip.OpenReader(inpath)
	if err != nil {
		return 0, err
	}
	defer zr.Close()

	var compressedSize int64

	for _, zf := range zr.File {
		cs, err := w.archive(func() (io.ReadCloser, error) { return zf.Open() }, size)
		if err != nil {
			return 0, err
		}
		compressedSize += cs
	}

	if addZipItself {
		cs, err := w.archive(func() (io.ReadCloser, error) { return os.Open(inpath) }, size)
		if err != nil {
			return 0, err
		}
		compressedSize += cs
	}
	return compressedSize, nil
}

func (w *slave) archiveRom(inpath string, size int64) (int64, error) {
	return w.archive(func() (io.ReadCloser, error) { return os.Open(inpath) }, size)
}

func (depot *Depot) loopObserver(logger *log.Logger) {
	ticker := time.NewTicker(time.Minute * 1)
	comps := make([]string, depot.numWorkers)

	for {
		select {
		case comp := <-depot.soFar:
			if comp.workerIndex == -1 {
				break
			}
			comps[comp.workerIndex] = comp.path
		case <-ticker.C:
			if comps[0] != "" {
				sort.Strings(comps)
				logger.Println(comps[0])

				depot.lock.Lock()

				for k, root := range depot.roots {
					err := writeSizeFile(root, depot.sizes[k])
					if err != nil {
						fmt.Printf("failed to write size file into %s: %v\n", root, err)
					}
				}
				depot.lock.Unlock()
			}
		}
	}

	ticker.Stop()
}
