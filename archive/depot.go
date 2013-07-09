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
	Roots      []string
	Sizes      []int64
	MaxSizes   []int64
	ResumePath string
	NumWorkers int
	toDat      chan *types.Rom
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

func (depot *Depot) accept(path string) bool {
	if depot.ResumePath != "" {
		return path > depot.ResumePath
	}
	return true
}

func (depot *Depot) newWorker(index int) worker.Worker {
	return &slave{
		depot: depot,
		hh:    newHashes(),
		index: index,
	}
}

func (depot *Depot) reserveRoot(size int64) string {
	depot.lock.Lock()
	defer depot.lock.Unlock()

	for i := depot.start; i < len(depot.Roots); i++ {
		if depot.Sizes[i]+size < depot.MaxSizes[i] {
			depot.Sizes[i] += size
			return depot.Roots[i]
		} else if depot.Sizes[i] >= depot.MaxSizes[i] {
			depot.start = i
		}
	}
	panic("Aborting archiving: Out of disk space")
}

func (w *slave) Process(path string, size int64, logger *log.Logger) error {
	var err error

	if filepath.Ext(path) == zipSuffix {
		err = w.archiveZip(path, size, false)
	} else {
		err = w.archiveRom(path, size)
	}

	if err != nil {
		// TODO: mark file as corrupted
		return err
	} else {
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

		w.depot.toDat <- rom
	}

	w.depot.soFar <- &completed{
		path:        path,
		workerIndex: w.index,
	}
	return nil
}

type readerOpener func() (io.ReadCloser, error)

func (w *slave) archive(ro readerOpener, size int64) error {
	r, err := ro()
	if err != nil {
		return err
	}

	br := bufio.NewReader(r)

	err = w.hh.forReader(br)
	if err != nil {
		r.Close()
		return err
	}
	err = r.Close()
	if err != nil {
		return err
	}

	sha1Hex := hex.EncodeToString(w.hh.sha1)

	root := w.depot.reserveRoot(size)

	outpath := pathFromSha1HexEncoding(root, sha1Hex, gzipSuffix)

	exists, err := pathExists(outpath)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	r, err = ro()
	if err != nil {
		return err
	}
	defer r.Close()

	return archive(outpath, r)
}

func (w *slave) archiveZip(inpath string, size int64, addZipItself bool) error {
	zr, err := czip.OpenReader(inpath)
	if err != nil {
		return err
	}
	defer zr.Close()

	for _, zf := range zr.File {
		err = w.archive(func() (io.ReadCloser, error) { return zf.Open() }, size)
		if err != nil {
			return err
		}
	}

	if addZipItself {
		return w.archive(func() (io.ReadCloser, error) { return os.Open(inpath) }, size)
	}
	return nil
}

func (w *slave) archiveRom(inpath string, size int64) error {
	return w.archive(func() (io.ReadCloser, error) { return os.Open(inpath) }, size)
}

func (depot *Depot) runArchiveObserver() {
	ticker := time.NewTicker(time.Minute * 5)
	comps := make([]string, depot.NumWorkers)

	for {
		select {
		case comp := <-depot.soFar:
			if comp.workerIndex == -1 {
				break
			}
			comps[comp.workerIndex] = comp.path
		case <-ticker.C:
			sort.Strings(comps)
			// TODO(uwe):
			// resumepath is comps[0]
		}
	}

	ticker.Stop()
}
