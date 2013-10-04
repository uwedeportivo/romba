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
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/uwedeportivo/torrentzip/czip"

	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
)

type Depot struct {
	roots    []string
	sizes    []int64
	maxSizes []int64
	romDB    db.RomDB
	lock     *sync.Mutex
	start    int
}

type completed struct {
	path        string
	workerIndex int
}

type archiveWorker struct {
	depot *Depot
	hh    *Hashes
	index int
	pm    *archiveMaster
}

type archiveMaster struct {
	depot           *Depot
	resumePath      string
	numWorkers      int
	pt              worker.ProgressTracker
	soFar           chan *completed
	resumeLogFile   *os.File
	resumeLogWriter *bufio.Writer
}

func NewDepot(roots []string, maxSize []int64, romDB db.RomDB) (*Depot, error) {
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
	depot.romDB = romDB
	depot.lock = new(sync.Mutex)
	return depot, nil
}

func (depot *Depot) Archive(paths []string, resumePath string, numWorkers int,
	logDir string, pt worker.ProgressTracker) (string, error) {

	resumeLogPath := filepath.Join(logDir, fmt.Sprintf("archive-resume-%s.log", time.Now().Format("2006-01-02-15_04_05")))
	resumeLogFile, err := os.Create(resumeLogPath)
	if err != nil {
		return "", err
	}
	resumeLogWriter := bufio.NewWriter(resumeLogFile)

	pm := new(archiveMaster)
	pm.depot = depot
	pm.resumePath = resumePath
	pm.pt = pt
	pm.numWorkers = numWorkers
	pm.soFar = make(chan *completed)
	pm.resumeLogWriter = resumeLogWriter
	pm.resumeLogFile = resumeLogFile

	go pm.loopObserver(resumeLogWriter)

	return worker.Work("archive roms", paths, pm)
}

func (pm *archiveMaster) Accept(path string) bool {
	if pm.resumePath != "" {
		return path > pm.resumePath
	}
	return true
}

func (pm *archiveMaster) NewWorker(workerIndex int) worker.Worker {
	return &archiveWorker{
		depot: pm.depot,
		hh:    newHashes(),
		index: workerIndex,
		pm:    pm,
	}
}

func (pm *archiveMaster) NumWorkers() int {
	return pm.numWorkers
}

func (pm *archiveMaster) ProgressTracker() worker.ProgressTracker {
	return pm.pt
}

func (pm *archiveMaster) FinishUp() error {
	pm.soFar <- &completed{
		workerIndex: -1,
	}

	pm.depot.writeSizes()
	pm.resumeLogWriter.Flush()

	return pm.resumeLogFile.Close()
}

func (pm *archiveMaster) Start() error {
	return nil
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
}

func (w *archiveWorker) Process(path string, size int64) error {
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
	rom.Crc = make([]byte, crc32.Size)
	rom.Md5 = make([]byte, md5.Size)
	rom.Sha1 = make([]byte, sha1.Size)
	copy(rom.Crc, w.hh.Crc)
	copy(rom.Md5, w.hh.Md5)
	copy(rom.Sha1, w.hh.Sha1)
	rom.Name = filepath.Base(path)
	rom.Size = size
	rom.Path = path

	err = w.depot.romDB.IndexRom(rom)
	if err != nil {
		return err
	}

	w.pm.soFar <- &completed{
		path:        path,
		workerIndex: w.index,
	}
	return nil
}

func (w *archiveWorker) Close() error {
	return nil
}

type readerOpener func() (io.ReadCloser, error)

func (w *archiveWorker) archive(ro readerOpener, size int64) (int64, error) {
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

	sha1Hex := hex.EncodeToString(w.hh.Sha1)

	root, err := w.depot.reserveRoot(size)
	if err != nil {
		return 0, err
	}

	outpath := pathFromSha1HexEncoding(w.depot.roots[root], sha1Hex, gzipSuffix)

	exists, err := PathExists(outpath)
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

func (w *archiveWorker) archiveZip(inpath string, size int64, addZipItself bool) (int64, error) {
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

func (w *archiveWorker) archiveRom(inpath string, size int64) (int64, error) {
	return w.archive(func() (io.ReadCloser, error) { return os.Open(inpath) }, size)
}

func (pm *archiveMaster) loopObserver(writer io.Writer) {
	ticker := time.NewTicker(time.Minute * 1)
	comps := make([]string, pm.numWorkers)

	for {
		select {
		case comp := <-pm.soFar:
			if comp.workerIndex == -1 {
				break
			}
			comps[comp.workerIndex] = comp.path
		case <-ticker.C:
			if comps[0] != "" {
				sort.Strings(comps)
				fmt.Fprint(writer, "%s\n", comps[0])
				pm.depot.writeSizes()
			}
		}
	}

	ticker.Stop()
}
