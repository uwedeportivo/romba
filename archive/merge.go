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
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/glog"
	"github.com/uwedeportivo/romba/worker"
)

type mergeWorker struct {
	depot        *Depot
	hh           *Hashes
	md5crcBuffer []byte
	index        int
	pm           *mergeGru
}

type mergeGru struct {
	depot           *Depot
	resumePath      string
	numWorkers      int
	pt              worker.ProgressTracker
	soFar           chan *completed
	resumeLogFile   *os.File
	resumeLogWriter *bufio.Writer
	onlyneeded      bool
	skipInitialScan bool
}

func (depot *Depot) Merge(paths []string, resumePath string, onlyneeded bool, numWorkers int,
	logDir string, pt worker.ProgressTracker, skipInitialScan bool) (string, error) {

	resumeLogPath := filepath.Join(logDir, fmt.Sprintf("merge-resume-%s.log", time.Now().Format(ResumeDateFormat)))
	resumeLogFile, err := os.Create(resumeLogPath)
	if err != nil {
		return "", err
	}
	resumeLogWriter := bufio.NewWriter(resumeLogFile)

	resumePoint := ""
	if len(resumePath) > 0 {
		resumePoint, err = extractResumePoint(resumePath, numWorkers)
		if err != nil {
			return "", err
		}
	}

	glog.Infof("resuming with path %s", resumePoint)

	pm := new(mergeGru)
	pm.depot = depot
	pm.resumePath = resumePoint
	pm.pt = pt
	pm.numWorkers = numWorkers
	pm.soFar = make(chan *completed)
	pm.resumeLogWriter = resumeLogWriter
	pm.resumeLogFile = resumeLogFile
	pm.onlyneeded = onlyneeded
	pm.skipInitialScan = skipInitialScan

	go loopObserver(pm.numWorkers, pm.soFar, pm.depot, pm.resumeLogWriter)

	return worker.Work("merge roms", paths, pm)
}

func (pm *mergeGru) Accept(path string) bool {
	ext := filepath.Ext(path)
	if ext != gzipSuffix {
		return false
	}

	if pm.resumePath != "" {
		return path > pm.resumePath
	}
	return true
}

func (pm *mergeGru) NewWorker(workerIndex int) worker.Worker {
	return &mergeWorker{
		depot:        pm.depot,
		hh:           newHashes(),
		md5crcBuffer: make([]byte, md5.Size+crc32.Size+8),
		index:        workerIndex,
		pm:           pm,
	}
}

func (pm *mergeGru) CalculateWork() bool {
	return !pm.skipInitialScan
}

func (pm *mergeGru) NeedsSizeInfo() bool {
	return true
}

func (pm *mergeGru) NumWorkers() int {
	return pm.numWorkers
}

func (pm *mergeGru) ProgressTracker() worker.ProgressTracker {
	return pm.pt
}

func (pm *mergeGru) FinishUp() error {
	pm.soFar <- &completed{
		workerIndex: -1,
	}

	pm.depot.writeSizes()
	pm.resumeLogWriter.Flush()

	return pm.resumeLogFile.Close()
}

func (pm *mergeGru) Start() error {
	return nil
}

func (pm *mergeGru) Scanned(numFiles int, numBytes int64, commonRootPath string) {}

func (w *mergeWorker) Process(path string, size int64) error {
	var err error

	err = w.mergeGzip(path, size)
	if err != nil {
		return err
	}

	w.pm.soFar <- &completed{
		path:        path,
		workerIndex: w.index,
	}
	return nil
}

func (w *mergeWorker) Close() error {
	return nil
}

func (w *mergeWorker) mergeGzip(path string, size int64) error {
	rom, err := RomFromGZDepotFile(path)
	if err != nil {
		return err
	}

	sha1Hex := hex.EncodeToString(rom.Sha1)
	exists, _, err := w.pm.depot.RomInDepot(sha1Hex)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	hh, rSize, err := HashesFromGZHeader(path, w.md5crcBuffer)
	if err != nil {
		return err
	}

	rom.Md5 = hh.Md5
	rom.Crc = hh.Crc

	rom.Size = rSize
	rom.Path = path

	if w.pm.onlyneeded {
		dats, err := w.depot.RomDB.DatsForRom(rom)
		if err != nil {
			return err
		}

		if len(dats) == 0 {
			return nil
		}
	}

	err = w.depot.RomDB.IndexRom(rom)
	if err != nil {
		return err
	}

	root, err := w.depot.reserveRoot(size)
	if err != nil {
		return err
	}

	outpath := pathFromSha1HexEncoding(w.depot.roots[root].path, sha1Hex, gzipSuffix)

	err = worker.Cp(path, outpath)
	if err != nil {
		return err
	}

	w.depot.adjustSize(root, size, sha1Hex)
	return nil
}
