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

package db

import (
	"bufio"
	"fmt"
	"github.com/uwedeportivo/romba/combine"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang/glog"

	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
)

const (
	generationFilename = "romba-generation"
	MaxBatchSize       = 10485760
)

type RomBatch interface {
	IndexRom(rom *types.Rom) error
	IndexDat(dat *types.Dat, sha1 []byte) error
	Size() int64
	Flush() error
	Close() error
}

type RomDB interface {
	StartBatch() RomBatch
	IndexRom(rom *types.Rom) error
	IndexDat(dat *types.Dat, sha1 []byte) error
	OrphanDats() error
	Flush()
	Close() error
	GetDat(sha1 []byte) (*types.Dat, error)
	IsRomReferencedByDats(rom *types.Rom) (bool, error)
	DatsForRom(rom *types.Rom) ([]*types.Dat, error)
	FilteredDatsForRom(rom *types.Rom, filter func(*types.Dat) bool) ([]*types.Dat, []*types.Dat, error)
	CompleteRom(rom *types.Rom) ([]*types.Rom, error)
	BeginDatRefresh() error
	EndDatRefresh() error
	PrintStats() string
	Generation() int64
	DebugGet(key []byte, size int64) string
	ResolveHash(key []byte) ([]byte, error)
	ForEachDat(datF func(dat *types.Dat) error) error
	JoinCrcMd5(combiner combine.Combiner) error
	NumRoms() int64
}

var Factory func(path string) (RomDB, error)

func FormatDuration(d time.Duration) string {
	secs := uint64(d.Seconds())
	mins := secs / 60
	secs = secs % 60
	hours := mins / 60
	mins = mins % 60

	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, mins, secs)
	}

	if mins > 0 {
		return fmt.Sprintf("%dm%ds", mins, secs)
	}
	return fmt.Sprintf("%ds", secs)
}

func New(path string) (RomDB, error) {
	glog.Infof("Loading DB")
	startTime := time.Now()

	db, err := Factory(path)

	elapsed := time.Since(startTime)

	glog.Infof("Done Loading DB in %s", FormatDuration(elapsed))

	return db, err
}

func WriteGenerationFile(root string, size int64) error {
	file, err := os.Create(filepath.Join(root, generationFilename))
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			glog.Errorf("error, failed to close generation file at %s: %v", root, err)
		}
	}()

	bw := bufio.NewWriter(file)
	defer func() {
		err := bw.Flush()
		if err != nil {
			glog.Errorf("error, failed to flush generation file at %s: %v", root, err)
		}
	}()

	_, err = bw.WriteString(strconv.FormatInt(size, 10))
	return err
}

func ReadGenerationFile(root string) (int64, error) {
	file, err := os.Open(filepath.Join(root, generationFilename))
	if err != nil {
		if os.IsNotExist(err) {
			err = WriteGenerationFile(root, 0)
			if err != nil {
				return 0, err
			}
			return 0, nil
		}
		return 0, err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			glog.Errorf("error, failed to close generation file at %s: %v", root, err)
		}
	}()

	bs, err := ioutil.ReadAll(file)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(bs), 10, 64)
}

type refreshWorker struct {
	romBatch RomBatch
	pm       *refreshGru
}

func (pw *refreshWorker) Process(path string, size int64) error {
	if pw.romBatch.Size() >= MaxBatchSize {
		glog.V(3).Infof("flushing batch of size %d", pw.romBatch.Size())
		err := pw.romBatch.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush: %v", err)
		}
	}
	dat, sha1Bytes, err := parser.Parse(path)
	if err != nil {
		return err
	}

	if pw.pm.missingSha1sWriter != nil && dat.MissingSha1s {
		_, err = fmt.Fprintln(pw.pm.missingSha1sWriter, dat.Path)
		if err != nil {
			return err
		}
	}

	return pw.romBatch.IndexDat(dat, sha1Bytes)
}

func (pw *refreshWorker) Close() error {
	err := pw.romBatch.Close()
	pw.romBatch = nil
	return err
}

type refreshGru struct {
	romdb              RomDB
	numWorkers         int
	pt                 worker.ProgressTracker
	missingSha1sWriter io.Writer
}

func (pm *refreshGru) CalculateWork() bool {
	return true
}

func (pm *refreshGru) NeedsSizeInfo() bool {
	return false
}

func (pm *refreshGru) Accept(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".dat" || ext == ".xml"
}

func (pm *refreshGru) NewWorker(workerIndex int) worker.Worker {
	return &refreshWorker{
		romBatch: pm.romdb.StartBatch(),
		pm:       pm,
	}
}

func (pm *refreshGru) NumWorkers() int {
	return pm.numWorkers
}

func (pm *refreshGru) ProgressTracker() worker.ProgressTracker {
	return pm.pt
}

func (pm *refreshGru) FinishUp() error {
	pm.romdb.Flush()

	return pm.romdb.EndDatRefresh()
}

func (pm *refreshGru) Start() error {
	return pm.romdb.BeginDatRefresh()
}

func (pm *refreshGru) Scanned(numFiles int, numBytes int64, commonRootPath string) {}

func Refresh(romdb RomDB, datsPath string, numWorkers int, pt worker.ProgressTracker, missingSha1s string) (string, error) {
	err := romdb.OrphanDats()
	if err != nil {
		return "", err
	}

	var missingSha1sWriter io.Writer

	if missingSha1s != "" {
		missingSha1sFile, err := os.Create(missingSha1s)
		if err != nil {
			return "", err
		}
		defer func() {
			err := missingSha1sFile.Close()
			if err != nil {
				glog.Errorf("error, failed to close missing sha1 file %s: %v", missingSha1s, err)
			}
		}()

		missingSha1sBuf := bufio.NewWriter(missingSha1sFile)
		defer func() {
			err := missingSha1sBuf.Flush()
			if err != nil {
				glog.Errorf("error, failed to flush missing sha1 file %s: %v", missingSha1s, err)
			}
		}()

		missingSha1sWriter = missingSha1sBuf
	}

	pm := &refreshGru{
		romdb:              romdb,
		numWorkers:         numWorkers,
		pt:                 pt,
		missingSha1sWriter: missingSha1sWriter,
	}

	return worker.Work("refresh dats", []string{datsPath}, pm)
}
