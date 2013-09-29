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

package worker

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
)

type countVisitor struct {
	numBytes int64
	numFiles int
	master   Master
}

func (cv *countVisitor) visit(path string, f os.FileInfo, err error) error {
	if !f.IsDir() && cv.master.Accept(path) {
		cv.numFiles += 1
		cv.numBytes += f.Size()
	}
	return nil
}

type scanVisitor struct {
	inwork chan *workUnit
	master Master
}

func (sv *scanVisitor) visit(path string, f os.FileInfo, err error) error {
	if !f.IsDir() && sv.master.Accept(path) {
		sv.inwork <- &workUnit{
			path: path,
			size: f.Size(),
		}
	}
	return nil
}

type Worker interface {
	Process(path string, size int64) error
	Close() error
}

type Master interface {
	Accept(path string) bool
	NewWorker(workerIndex int) Worker
	NumWorkers() int
	ProgressTracker() ProgressTracker
	FinishUp() error
	Start() error
}

type workUnit struct {
	path string
	size int64
}

type slave struct {
	wg      *sync.WaitGroup
	closeWg *sync.WaitGroup
	pt      ProgressTracker
	worker  Worker
}

func runSlave(w *slave, inwork <-chan *workUnit, workerNum int, workname string) {
	glog.Infof("starting worker %d for %s", workerNum, workname)
	for wu := range inwork {
		path := wu.path

		err := w.worker.Process(path, wu.size)
		if err != nil {
			glog.Errorf("failed to process %s: %v", path, err)
		}

		w.pt.AddBytesFromFile(wu.size)
		w.wg.Done()
	}
	err := w.worker.Close()
	if err != nil {
		glog.Errorf("failed to close worker: %v", err)
	}

	w.closeWg.Done()
	glog.Infof("exiting worker %d for %s", workerNum, workname)
}

func Work(workname string, paths []string, master Master) (string, error) {
	pt := master.ProgressTracker()

	glog.Infof("starting %s\n", workname)
	startTime := time.Now()

	err := master.Start()
	if err != nil {
		glog.Errorf("failed to start master: %v\n", err)
		return "", err
	}

	cv := new(countVisitor)
	cv.master = master

	for _, name := range paths {
		glog.Infof("initial scan of %s to determine amount of work\n", name)

		err := filepath.Walk(name, cv.visit)
		if err != nil {
			glog.Errorf("failed to count in dir %s: %v\n", name, err)
			return "", err
		}
	}

	glog.Infof("found %d files and %s to do. starting work...\n", cv.numFiles, humanize.Bytes(uint64(cv.numBytes)))

	pt.SetTotalBytes(cv.numBytes)
	pt.SetTotalFiles(int32(cv.numFiles))

	inwork := make(chan *workUnit)

	sv := &scanVisitor{
		inwork: inwork,
		master: master,
	}

	wg := new(sync.WaitGroup)
	wg.Add(cv.numFiles)
	closeWg := new(sync.WaitGroup)
	closeWg.Add(master.NumWorkers())

	for i := 0; i < master.NumWorkers(); i++ {
		worker := &slave{
			pt:      pt,
			worker:  master.NewWorker(i),
			wg:      wg,
			closeWg: closeWg,
		}

		go runSlave(worker, inwork, i, workname)
	}

	for _, name := range paths {
		err := filepath.Walk(name, sv.visit)
		if err != nil {
			glog.Errorf("failed to scan dir %s: %v\n", name, err)
			close(inwork)
			closeWg.Wait()
			return "", err
		}
	}

	wg.Wait()
	close(inwork)

	pt.Finished()

	glog.Infof("Flushing workers and closing work. Hang in there...\n")
	closeWg.Wait()

	err = master.FinishUp()
	if err != nil {
		glog.Errorf("failed to finish up master: %v\n", err)
		return "", err
	}

	glog.Infof("Done.\n")

	elapsed := time.Since(startTime)

	var endMsg bytes.Buffer

	endMsg.WriteString(fmt.Sprintf("finished %s\n", workname))
	endMsg.WriteString(fmt.Sprintf("total number of files: %d\n", cv.numFiles))
	endMsg.WriteString(fmt.Sprintf("total number of bytes: %s\n", humanize.Bytes(uint64(cv.numBytes))))
	endMsg.WriteString(fmt.Sprintf("elapsed time: %s\n", formatDuration(elapsed)))

	ts := uint64(float64(cv.numBytes) / float64(elapsed.Seconds()))

	endMsg.WriteString(fmt.Sprintf("throughput: %s/s \n", humanize.Bytes(ts)))

	endS := endMsg.String()

	glog.Info(endS)

	return endS, nil
}

func formatDuration(d time.Duration) string {
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
