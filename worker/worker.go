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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"

	"github.com/uwedeportivo/romba/config"
)

type countVisitor struct {
	numBytes       int64
	numFiles       int
	commonRootPath string
	master         Master
}

func CommonRoot(pa, pb string) string {
	if pa == "" || pb == "" {
		return ""
	}

	pac := filepath.Clean(pa)
	pbc := filepath.Clean(pb)

	va := filepath.VolumeName(pac)
	vb := filepath.VolumeName(pbc)

	if va != vb {
		return ""
	}

	sa := pac[len(va):]
	sb := pbc[len(vb):]

	na := len(sa)
	nb := len(sb)

	var cursor, lastSep int
	lastSep = -1

	for {
		if cursor < na && cursor < nb && sa[cursor] == sb[cursor] {
			if sa[cursor] == filepath.Separator {
				lastSep = cursor
			}
			cursor++
		} else {
			break
		}
	}

	if cursor == na && na == nb {
		return pac
	}

	if cursor == na && na < nb && sb[na] == filepath.Separator {
		return pac
	}

	if cursor == nb && nb < na && sa[nb] == filepath.Separator {
		return pbc
	}

	if lastSep == -1 {
		return va + string(filepath.Separator)
	}

	res := pac[0 : len(va)+lastSep]

	if res == "" && filepath.Separator == '/' {
		return "/"
	}

	return res
}

func (cv *countVisitor) visit(path string, f os.FileInfo, err error) error {
	if f == nil || f.Name() == ".DS_Store" {
		return nil
	}
	if !f.IsDir() && cv.master.Accept(path) {
		glog.V(2).Infof("visiting path %s, current common root is %s", path, cv.commonRootPath)
		cv.numFiles += 1
		cv.numBytes += f.Size()
		if cv.commonRootPath == "" {
			cv.commonRootPath = path
		} else {
			cv.commonRootPath = CommonRoot(cv.commonRootPath, path)
		}
		glog.V(2).Infof("new current common root is %s", cv.commonRootPath)
	}
	return nil
}

type scanVisitor struct {
	inwork chan *workUnit
	master Master
	pt     ProgressTracker
}

var scanStopped = errors.New("scan stopped")

func (sv *scanVisitor) visit(path string, f os.FileInfo, err error) error {
	if sv.pt.Stopped() {
		glog.Info("scan stopped")
		return scanStopped
	}
	if f == nil || f.Name() == ".DS_Store" {
		return nil
	}
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

type ErrorHandler interface {
	Handle(path string)
}

type Master interface {
	Accept(path string) bool
	NewWorker(workerIndex int) Worker
	NumWorkers() int
	ProgressTracker() ProgressTracker
	FinishUp() error
	Start() error
	Scanned(numFiles int, numBytes int64, commonRootPath string)
	CalculateWork() bool
}

type workUnit struct {
	path string
	size int64
}

type slave struct {
	closeC chan error
	pt     ProgressTracker
	worker Worker
}

func Cp(src, dst string) error {
	dstDir := filepath.Dir(dst)
	err := os.MkdirAll(dstDir, 0777)
	if err != nil {
		return err
	}
	cmd := exec.Command("cp", src, dst)
	err = cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func Mv(src, dst string) error {
	dstDir := filepath.Dir(dst)
	err := os.MkdirAll(dstDir, 0777)
	if err != nil {
		return err
	}
	cmd := exec.Command("mv", src, dst)
	err = cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func handleErredFile(path string) {
	dstroot := config.GlobalConfig.General.BadDir
	commonPrefix := CommonRoot(path, dstroot)
	srcSuffix := strings.TrimPrefix(path, commonPrefix)
	dst := filepath.Join(dstroot, srcSuffix)
	glog.Infof("copying bad file %s to %s", path, dst)
	err := Cp(path, dst)
	if err != nil {
		glog.Errorf("failed to handle erred file %s: %v", path, err)
	}
}

func runSlave(w *slave, inwork <-chan *workUnit, workerNum int, workname string) {
	glog.Infof("starting worker %d for %s", workerNum, workname)
	var perr error
	for wu := range inwork {
		path := wu.path

		if glog.V(3) {
			glog.Infof("processing file %s", path)
		}

		erred := false
		err := w.worker.Process(path, wu.size)
		if err != nil {
			erred = true
			glog.Errorf("failed to process %s: %v", path, err)
			if perr == nil {
				perr = err
			}
			handleErredFile(path)
		}

		w.pt.AddBytesFromFile(wu.size, erred)

		if glog.V(3) {
			glog.Infof("finished processing file %s", path)
		}
	}

	err := w.worker.Close()
	if err != nil {
		glog.Errorf("failed to close worker: %v", err)
	}

	w.closeC <- perr
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

	var cv *countVisitor

	if master.CalculateWork() {
		cv = new(countVisitor)
		cv.master = master

		for k, name := range paths {
			if !filepath.IsAbs(name) {
				absname, err := filepath.Abs(name)
				if err != nil {
					return "", err
				}
				paths[k] = absname
			}
		}

		for _, name := range paths {
			glog.Infof("initial scan of %s to determine amount of work\n", name)

			err := filepath.Walk(name, cv.visit)
			if err != nil {
				glog.Errorf("failed to count in dir %s: %v\n", name, err)
				return "", err
			}
		}

		glog.Infof("found %d files and %s to do. starting work...\n", cv.numFiles, humanize.IBytes(uint64(cv.numBytes)))

		master.Scanned(cv.numFiles, cv.numBytes, cv.commonRootPath)

		pt.SetTotalBytes(cv.numBytes)
		pt.SetTotalFiles(int32(cv.numFiles))
	}

	inwork := make(chan *workUnit, master.NumWorkers())

	sv := &scanVisitor{
		inwork: inwork,
		master: master,
		pt:     pt,
	}

	closeC := make(chan error, master.NumWorkers())

	for i := 0; i < master.NumWorkers(); i++ {
		worker := &slave{
			pt:     pt,
			worker: master.NewWorker(i),
			closeC: closeC,
		}

		go runSlave(worker, inwork, i, workname)
	}

	for _, name := range paths {
		if pt.Stopped() {
			break
		}
		err := filepath.Walk(name, sv.visit)
		if err == scanStopped {
			break
		}
		if err != nil {
			glog.Errorf("failed to scan dir %s: %v\n", name, err)

			close(inwork)
			pt.Finished()

			glog.Infof("Flushing workers and closing work. Hang in there...\n")
			for i := 0; i < master.NumWorkers(); i++ {
				perr := <-closeC
				if perr != nil {
					glog.Errorf("master found worker error %v", perr)
				}
			}
			return "", err
		}
	}

	close(inwork)

	var perr error
	for i := 0; i < master.NumWorkers(); i++ {
		err := <-closeC
		if err != nil {
			glog.Errorf("master found worker error %v", err)
			if perr == nil {
				perr = err
			}
		}
	}

	pt.Finished()

	err = master.FinishUp()
	if err != nil {
		glog.Errorf("failed to finish up master: %v\n", err)
		return "", err
	}

	if perr != nil {
		glog.Infof("Worker errors happened. First error was %v.\n", perr)
	}

	glog.Infof("Done.\n")

	elapsed := time.Since(startTime)

	if pt.Stopped() {
		return "Cancelled " + workname, nil
	}

	var endMsg bytes.Buffer

	pgr := pt.GetProgress()

	endMsg.WriteString(fmt.Sprintf("finished %s\n", workname))
	if cv != nil {
		endMsg.WriteString(fmt.Sprintf("total number of files: %d\n", cv.numFiles))
	}
	endMsg.WriteString(fmt.Sprintf("number of files processed: %d\n", pgr.FilesSoFar))
	endMsg.WriteString(fmt.Sprintf("number of files with errors: %d\n", pgr.ErrorFiles))

	if cv != nil {
		endMsg.WriteString(fmt.Sprintf("total number of bytes: %s\n", humanize.IBytes(uint64(cv.numBytes))))
	}
	endMsg.WriteString(fmt.Sprintf("number of bytes processed: %s\n", humanize.IBytes(uint64(pgr.BytesSoFar))))
	endMsg.WriteString(fmt.Sprintf("elapsed time: %s\n", formatDuration(elapsed)))

	if cv != nil {
		ts := uint64(float64(cv.numBytes) / float64(elapsed.Seconds()))
		endMsg.WriteString(fmt.Sprintf("throughput: %s/s \n", humanize.IBytes(ts)))
	}

	endS := endMsg.String()

	glog.Info(endS)

	return endS, perr
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
