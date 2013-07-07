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
	"fmt"
	"github.com/cheggaaa/pb"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	megabyte = 1 << 20
)

type countVisitor struct {
	numBytes int64
	numFiles int
	acceptFn AcceptFun
}

func (cv *countVisitor) visit(path string, f os.FileInfo, err error) error {
	if !f.IsDir() && cv.acceptFn(path) {
		cv.numFiles += 1
		cv.numBytes += f.Size()
	}
	return nil
}

type scanVisitor struct {
	inwork   chan *workUnit
	acceptFn AcceptFun
}

func (sv *scanVisitor) visit(path string, f os.FileInfo, err error) error {
	if !f.IsDir() && sv.acceptFn(path) {
		sv.inwork <- &workUnit{
			path: path,
			size: f.Size(),
		}
	}
	return nil
}

type WorkFun func(path string) error
type AcceptFun func(path string) bool

type workUnit struct {
	path string
	size int64
}

type slave struct {
	logger       *log.Logger
	wg           *sync.WaitGroup
	inwork       chan *workUnit
	byteProgress *pb.ProgressBar
	workFn       WorkFun
}

func (w *slave) run() {
	var truemb float64
	for wu := range w.inwork {
		path := wu.path

		err := w.workFn(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to process %s: %v\n", path, err)
			if w.logger != nil {
				w.logger.Printf("failed to process %s: %v\n", path, err)
			}
		}

		truemb += float64(wu.size) / float64(megabyte)

		if w.byteProgress != nil {
			if truemb >= 1.0 {
				floor := math.Floor(truemb)
				delta := truemb - floor
				v := int(floor)
				w.byteProgress.Add(v)
				truemb = delta
			}
		}
		w.wg.Done()
	}
}

func Work(paths []string, acceptFn AcceptFun, workFn WorkFun, numSlaves int, logger *log.Logger) error {
	cv := new(countVisitor)
	cv.acceptFn = acceptFn

	for _, name := range paths {
		fmt.Fprintf(os.Stdout, "initial scan of %s to determine amount of work\n", name)

		err := filepath.Walk(name, cv.visit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to count in dir %s: %v\n", name, err)
			if logger != nil {
				logger.Printf("failed to count in dir %s: %v\n", name, err)
			}
			return err
		}
	}

	mg := int(cv.numBytes / megabyte)

	fmt.Fprintf(os.Stdout, "found %d files and %d MB to do. starting work...\n", cv.numFiles, mg)
	if logger != nil {
		logger.Printf("found %d files and %d MB to do. starting work...\n", cv.numFiles, mg)
	}

	var byteProgress *pb.ProgressBar

	if mg > 10 {
		pb.BarStart = "MB ["

		byteProgress = pb.New(mg)
		byteProgress.RefreshRate = 5 * time.Second
		byteProgress.ShowCounters = true
		byteProgress.Start()
	}

	inwork := make(chan *workUnit)

	sv := &scanVisitor{
		inwork:   inwork,
		acceptFn: acceptFn,
	}

	wg := new(sync.WaitGroup)
	wg.Add(cv.numFiles)

	for i := 0; i < numSlaves; i++ {
		worker := &slave{
			byteProgress: byteProgress,
			inwork:       inwork,
			logger:       logger,
			workFn:       workFn,
			wg:           wg,
		}

		go worker.run()
	}

	for _, name := range paths {
		err := filepath.Walk(name, sv.visit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to scan dir %s: %v\n", name, err)
			if logger != nil {
				logger.Printf("failed to scan dir %s: %v\n", name, err)
			}
			close(inwork)
			return err
		}
	}

	wg.Wait()
	close(inwork)

	if byteProgress != nil {
		byteProgress.Set(int(byteProgress.Total))
		byteProgress.Finish()
	}

	fmt.Fprintf(os.Stdout, "Done.\n")
	if logger != nil {
		logger.Printf("Done.\n")
	}
	return nil
}
