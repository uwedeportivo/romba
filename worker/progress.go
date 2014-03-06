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
	"sync"

	"github.com/golang/glog"
)

const ErrorLimit = 100

type ProgressTracker interface {
	SetTotalBytes(value int64)
	SetTotalFiles(value int32)
	AddBytesFromFile(value int64, erred bool)
	Finished()
	Reset()
	GetProgress() *Progress
	Stop(wc chan bool)
	Stopped() bool
}

type Progress struct {
	TotalBytes int64
	TotalFiles int32
	ErrorFiles int32
	BytesSoFar int64
	FilesSoFar int32
	stopped    bool
	m          *sync.Mutex
	wc         chan bool
}

func NewProgressTracker() ProgressTracker {
	pt := new(Progress)
	pt.m = new(sync.Mutex)
	return pt
}

func (pt *Progress) SetTotalBytes(value int64) {
	pt.TotalBytes = value
}

func (pt *Progress) SetTotalFiles(value int32) {
	pt.TotalFiles = value
}

func (pt *Progress) AddBytesFromFile(value int64, erred bool) {
	pt.m.Lock()
	defer pt.m.Unlock()

	pt.BytesSoFar += value
	pt.FilesSoFar++

	if erred {
		pt.ErrorFiles++
		if pt.ErrorFiles > ErrorLimit {
			glog.Infof("reached error limit %d, stopping ...", ErrorLimit)
			pt.stopped = true
		}
	}
}

func (pt *Progress) Stop(wc chan bool) {
	pt.m.Lock()
	defer pt.m.Unlock()

	pt.stopped = true
	pt.wc = wc
}

func (pt *Progress) Stopped() bool {
	pt.m.Lock()
	defer pt.m.Unlock()

	return pt.stopped
}

func (pt *Progress) Finished() {
	pt.m.Lock()
	defer pt.m.Unlock()

	pt.BytesSoFar = pt.TotalBytes
	pt.FilesSoFar = pt.TotalFiles
	if pt.wc != nil {
		pt.wc <- true
		pt.wc = nil
	}
}

func (pt *Progress) Reset() {
	pt.TotalBytes = 0
	pt.TotalFiles = 0
	pt.BytesSoFar = 0
	pt.FilesSoFar = 0
	pt.stopped = false
	pt.wc = nil
}

func (pt *Progress) GetProgress() *Progress {
	pt.m.Lock()
	defer pt.m.Unlock()

	p := new(Progress)
	p.TotalBytes = pt.TotalBytes
	p.TotalFiles = pt.TotalFiles
	p.ErrorFiles = pt.ErrorFiles
	p.BytesSoFar = pt.BytesSoFar
	p.FilesSoFar = pt.FilesSoFar
	return p
}
