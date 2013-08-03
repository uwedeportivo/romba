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

package kivi

import (
	"sync"
)

const (
	numParts = 51
	keySize  = 20
)

type keydirEntry struct {
	fileId int16
	tstamp int64
	vpos   int64
	vsize  int64
}

type keydir struct {
	parts [numParts]*mPart
}

type mPart struct {
	mtx sync.RWMutex
	m   map[[keySize]byte]*keydirEntry
}

func calcBucket(bs []byte) int {
	if len(bs) < 2 {
		return 0
	}
	var v int = 256*int(bs[1]) + int(bs[0])
	return v % numParts
}

func newKeydir() *keydir {
	cm := new(keydir)

	for k := 0; k < numParts; k++ {
		p := new(mPart)
		p.m = make(map[[keySize]byte]*keydirEntry)
		cm.parts[k] = p
	}
	return cm
}

func (cm *keydir) get(bs []byte) *keydirEntry {
	k := calcBucket(bs)
	p := cm.parts[k]

	n := len(bs)
	if n > keySize {
		n = keySize
	}

	var key [keySize]byte
	copy(key[:], bs[0:n])

	p.mtx.RLock()
	v := p.m[key]
	p.mtx.RUnlock()
	return v
}

func (cm *keydir) put(bs []byte, vs *keydirEntry) {
	k := calcBucket(bs)
	p := cm.parts[k]

	n := len(bs)
	if n > keySize {
		n = keySize
	}

	var key [keySize]byte
	copy(key[:], bs[0:n])

	p.mtx.Lock()
	p.m[key] = vs
	p.mtx.Unlock()
}
