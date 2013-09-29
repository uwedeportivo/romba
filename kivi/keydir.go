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
	numParts    = 13 //51
	keySizeCrc  = 4
	keySizeMd5  = 16
	keySizeSha1 = 20
)

type keydirEntry struct {
	fileId int32
	vpos   int32
	vsize  int32
}

type keydir struct {
	keySize  int
	orphaned int64
	parts    [numParts]*mPart
}

type mPart struct {
	mtx   sync.RWMutex
	mCrc  map[[keySizeCrc]byte][]*keydirEntry
	mMd5  map[[keySizeMd5]byte][]*keydirEntry
	mSha1 map[[keySizeSha1]byte][]*keydirEntry
}

func calcBucket(bs []byte) int {
	if len(bs) < 2 {
		return 0
	}
	var v int = 256*int(bs[1]) + int(bs[0])
	return v % numParts
}

func newKeydir(keySize int) *keydir {
	cm := new(keydir)
	cm.keySize = keySize

	for k := 0; k < numParts; k++ {
		p := new(mPart)
		switch keySize {
		case keySizeCrc:
			p.mCrc = make(map[[keySizeCrc]byte][]*keydirEntry)
		case keySizeMd5:
			p.mMd5 = make(map[[keySizeMd5]byte][]*keydirEntry)
		case keySizeSha1:
			p.mSha1 = make(map[[keySizeSha1]byte][]*keydirEntry)
		default:
			panic("unknown keysize")
		}
		cm.parts[k] = p
	}
	return cm
}

func (cm *keydir) size() int64 {
	var s int64

	for k := 0; k < numParts; k++ {
		p := cm.parts[k]
		switch cm.keySize {
		case keySizeCrc:
			s += int64(len(p.mCrc))
		case keySizeMd5:
			s += int64(len(p.mMd5))
		case keySizeSha1:
			s += int64(len(p.mSha1))
		default:
			panic("unknown keysize")
		}
	}
	return s
}

func (cm *keydir) get(bs []byte) []*keydirEntry {
	k := calcBucket(bs)
	p := cm.parts[k]

	var v []*keydirEntry

	p.mtx.RLock()
	switch cm.keySize {
	case keySizeCrc:
		var key [keySizeCrc]byte
		copy(key[:], bs[:])
		v = p.mCrc[key]
	case keySizeMd5:
		var key [keySizeMd5]byte
		copy(key[:], bs[:])
		v = p.mMd5[key]
	case keySizeSha1:
		var key [keySizeSha1]byte
		copy(key[:], bs[:])
		v = p.mSha1[key]
	default:
		panic("unknown keysize")
	}
	p.mtx.RUnlock()
	return v
}

func (cm *keydir) put(bs []byte, vs *keydirEntry) {
	k := calcBucket(bs)
	p := cm.parts[k]

	var found bool

	p.mtx.Lock()

	switch cm.keySize {
	case keySizeCrc:
		var key [keySizeCrc]byte
		copy(key[:], bs[:])
		_, found = p.mCrc[key]
		p.mCrc[key] = []*keydirEntry{vs}
	case keySizeMd5:
		var key [keySizeMd5]byte
		copy(key[:], bs[:])
		_, found = p.mMd5[key]
		p.mMd5[key] = []*keydirEntry{vs}
	case keySizeSha1:
		var key [keySizeSha1]byte
		copy(key[:], bs[:])
		_, found = p.mSha1[key]
		p.mSha1[key] = []*keydirEntry{vs}
	default:
		panic("unknown keysize")
	}

	if found {
		cm.orphaned++
	}

	p.mtx.Unlock()
}

func (cm *keydir) append(bs []byte, vs *keydirEntry) {
	k := calcBucket(bs)
	p := cm.parts[k]

	p.mtx.Lock()
	switch cm.keySize {
	case keySizeCrc:
		var key [keySizeCrc]byte
		copy(key[:], bs[:])
		p.mCrc[key] = append(p.mCrc[key], vs)
	case keySizeMd5:
		var key [keySizeMd5]byte
		copy(key[:], bs[:])
		p.mMd5[key] = append(p.mMd5[key], vs)
	case keySizeSha1:
		var key [keySizeSha1]byte
		copy(key[:], bs[:])
		p.mSha1[key] = append(p.mSha1[key], vs)
	default:
		panic("unknown keysize")
	}

	p.mtx.Unlock()
}

func (cm *keydir) delete(bs []byte) {
	k := calcBucket(bs)
	p := cm.parts[k]

	p.mtx.Lock()
	switch cm.keySize {
	case keySizeCrc:
		var key [keySizeCrc]byte
		copy(key[:], bs[:])
		delete(p.mCrc, key)
	case keySizeMd5:
		var key [keySizeMd5]byte
		copy(key[:], bs[:])
		delete(p.mMd5, key)
	case keySizeSha1:
		var key [keySizeSha1]byte
		copy(key[:], bs[:])
		delete(p.mSha1, key)
	default:
		panic("unknown keysize")
	}

	cm.orphaned++

	p.mtx.Unlock()
}

func filterKeydirEntries(kdes []*keydirEntry, activeFileId int32) []*keydirEntry {
	var rkdes []*keydirEntry

	for _, kde := range kdes {
		if kde.fileId == activeFileId {
			rkdes = append(rkdes, kde)
		}
	}
	return rkdes
}

func (cm *keydir) forgetPast(activeFileId int32) {
	for k := 0; k < numParts; k++ {
		p := cm.parts[k]

		p.mtx.Lock()

		switch cm.keySize {
		case keySizeCrc:
			for key, kdes := range p.mCrc {
				rkdes := filterKeydirEntries(kdes, activeFileId)
				if rkdes != nil {
					p.mCrc[key] = rkdes
				}
			}
		case keySizeMd5:
			for key, kdes := range p.mMd5 {
				rkdes := filterKeydirEntries(kdes, activeFileId)
				if rkdes != nil {
					p.mMd5[key] = rkdes
				}
			}
		case keySizeSha1:
			for key, kdes := range p.mSha1 {
				rkdes := filterKeydirEntries(kdes, activeFileId)
				if rkdes != nil {
					p.mSha1[key] = rkdes
				}
			}
		default:
			panic("unknown keysize")
		}
		p.mtx.Unlock()
	}
}
