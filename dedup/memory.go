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

package dedup

import (
	"sync"

	"github.com/uwedeportivo/romba/types"
)

type memoryDeduper struct {
	crcs  map[string]bool
	md5s  map[string]bool
	sha1s map[string]bool

	mutex sync.Mutex
}

func NewMemoryDeduper() Deduper {
	return &memoryDeduper{
		crcs:  make(map[string]bool),
		md5s:  make(map[string]bool),
		sha1s: make(map[string]bool),
	}
}

func (md *memoryDeduper) Declare(r *types.Rom) error {
	md.mutex.Lock()
	defer md.mutex.Unlock()

	if len(r.Crc) > 0 {
		md.crcs[string(r.CrcWithSizeKey())] = true
	}

	if len(r.Md5) > 0 {
		md.md5s[string(r.Md5WithSizeKey())] = true
	}

	if len(r.Sha1) > 0 {
		md.sha1s[string(r.Sha1)] = true
	}
	return nil
}

func (md *memoryDeduper) Seen(r *types.Rom) (bool, error) {
	md.mutex.Lock()
	defer md.mutex.Unlock()

	if len(r.Sha1) > 0 && md.sha1s[string(r.Sha1)] {
		return true, nil
	}

	if len(r.Md5) > 0 && md.md5s[string(r.Md5WithSizeKey())] {
		return true, nil
	}

	if len(r.Crc) > 0 && md.crcs[string(r.CrcWithSizeKey())] {
		return true, nil
	}

	return false, nil
}

func (md *memoryDeduper) Close() error {
	return nil
}
