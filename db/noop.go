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
	"github.com/uwedeportivo/romba/types"
)

type NoOpDB struct{}
type NoOpBatch struct{}

func (noop *NoOpDB) IndexRom(rom *types.Rom) error {
	return nil
}

func (noop *NoOpDB) IndexDat(dat *types.Dat, sha1 []byte) error {
	return nil
}

func (noop *NoOpDB) OrphanDats() error {
	return nil
}

func (noop *NoOpDB) Close() error {
	return nil
}

func (noop *NoOpDB) GetDat(sha1 []byte) (*types.Dat, error) {
	return nil, nil
}

func (noop *NoOpDB) DatsForRom(rom *types.Rom) ([]*types.Dat, error) {
	return nil, nil
}

func (noop *NoOpDB) FilteredDatsForRom(rom *types.Rom) ([]*types.Dat, []*types.Dat, error) {
	return nil, nil, nil
}

func (noop *NoOpDB) StartBatch() RomBatch {
	return new(NoOpBatch)
}

func (noop *NoOpBatch) Flush() error {
	return nil
}

func (noop *NoOpBatch) Close() error {
	return nil
}

func (noop *NoOpBatch) IndexRom(rom *types.Rom) error {
	return nil
}

func (noop *NoOpBatch) IndexDat(dat *types.Dat, sha1 []byte) error {
	return nil
}

func (noop *NoOpBatch) Size() int64 {
	return 0
}

func (noop *NoOpDB) DebugGet(key []byte, size int64) string {
	return ""
}

func (noop *NoOpDB) ResolveHash(key []byte) ([]byte, error) {
	return nil, nil
}
