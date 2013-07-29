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

package kyoto

import (
	"fmt"
	"github.com/uwedeportivo/cabinet"
	"github.com/uwedeportivo/romba/db"
)

func init() {
	db.StoreOpener = openDb
}

func openDb(path string) (db.KVStore, error) {
	dbn := cabinet.New()
	err := dbn.Open(path+".kch", cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	return &store{
		dbn: dbn,
	}, nil
}

type store struct {
	dbn *cabinet.KCDB
}

func (s *store) Set(key, value []byte) error {
	return s.dbn.Set(key, value)
}

func (s *store) Get(key []byte) ([]byte, error) {
	v, err := s.dbn.Get(key)
	if err != nil && s.dbn.Ecode() != cabinet.KCENOREC {
		return nil, fmt.Errorf("kyoto error on get: %v, error code: %d", err, s.dbn.Ecode())
	}
	return v, nil
}

func (s *store) StartBatch() db.KVBatch {
	bn := cabinet.New()
	err := bn.Open("-", cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		fmt.Printf("failed to open kyoto batch: %v\n", err)
		panic(err)
	}

	return &batch{
		bn: bn,
	}
}

func (s *store) WriteBatch(b db.KVBatch) error {
	cb := b.(*batch)
	return s.dbn.Merge([]*cabinet.KCDB{cb.bn}, cabinet.KCMSET)
}

func (s *store) Close() error {
	return s.dbn.Close()
}

type batch struct {
	bn *cabinet.KCDB
}

func (b *batch) Set(key, value []byte) {
	b.bn.Set(key, value)
}

func (b *batch) Clear() {
	b.bn.Clear()
}
