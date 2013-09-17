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

package clevel

import (
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/uwedeportivo/romba/db"
)

var rOptions *levigo.ReadOptions = levigo.NewReadOptions()
var wOptions *levigo.WriteOptions = levigo.NewWriteOptions()

func init() {
	db.StoreOpener = openDb
}

func openDb(path string) (db.KVStore, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetFilterPolicy(levigo.NewBloomFilter(16))
	opts.SetCache(levigo.NewLRUCache(10490000))
	opts.SetMaxOpenFiles(500)
	opts.SetWriteBufferSize(62914560)
	opts.SetEnv(levigo.NewDefaultEnv())
	dbn, err := levigo.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open db at %s: %v\n", path, err)
	}
	return &store{
		dbn: dbn,
	}, nil
}

type store struct {
	dbn *levigo.DB
}

func (s *store) Set(key, value []byte) error {
	return s.dbn.Put(wOptions, key, value)
}

func (s *store) Get(key []byte) ([]byte, error) {
	return s.dbn.Get(rOptions, key)
}

func (s *store) StartBatch() db.KVBatch {
	return &batch{
		bn: levigo.NewWriteBatch(),
	}
}

func (s *store) WriteBatch(b db.KVBatch) error {
	cb := b.(*batch)
	return s.dbn.Write(wOptions, cb.bn)
}

func (s *store) Close() error {
	s.dbn.Close()
	return nil
}

type batch struct {
	bn *levigo.WriteBatch
}

func (b *batch) Set(key, value []byte) {
	b.bn.Put(key, value)
}

func (b *batch) Clear() {
	b.bn.Clear()
}