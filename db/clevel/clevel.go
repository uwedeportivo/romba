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
	"bytes"
	"fmt"

	"github.com/jmhodges/levigo"
	"github.com/uwedeportivo/romba/db"
)

var rOptions *levigo.ReadOptions = levigo.NewReadOptions()
var wOptions *levigo.WriteOptions = levigo.NewWriteOptions()

func init() {
	db.StoreOpener = openDb
}

func openDb(path string, keySize int) (db.KVStore, error) {
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
		dbn:     dbn,
		keySize: keySize,
	}, nil
}

type store struct {
	dbn     *levigo.DB
	keySize int
}

func (s *store) Set(key, value []byte) error {
	return s.dbn.Put(wOptions, key, value)
}

func (s *store) Get(key []byte) ([]byte, error) {
	return s.dbn.Get(rOptions, key)
}

func (s *store) GetKeySuffixesFor(keyPrefix []byte) ([]byte, error) {
	var suffixes []byte

	it := s.dbn.NewIterator(rOptions)
	n := len(keyPrefix)

	key := make([]byte, s.keySize)
	copy(key[:n], keyPrefix)

	it.Seek(key)

	for it.Valid() {
		ik := it.Key()
		if bytes.Equal(ik[:n], keyPrefix) {
			suffixes = append(suffixes, ik[n:]...)
		} else {
			break
		}
		it.Next()
	}
	return suffixes, nil
}

func (s *store) Delete(key []byte) error {
	return s.dbn.Delete(wOptions, key)
}

func (s *store) Exists(key []byte) (bool, error) {
	v, err := s.Get(key)
	if err != nil {
		return false, err
	}

	return v != nil, nil
}

func (s *store) BeginRefresh() error { return nil }
func (s *store) EndRefresh() error   { return nil }
func (s *store) PrintStats() string {
	return s.dbn.PropertyValue("leveldb.stats")
}

func (s *store) Flush() {}

func (s *store) Size() int64 {
	return 0
}

func (s *store) StartBatch() db.KVBatch {
	return &batch{
		bn: levigo.NewWriteBatch(),
		s:  s,
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
	s  *store
}

func (b *batch) Set(key, value []byte) error {
	b.bn.Put(key, value)
	return nil
}

func (b *batch) Delete(key []byte) error {
	b.bn.Delete(key)
	return nil
}

func (b *batch) Clear() {
	b.bn.Clear()
}
