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

package level

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/uwedeportivo/romba/db"
)

const (
	batchSize = 5242880
)

var rOptions *opt.ReadOptions = &opt.ReadOptions{}
var wOptions *opt.WriteOptions = &opt.WriteOptions{}

func init() {
	db.StoreOpener = openDb
}

func openDb(path string) (db.KVStore, error) {
	stor, err := storage.OpenFile(path)
	if err != nil {
		return nil, err
	}

	dbn, err := leveldb.Open(stor, &opt.Options{
		Flag:         opt.OFCreateIfMissing,
		WriteBuffer:  62914560,
		MaxOpenFiles: 500,
		BlockCache:   cache.NewLRUCache(10490000),
		Filter:       filter.NewBloomFilter(16),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open db at %s: %v\n", path, err)
	}
	return &store{
		dbn: dbn,
	}, nil
}

type store struct {
	dbn *leveldb.DB
}

func (s *store) Set(key, value []byte) error {
	return s.dbn.Put(key, value, wOptions)
}

func (s *store) Get(key []byte) ([]byte, error) {
	value, err := s.dbn.Get(key, rOptions)
	if err != nil && err != errors.ErrNotFound {
		return nil, err
	}
	return value, nil
}

func (s *store) StartBatch() db.KVBatch {
	return &batch{
		bn: leveldb.NewBatch(batchSize),
	}
}

func (s *store) WriteBatch(b db.KVBatch) error {
	cb := b.(*batch)
	return s.dbn.Write(cb.bn, wOptions)
}

func (s *store) Close() error {
	return s.dbn.Close()
}

type batch struct {
	bn *leveldb.Batch
}

func (b *batch) Set(key, value []byte) {
	b.bn.Put(key, value)
}

func (b *batch) Clear() {
	b.bn.Reset()
}
