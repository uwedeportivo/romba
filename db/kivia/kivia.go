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

package kivia

import (
	"fmt"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/kivi"
)

func init() {
	db.StoreOpener = openDb
}

func openDb(path string, keySize int) (db.KVStore, error) {
	dbn, err := kivi.Open(path, keySize)

	if err != nil {
		return nil, fmt.Errorf("failed to open db at %s: %v\n", path, err)
	}
	return &store{
		dbn: dbn,
	}, nil
}

type store struct {
	dbn *kivi.DB
}

func (s *store) Flush() {
	s.dbn.Flush()
}

func (s *store) Size() int64 {
	return s.dbn.Size()
}

func (s *store) Set(key, value []byte) error {
	return s.dbn.Put(key, value)
}

func (s *store) Append(key, value []byte) error {
	return s.dbn.Append(key, value)
}

func (s *store) Delete(key []byte) error {
	return s.dbn.Delete(key)
}

func (s *store) Get(key []byte) ([]byte, error) {
	v, err := s.dbn.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s *store) Exists(key []byte) (bool, error) {
	return s.dbn.Exists(key)
}

func (s *store) StartBatch() db.KVBatch {
	return &batch{
		bn: s.dbn,
	}
}

func (s *store) WriteBatch(b db.KVBatch) error {
	return nil
}

func (s *store) Close() error {
	return s.dbn.Close()
}

func (s *store) BeginRefresh() error {
	return s.dbn.BeginRefresh()
}

func (s *store) EndRefresh() error {
	return s.dbn.EndRefresh()
}

type batch struct {
	bn *kivi.DB
}

func (b *batch) Set(key, value []byte) error {
	return b.bn.Put(key, value)
}

func (b *batch) Append(key, value []byte) error {
	return b.bn.Append(key, value)
}

func (b *batch) Delete(key []byte) error {
	return b.bn.Delete(key)
}

func (b *batch) Clear() {
}
