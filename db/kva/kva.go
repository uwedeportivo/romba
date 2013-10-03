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

package kva

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cznic/kv"
	"github.com/uwedeportivo/romba/db"
)

const (
	dbFilename = "data"
)

func init() {
	db.StoreOpener = openDb
}

func openDb(path string, keySize int) (db.KVStore, error) {
	dbPath := filepath.Join(path, dbFilename)

	createOpen := kv.Open
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		err = os.MkdirAll(path, 0755)
		if err != nil {
			return nil, err
		}
		createOpen = kv.Create
	}

	opts := &kv.Options{}

	dbn, err := createOpen(dbPath, opts)

	if err != nil {
		return nil, fmt.Errorf("failed to open db at %s: %v\n", dbPath, err)
	}
	return &store{
		dbn: dbn,
	}, nil
}

type store struct {
	dbn *kv.DB
}

func (s *store) Append(key, value []byte) error {
	_, _, err := s.dbn.Put(nil, key,
		func(key, old []byte) ([]byte, bool, error) {
			if old == nil {
				return value, true, nil
			}

			found := false
			vsize := len(value)

			for i := 0; i < len(old); i += vsize {
				if bytes.Equal(value, old[i:i+vsize]) {
					found = true
					break
				}
			}

			if found {
				return nil, false, nil
			}

			return append(old, value...), true, nil
		})

	return err
}

func (s *store) Set(key, value []byte) error {
	return s.dbn.Set(key, value)
}

func (s *store) Delete(key []byte) error {
	return s.dbn.Delete(key)
}

func (s *store) Get(key []byte) ([]byte, error) {
	return s.dbn.Get(nil, key)
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
func (s *store) PrintStats() string  { return "" }

func (s *store) Flush() {}

func (s *store) Size() int64 {
	return 0
}

func (s *store) StartBatch() db.KVBatch {
	return &batch{
		s: s,
	}
}

func (s *store) WriteBatch(b db.KVBatch) error {
	return nil
}

func (s *store) Close() error {
	return s.dbn.Close()
}

type batch struct {
	s *store
}

func (b *batch) Set(key, value []byte) error {
	return b.s.Set(key, value)
}

func (b *batch) Append(key, value []byte) error {
	return b.s.Append(key, value)
}

func (b *batch) Delete(key []byte) error {
	return b.s.Delete(key)
}

func (b *batch) Clear() {
}
