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
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	dataFilenamePrefix = "data_"
)

type kvPair struct {
	key   []byte
	value []byte
}

type DB struct {
	kd      *keydir
	wchan   chan *kvPair
	closing chan bool
	root    string
	active  *os.File
}

func Open(root string) (*DB, error) {
	err := os.MkdirAll(root, 0766)
	if err != nil {
		return nil, err
	}
	kvdb := new(DB)
	kvdb.kd = newKeydir()
	kvdb.root = root
	kvdb.wchan = make(chan *kvPair)
	kvdb.closing = make(chan bool)

	f, err := os.OpenFile(filepath.Join(root, dataFilenamePrefix+"0"), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	kvdb.active = f

	go kvdb.runWrites()

	return kvdb, nil
}

func (kvdb *DB) Close() error {
	close(kvdb.wchan)
	<-kvdb.closing
	return kvdb.active.Close()
}

func (kvdb *DB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (kvdb *DB) Put(key, value []byte) error {
	kcp := make([]byte, len(key))
	vcp := make([]byte, len(value))

	copy(kcp, key)
	copy(vcp, value)

	kvdb.wchan <- &kvPair{
		key:   kcp,
		value: vcp,
	}
	return nil
}

type countWriter struct {
	w     io.Writer
	count int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.count += int64(n)
	return n, err
}

func (kvdb *DB) runWrites() {
	fi, _ := kvdb.active.Stat()

	var buf bytes.Buffer
	bw := bufio.NewWriter(kvdb.active)
	cw := &countWriter{
		w:     bw,
		count: fi.Size(),
	}

	for kvp := range kvdb.wchan {
		buf.Reset()

		ts := time.Now()
		pos := cw.count

		binary.Write(&buf, binary.BigEndian, ts.UnixNano())
		binary.Write(&buf, binary.BigEndian, int64(len(kvp.key)))
		binary.Write(&buf, binary.BigEndian, int64(len(kvp.value)))
		buf.Write(kvp.key)
		buf.Write(kvp.value)

		crc := crc32.ChecksumIEEE(buf.Bytes())

		binary.Write(cw, binary.BigEndian, crc)
		cw.Write(buf.Bytes())

		kde := &keydirEntry{
			fileId: 0,
			tstamp: ts.UnixNano(),
			vpos:   pos,
			vsize:  int64(len(kvp.value)),
		}

		kvdb.kd.put(kvp.key, kde)
	}

	bw.Flush()
	kvdb.closing <- true
}
