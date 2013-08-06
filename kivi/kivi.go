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
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/glog"
)

const (
	dataFilenamePrefix = "data_"
)

type kvPair struct {
	key   []byte
	value []byte
}

type DB struct {
	kd         *keydir
	wchan      chan *kvPair
	closing    chan bool
	root       string
	active     *os.File
	activeMark int64
}

func Open(root string) (*DB, error) {
	glog.Infof("Opening database %s\n", root)
	startTime := time.Now()

	err := os.MkdirAll(root, 0766)
	if err != nil {
		return nil, err
	}
	kvdb := new(DB)
	kd, err := openKeydir(root, 0)
	if err != nil {
		glog.Infof("error opening keydir file: %v\n", err)
		kd = newKeydir()
	}
	kvdb.kd = kd
	kvdb.root = root
	kvdb.wchan = make(chan *kvPair)
	kvdb.closing = make(chan bool)

	f, err := os.OpenFile(filepath.Join(root, dataFilenamePrefix+"0"), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	kvdb.active = f

	go runWrites(kvdb)

	elapsed := time.Since(startTime)
	glog.Infof("finished opening %s (elapsed time %s) \n", root, formatDuration(elapsed))

	return kvdb, nil
}

func formatDuration(d time.Duration) string {
	secs := uint64(d.Seconds())
	mins := secs / 60
	secs = secs % 60
	hours := mins / 60
	mins = mins % 60

	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, mins, secs)
	}

	if mins > 0 {
		return fmt.Sprintf("%dm%ds", mins, secs)
	}
	return fmt.Sprintf("%ds", secs)
}

func (kvdb *DB) Close() error {
	glog.Infof("Closing database %s\n", kvdb.root)
	startTime := time.Now()

	close(kvdb.wchan)
	<-kvdb.closing
	err := kvdb.active.Close()
	if err != nil {
		return err
	}

	err = saveKeydir(kvdb.root, kvdb.kd, 0)
	if err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	glog.Infof("finished closing %s (elapsed time %s)\n", kvdb.root, formatDuration(elapsed))

	kvdb.kd = nil
	return nil
}

func (kvdb *DB) Get(key []byte) ([]byte, error) {
	kde := kvdb.kd.get(key)
	if kde == nil {
		return nil, nil
	}
	buflen := kde.vsize + keySize + 8 + 8 + 4

	if kde.vpos+buflen > kvdb.activeMark {
		// TODO(uwe): not flushed ?
		return nil, nil
	}

	buf := make([]byte, buflen)

	_, err := kvdb.active.ReadAt(buf, kde.vpos)
	if err != nil {
		return nil, err
	}

	br := bytes.NewBuffer(buf)

	var crc int32
	var tstamp, vlen int64

	binary.Read(br, binary.BigEndian, &crc)
	binary.Read(br, binary.BigEndian, &tstamp)
	binary.Read(br, binary.BigEndian, &vlen)

	keybuf := make([]byte, keySize)

	_, err = io.ReadFull(br, keybuf)
	if err != nil {
		return nil, err
	}

	vbuf := make([]byte, int(vlen))

	_, err = io.ReadFull(br, vbuf)
	if err != nil {
		return nil, err
	}
	return vbuf, nil
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

func runWrites(kvdb *DB) {
	var buf bytes.Buffer

	fi, _ := kvdb.active.Stat()
	kvdb.activeMark = fi.Size()

	bw := bufio.NewWriter(kvdb.active)
	cw := &countWriter{
		w:     bw,
		count: fi.Size(),
	}

	keybuf := make([]byte, keySize)

	for kvp := range kvdb.wchan {
		buf.Reset()

		ts := time.Now()
		pos := cw.count

		n := len(kvp.key)
		if n > keySize {
			n = keySize
		}
		copy(keybuf, kvp.key[0:n])

		binary.Write(&buf, binary.BigEndian, ts.UnixNano())
		binary.Write(&buf, binary.BigEndian, int64(len(kvp.value)))
		buf.Write(keybuf)
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
