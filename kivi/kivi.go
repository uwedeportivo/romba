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
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/golang/groupcache/lru"
)

const (
	dataFilenamePrefix = "data_"
)

type OpType byte

const (
	PutOp OpType = iota
	AppendOp
	DeleteOp
	FlushOp
	RotateOp
)

type kvPair struct {
	key    []byte
	value  []byte
	op     OpType
	finish chan bool
}

type DB struct {
	kd            *keydir
	wchan         chan *kvPair
	closing       chan bool
	root          string
	active        *os.File
	activeFileId  int32
	openDataFiles *lru.Cache
	openCacheLock *sync.Mutex
}

func dataFilename(root string, fileId int32) string {
	filename := fmt.Sprintf("%s%d", dataFilenamePrefix, fileId)
	return filepath.Join(root, filename)
}

func populateKeydir(root string, kd *keydir, fileId int32) error {
	fd, err := os.Open(dataFilename(root, fileId))
	if err != nil {
		return err
	}
	defer fd.Close()

	fi, err := fd.Stat()
	if err != nil {
		return err
	}

	size := fi.Size()
	var count int64

	buf := make([]byte, kd.keySize+1+4+4)

	for count < size {
		_, err := io.ReadFull(fd, buf)
		if err != nil {
			return err
		}

		br := bytes.NewBuffer(buf)

		var vlen int32
		var crc uint32
		var op byte

		binary.Read(br, binary.BigEndian, &crc)
		binary.Read(br, binary.BigEndian, &op)
		binary.Read(br, binary.BigEndian, &vlen)

		key := buf[1+4+4:]

		kde := &keydirEntry{
			fileId: fileId,
			vpos:   int32(count),
			vsize:  vlen,
		}

		switch OpType(op) {
		case PutOp:
			kd.put(key, kde)
		case AppendOp:
			kd.append(key, kde)
		case DeleteOp:
			kd.delete(key)
		}

		_, err = io.CopyN(ioutil.Discard, fd, int64(vlen))
		if err != nil {
			return err
		}
		count += int64(kd.keySize) + 1 + 4 + 4 + int64(vlen)
	}
	return nil
}

func readDataFiles(root string, kd *keydir, maxFileId int32) (int32, error) {
	glog.Info("reading data files")
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return 0, err
	}

	var fileIds []int

	for _, file := range files {
		if strings.HasPrefix(file.Name(), dataFilenamePrefix) {
			var fileId int
			_, err = fmt.Sscanf(file.Name(), dataFilenamePrefix+"%d", &fileId)
			if err != nil {
				return 0, err
			}
			fileIds = append(fileIds, fileId)
		}
	}

	l := len(fileIds)

	if l == 0 {
		return -1, nil
	}

	sort.Ints(fileIds)
	index := sort.SearchInts(fileIds, int(maxFileId+1))

	for i := index; i < l; i++ {
		glog.Infof("populating keydir from data file %d\n", fileIds[i])
		err = populateKeydir(root, kd, int32(fileIds[i]))
		if err != nil {
			return 0, err
		}
	}

	return int32(fileIds[l-1]), nil
}

func deleteOldDataFiles(root string, kd *keydir, activeFileId int32) error {
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), dataFilenamePrefix) {
			var fileId int
			_, err = fmt.Sscanf(file.Name(), dataFilenamePrefix+"%d", &fileId)
			if err != nil {
				return err
			}
			if int32(fileId) < activeFileId {
				err = os.Remove(filepath.Join(root, file.Name()))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func closeEvictedDataFile(key lru.Key, value interface{}) {
	readCloser := value.(io.ReadCloser)
	fileId := key.(int32)
	err := readCloser.Close()
	if err != nil {
		glog.Errorf("error closing data file %d: %v", fileId, err)
	}
}

func Open(root string, keySize int) (*DB, error) {
	glog.Infof("Opening database %s\n", root)
	startTime := time.Now()

	err := os.MkdirAll(root, 0766)
	if err != nil {
		return nil, err
	}
	kvdb := new(DB)
	kd, fileId, err := openKeydir(root)
	if err != nil {
		return nil, err
	}

	if kd == nil {
		glog.Infof("no keydir file")
		kd = newKeydir(keySize)
	}

	lastFileId, err := readDataFiles(root, kd, fileId)
	if err != nil {
		return nil, err
	}

	kvdb.activeFileId = lastFileId + 1

	kvdb.kd = kd
	kvdb.root = root
	kvdb.wchan = make(chan *kvPair)
	kvdb.closing = make(chan bool)
	kvdb.openDataFiles = lru.New(100)
	kvdb.openDataFiles.OnEvicted = closeEvictedDataFile
	kvdb.openCacheLock = new(sync.Mutex)

	f, err := os.OpenFile(dataFilename(kvdb.root, kvdb.activeFileId), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
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

	err = saveKeydir(kvdb.root, kvdb.kd, kvdb.activeFileId)
	if err != nil {
		return err
	}

	for kvdb.openDataFiles.Len() > 0 {
		kvdb.openDataFiles.RemoveOldest()
	}

	elapsed := time.Since(startTime)
	glog.Infof("finished closing %s (elapsed time %s)\n", kvdb.root, formatDuration(elapsed))

	kvdb.kd = nil
	return nil
}

func (kvdb *DB) dataReader(fileId int32) (io.ReaderAt, error) {
	if fileId == kvdb.activeFileId {
		return kvdb.active, nil
	}

	kvdb.openCacheLock.Lock()
	defer kvdb.openCacheLock.Unlock()

	var reader io.ReaderAt
	if value, ok := kvdb.openDataFiles.Get(fileId); ok {
		reader = value.(io.ReaderAt)
	}

	if reader == nil {
		rd, err := os.Open(dataFilename(kvdb.root, fileId))
		if err != nil {
			return nil, err
		}
		reader = rd
		kvdb.openDataFiles.Add(fileId, rd)
	}

	return reader, nil
}

func (kvdb *DB) getAt(kde *keydirEntry, keybuf, key []byte) ([]byte, error) {
	dataReader, err := kvdb.dataReader(kde.fileId)
	if err != nil {
		return nil, err
	}

	buflen := int(kde.vsize) + kvdb.kd.keySize + 4 + 4 + 1

	buf := make([]byte, buflen)

	_, err = dataReader.ReadAt(buf, int64(kde.vpos))
	if err != nil {
		return nil, err
	}

	br := bytes.NewBuffer(buf)

	var vlen int32
	var crc uint32
	var op byte

	binary.Read(br, binary.BigEndian, &crc)
	binary.Read(br, binary.BigEndian, &op)
	binary.Read(br, binary.BigEndian, &vlen)

	_, err = io.ReadFull(br, keybuf)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(keybuf, key) {
		return nil, fmt.Errorf("keydir corruption: key differs from requested key")
	}

	vbuf := make([]byte, int(vlen))

	_, err = io.ReadFull(br, vbuf)
	if err != nil {
		return nil, err
	}

	calcCrc := crc32.ChecksumIEEE(buf[4:])

	if calcCrc != crc {
		return nil, fmt.Errorf("calculated crc %d differs from saved crc %d", calcCrc, crc)
	}

	return vbuf, nil
}

func (kvdb *DB) Get(key []byte) ([]byte, error) {
	kdes := kvdb.kd.get(key)
	if kdes == nil {
		return nil, nil
	}

	keybuf := make([]byte, kvdb.kd.keySize)

	var rBuf []byte

	for _, kde := range kdes {
		v, err := kvdb.getAt(kde, keybuf, key)
		if err != nil {
			return nil, err
		}

		if v != nil {
			rBuf = append(rBuf, v...)
		}
	}
	return rBuf, nil
}

func (kvdb *DB) Exists(key []byte) (bool, error) {
	kdes := kvdb.kd.get(key)
	ex := kdes != nil

	return ex, nil
}

func (kvdb *DB) Size() int64 {
	return kvdb.kd.size()
}

func (kvdb *DB) modify(key, value []byte, op OpType) error {
	kcp := make([]byte, len(key))

	var vcp []byte

	if value != nil {
		vcp = make([]byte, len(value))
	}

	copy(kcp, key)

	if value != nil {
		copy(vcp, value)
	}

	kvdb.wchan <- &kvPair{
		key:   kcp,
		value: vcp,
		op:    op,
	}
	return nil
}

func (kvdb *DB) Flush() {
	finish := make(chan bool)
	kvdb.wchan <- &kvPair{
		op:     FlushOp,
		finish: finish,
	}

	<-finish
}

func (kvdb *DB) BeginRefresh() error {
	finish := make(chan bool)
	kvdb.wchan <- &kvPair{
		op:     RotateOp,
		finish: finish,
	}

	<-finish
	return nil
}

func (kvdb *DB) EndRefresh() error {
	kvdb.kd.forgetPast(kvdb.activeFileId)

	return deleteOldDataFiles(kvdb.root, kvdb.kd, kvdb.activeFileId)
}

func (kvdb *DB) Put(key, value []byte) error {
	return kvdb.modify(key, value, PutOp)
}

func (kvdb *DB) Append(key, value []byte) error {
	return kvdb.modify(key, value, AppendOp)
}

func (kvdb *DB) Delete(key []byte) error {
	return kvdb.modify(key, nil, DeleteOp)
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

	bw := bufio.NewWriter(kvdb.active)
	cw := &countWriter{
		w:     bw,
		count: 0,
	}

	keybuf := make([]byte, kvdb.kd.keySize)

	for kvp := range kvdb.wchan {
		buf.Reset()

		if kvp.key != nil {
			pos := cw.count

			copy(keybuf, kvp.key)

			buf.WriteByte(byte(kvp.op))
			binary.Write(&buf, binary.BigEndian, int32(len(kvp.value)))
			buf.Write(keybuf)
			buf.Write(kvp.value)

			crc := crc32.ChecksumIEEE(buf.Bytes())

			err := binary.Write(cw, binary.BigEndian, crc)
			if err != nil {
				glog.Errorf("failed to write crc: %v", err)
				continue
			}

			_, err = cw.Write(buf.Bytes())
			if err != nil {
				glog.Errorf("failed to write: %v", err)
				continue
			}

			kde := &keydirEntry{
				fileId: kvdb.activeFileId,
				vpos:   int32(pos),
				vsize:  int32(len(kvp.value)),
			}

			switch kvp.op {
			case PutOp:
				kvdb.kd.put(kvp.key, kde)
			case AppendOp:
				kvdb.kd.append(kvp.key, kde)
			case DeleteOp:
				kvdb.kd.delete(kvp.key)
			}
		}

		if kvp.op == FlushOp {
			err := bw.Flush()
			if err != nil {
				glog.Errorf("failed to flush: %v", err)
			}
		} else if kvp.op == RotateOp {
			err := kvdb.active.Close()
			if err != nil {
				glog.Errorf("failed to rotate close active: %v", err)
				panic(err)
			}

			kvdb.activeFileId += 1

			f, err := os.OpenFile(dataFilename(kvdb.root, kvdb.activeFileId), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				glog.Errorf("failed to rotate open active: %v", err)
				panic(err)
			}
			kvdb.active = f

			bw = bufio.NewWriter(kvdb.active)
			cw = &countWriter{
				w:     bw,
				count: 0,
			}
		}

		if kvp.finish != nil {
			kvp.finish <- true
		}
	}

	err := bw.Flush()
	if err != nil {
		glog.Errorf("failed to flush: %v", err)
	}
	kvdb.closing <- true
}
