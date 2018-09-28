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

package combine

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/golang/glog"
	"github.com/klauspost/crc32"
	"github.com/uwedeportivo/romba/util"
	"os"
	"path/filepath"

	"github.com/jmhodges/levigo"

	"github.com/uwedeportivo/romba/types"
)

var ro *levigo.ReadOptions = levigo.NewReadOptions()
var wo *levigo.WriteOptions = levigo.NewWriteOptions()


type dbCombiner struct {
	sha1DB   *levigo.DB
	tempPath string
}

func openDb(path string) (*levigo.DB, error) {
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
	return dbn, nil
}

func NewLevelDBCombiner(tempPath string) (Combiner, error) {
	dbc := new(dbCombiner)

	dbn, err := openDb(filepath.Join(tempPath, "sha1_db"))
	if err != nil {
		return nil, err
	}

	dbc.sha1DB = dbn

	dbc.tempPath = tempPath

	return dbc, nil
}

func (dbc *dbCombiner) Declare(rom *types.Rom) error {
	glog.V(4).Infof("combining rom %s with size %d", hex.EncodeToString(rom.Sha1), rom.Size)

	if rom.Sha1 != nil {
		rBytes, err := dbc.sha1DB.Get(ro, rom.Sha1)
		if err != nil {
			return err
		}

		if rBytes == nil {
			rBytes = make([]byte, crc32.Size + md5.Size + 8)
			util.Int64ToBytes(rom.Size, rBytes[crc32.Size + md5.Size:])
		}

		if rom.Crc != nil {
			glog.V(4).Infof("declaring crc %s <-> sha1 %s mapping", hex.EncodeToString(rom.Crc), hex.EncodeToString(rom.Sha1))

			copy(rBytes, rom.Crc)
		}
		if rom.Md5 != nil {
			glog.V(4).Infof("declaring md5 %s <-> sha1 %s mapping", hex.EncodeToString(rom.Md5), hex.EncodeToString(rom.Sha1))

			copy(rBytes[crc32.Size:], rom.Md5)
		}

		err = dbc.sha1DB.Put(wo, rom.Sha1, rBytes)
		if err != nil {
			return err
		}
	} else {
		glog.V(4).Infof("combining rom %s with missing SHA1", rom.Name)
	}

	return nil
}

func (dbc *dbCombiner) ForEachRom(romF func(rom *types.Rom) error) error {
	it := dbc.sha1DB.NewIterator(ro)
	defer it.Close()

	it.SeekToFirst()

	for it.Valid() {
		rom := new(types.Rom)

		rom.Sha1 = it.Key()
		rom.Name = hex.EncodeToString(rom.Sha1)

		buf := it.Value()

		rom.Crc = buf[:crc32.Size]
		rom.Md5 = buf[crc32.Size: crc32.Size + md5.Size]
		rom.Size = util.BytesToInt64(buf[crc32.Size + md5.Size:])

		glog.V(4).Infof("combiner processing rom %s", rom.Name)
		err := romF(rom)

		if err != nil {
			return err
		}

		it.Next()
	}
	return nil
}

func (dbc *dbCombiner) Close() error {
	dbc.sha1DB.Close()

	return os.RemoveAll(dbc.tempPath)
}
