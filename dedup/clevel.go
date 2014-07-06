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

package dedup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jmhodges/levigo"

	"github.com/uwedeportivo/romba/types"
)

var ro *levigo.ReadOptions = levigo.NewReadOptions()
var wo *levigo.WriteOptions = levigo.NewWriteOptions()

var trueVal []byte = []byte{1}
var falseVal []byte = []byte{0}

type dbDeduper struct {
	crcDB    *levigo.DB
	md5DB    *levigo.DB
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

func NewLevelDBDeduper() (Deduper, error) {
	tempPath, err := ioutil.TempDir("", "romba_dedup")
	if err != nil {
		return nil, err
	}
	dbd := new(dbDeduper)

	dbn, err := openDb(filepath.Join(tempPath, "crc_db"))
	if err != nil {
		return nil, err
	}

	dbd.crcDB = dbn

	dbn, err = openDb(filepath.Join(tempPath, "md5_db"))
	if err != nil {
		return nil, err
	}

	dbd.md5DB = dbn

	dbn, err = openDb(filepath.Join(tempPath, "sha1_db"))
	if err != nil {
		return nil, err
	}

	dbd.sha1DB = dbn

	dbd.tempPath = tempPath

	return dbd, nil
}

func (dbd *dbDeduper) Declare(r *types.Rom) error {
	if len(r.Crc) > 0 {
		err := dbd.crcDB.Put(wo, r.CrcWithSizeKey(), trueVal)
		if err != nil {
			return err
		}
	}

	if len(r.Md5) > 0 {
		err := dbd.md5DB.Put(wo, r.Md5WithSizeKey(), trueVal)
		if err != nil {
			return err
		}
	}

	if len(r.Sha1) > 0 {
		err := dbd.sha1DB.Put(wo, r.Sha1, trueVal)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbd *dbDeduper) Seen(r *types.Rom) (bool, error) {
	if len(r.Sha1) > 0 {
		val, err := dbd.sha1DB.Get(ro, r.Sha1)
		if err != nil {
			return false, err
		}

		return len(val) == 1 && val[0] == 1, nil
	}

	if len(r.Md5) > 0 {
		val, err := dbd.md5DB.Get(ro, r.Md5WithSizeKey())
		if err != nil {
			return false, err
		}

		return len(val) == 1 && val[0] == 1, nil
	}

	if len(r.Crc) > 0 {
		val, err := dbd.crcDB.Get(ro, r.CrcWithSizeKey())
		if err != nil {
			return false, err
		}

		return len(val) == 1 && val[0] == 1, nil
	}

	return false, nil
}

func (dbd *dbDeduper) Close() error {
	dbd.crcDB.Close()
	dbd.md5DB.Close()
	dbd.sha1DB.Close()

	return os.RemoveAll(dbd.tempPath)
}
