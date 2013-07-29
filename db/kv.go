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

package db

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"github.com/uwedeportivo/romba/types"
	"io"
	"path/filepath"
)

const (
	datsDBName    = "dats_db"
	crcDBName     = "crc_db"
	md5DBName     = "md5_db"
	sha1DBName    = "sha1_db"
	crcsha1DBName = "crcsha1_db"
	md5sha1DBName = "md5sha1_db"
)

type KVStore interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	StartBatch() KVBatch
	WriteBatch(batch KVBatch) error
	Close() error
}

type KVBatch interface {
	Set(key, value []byte)
	Clear()
}

var StoreOpener func(pathPrefix string) (KVStore, error)

type kvStore struct {
	generation int64
	datsDB     KVStore
	crcDB      KVStore
	md5DB      KVStore
	sha1DB     KVStore
	crcsha1DB  KVStore
	md5sha1DB  KVStore
	path       string
}

type kvBatch struct {
	db           *kvStore
	datsBatch    KVBatch
	crcBatch     KVBatch
	md5Batch     KVBatch
	sha1Batch    KVBatch
	crcsha1Batch KVBatch
	md5sha1Batch KVBatch
	size         int64
}

func openDb(pathPrefix string) (KVStore, error) {
	return StoreOpener(pathPrefix)
}

func NewKVStoreDB(path string) (RomDB, error) {
	kvdb := new(kvStore)
	kvdb.path = path

	gen, err := ReadGenerationFile(path)
	if err != nil {
		return nil, err
	}
	kvdb.generation = gen

	db, err := openDb(filepath.Join(path, datsDBName))
	if err != nil {
		return nil, err
	}
	kvdb.datsDB = db

	db, err = openDb(filepath.Join(path, crcDBName))
	if err != nil {
		return nil, err
	}
	kvdb.crcDB = db

	db, err = openDb(filepath.Join(path, md5DBName))
	if err != nil {
		return nil, err
	}
	kvdb.md5DB = db

	db, err = openDb(filepath.Join(path, sha1DBName))
	if err != nil {
		return nil, err
	}
	kvdb.sha1DB = db

	db, err = openDb(filepath.Join(path, crcsha1DBName))
	if err != nil {
		return nil, err
	}
	kvdb.crcsha1DB = db

	db, err = openDb(filepath.Join(path, md5sha1DBName))
	if err != nil {
		return nil, err
	}
	kvdb.md5sha1DB = db

	return kvdb, nil
}

func dbSha1Append(db KVStore, batch KVBatch, key, sha1Bytes []byte) error {
	if key == nil {
		return nil
	}

	vBytes, err := db.Get(key)
	if err != nil {
		return fmt.Errorf("failed to lookup in dbSha1Append: %v", err)
	}

	found := false
	for i := 0; i < len(vBytes); i += sha1.Size {
		if bytes.Equal(sha1Bytes, vBytes[i:i+sha1.Size]) {
			found = true
			break
		}
	}

	if !found {
		vBytes = append(vBytes, sha1Bytes...)
		batch.Set(key, vBytes)
	}
	return nil
}

func init() {
	DBFactory = NewKVStoreDB
}

func (kvdb *kvStore) IndexRom(rom *types.Rom) error {
	batch := kvdb.StartBatch()
	err := batch.IndexRom(rom)
	if err != nil {
		return err
	}
	return batch.Close()
}

func (kvdb *kvStore) IndexDat(dat *types.Dat, sha1Bytes []byte) error {
	batch := kvdb.StartBatch()
	err := batch.IndexDat(dat, sha1Bytes)
	if err != nil {
		return err
	}
	return batch.Close()
}

func (kvdb *kvStore) OrphanDats() error {
	kvdb.generation++
	err := WriteGenerationFile(kvdb.path, kvdb.generation)
	if err != nil {
		return err
	}
	return nil
}

func (kvdb *kvStore) GetDat(sha1Bytes []byte) (*types.Dat, error) {
	dBytes, err := kvdb.datsDB.Get(sha1Bytes)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(dBytes)
	datDecoder := gob.NewDecoder(buf)

	var dat types.Dat

	err = datDecoder.Decode(&dat)
	if err != nil {
		return nil, err
	}
	return &dat, nil
}

func (kvdb *kvStore) DatsForRom(rom *types.Rom) ([]*types.Dat, error) {
	var dBytes []byte
	var err error

	// TODO(uwe): crcsha1, md5sha1

	if rom.Sha1 != nil {
		dBytes, err = kvdb.sha1DB.Get(rom.Sha1)
		if err != nil {
			return nil, err
		}
	} else if rom.Md5 != nil {
		dBytes, err = kvdb.md5DB.Get(rom.Md5)
		if err != nil {
			return nil, err
		}
	} else if rom.Crc != nil {
		dBytes, err = kvdb.crcDB.Get(rom.Crc)
		if err != nil {
			return nil, err
		}
	}

	if dBytes == nil {
		return nil, nil
	}

	var dats []*types.Dat

	for i := 0; i < len(dBytes); i += sha1.Size {
		sha1Bytes := dBytes[i : i+sha1.Size]
		dat, err := kvdb.GetDat(sha1Bytes)
		if err != nil {
			return nil, err
		}
		dats = append(dats, dat)
	}

	return dats, nil
}

func (kvdb *kvStore) Close() error {
	err := kvdb.datsDB.Close()
	if err != nil {
		return err
	}

	err = kvdb.crcDB.Close()
	if err != nil {
		return err
	}

	err = kvdb.md5DB.Close()
	if err != nil {
		return err
	}

	err = kvdb.sha1DB.Close()
	if err != nil {
		return err
	}

	err = kvdb.crcsha1DB.Close()
	if err != nil {
		return err
	}

	err = kvdb.md5sha1DB.Close()
	if err != nil {
		return err
	}
	return nil
}

func (kvdb *kvStore) StartBatch() RomBatch {
	return &kvBatch{
		db:           kvdb,
		datsBatch:    kvdb.datsDB.StartBatch(),
		crcBatch:     kvdb.crcDB.StartBatch(),
		md5Batch:     kvdb.md5DB.StartBatch(),
		sha1Batch:    kvdb.sha1DB.StartBatch(),
		crcsha1Batch: kvdb.crcsha1DB.StartBatch(),
		md5sha1Batch: kvdb.md5sha1DB.StartBatch(),
	}
}

func (kvb *kvBatch) Flush() error {
	err := kvb.db.datsDB.WriteBatch(kvb.datsBatch)
	if err != nil {
		return err
	}
	kvb.datsBatch.Clear()

	err = kvb.db.crcDB.WriteBatch(kvb.crcBatch)
	if err != nil {
		return err
	}
	kvb.crcBatch.Clear()

	err = kvb.db.md5DB.WriteBatch(kvb.md5Batch)
	if err != nil {
		return err
	}
	kvb.md5Batch.Clear()

	err = kvb.db.sha1DB.WriteBatch(kvb.sha1Batch)
	if err != nil {
		return err
	}
	kvb.sha1Batch.Clear()

	err = kvb.db.crcsha1DB.WriteBatch(kvb.crcsha1Batch)
	if err != nil {
		return err
	}
	kvb.crcsha1Batch.Clear()

	err = kvb.db.md5sha1DB.WriteBatch(kvb.md5sha1Batch)
	if err != nil {
		return err
	}
	kvb.md5sha1Batch.Clear()

	kvb.size = 0

	return nil
}

func (kvb *kvBatch) Close() error {
	return kvb.Flush()
}

func (kvb *kvBatch) IndexRom(rom *types.Rom) error {
	dats, err := kvb.db.DatsForRom(rom)
	if err != nil {
		return err
	}

	if len(dats) > 0 {
		return nil
	}

	dat := new(types.Dat)
	dat.Artificial = true
	dat.Generation = kvb.db.generation
	dat.Name = fmt.Sprintf("Artificial Dat for %s", rom.Name)
	dat.Path = rom.Path
	game := new(types.Game)
	game.Roms = []*types.Rom{rom}
	dat.Games = []*types.Game{game}

	var buf bytes.Buffer

	gobEncoder := gob.NewEncoder(&buf)
	err = gobEncoder.Encode(dat)
	if err != nil {
		return err
	}

	hh := sha1.New()
	_, err = io.Copy(hh, &buf)
	if err != nil {
		return err
	}

	// TODO(uwe) crcsha1, md5sha1

	return kvb.IndexDat(dat, hh.Sum(nil))
}

func (kvb *kvBatch) IndexDat(dat *types.Dat, sha1Bytes []byte) error {
	if sha1Bytes == nil {
		return fmt.Errorf("sha1 is nil for %s", dat.Path)
	}

	dat.Generation = kvb.db.generation

	var buf bytes.Buffer

	gobEncoder := gob.NewEncoder(&buf)
	err := gobEncoder.Encode(dat)
	if err != nil {
		return err
	}

	dBytes, err := kvb.db.datsDB.Get(sha1Bytes)
	if err != nil {
		return fmt.Errorf("failed to lookup sha1 indexing dats: %v", err)
	}

	// TODO(uwe) crcsha1, md5sha1

	kvb.datsBatch.Set(sha1Bytes, buf.Bytes())

	kvb.size += int64(sha1.Size + buf.Len())

	if dBytes == nil {
		for _, g := range dat.Games {
			for _, r := range g.Roms {
				if r.Sha1 != nil {
					err = dbSha1Append(kvb.db.sha1DB, kvb.sha1Batch, r.Sha1, sha1Bytes)
					if err != nil {
						return err
					}
					kvb.size += int64(sha1.Size)
				}

				if r.Md5 != nil {
					err = dbSha1Append(kvb.db.md5DB, kvb.md5Batch, r.Md5, sha1Bytes)
					if err != nil {
						return err
					}
					kvb.size += int64(sha1.Size)
				}

				if r.Crc != nil {
					err = dbSha1Append(kvb.db.crcDB, kvb.crcBatch, r.Crc, sha1Bytes)
					if err != nil {
						return err
					}
					kvb.size += int64(sha1.Size)
				}
			}
		}
	}
	return nil
}

func (kvb *kvBatch) Size() int64 {
	return kvb.size
}
