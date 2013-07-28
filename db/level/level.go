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
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/uwedeportivo/romba/db"
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

	generationFilename = "romba-generation"

	batchSize = 5242880
)

type levelRomDB struct {
	generation int64
	datsDB     *leveldb.DB
	crcDB      *leveldb.DB
	md5DB      *leveldb.DB
	sha1DB     *leveldb.DB
	crcsha1DB  *leveldb.DB
	md5sha1DB  *leveldb.DB
	path       string
}

type levelRomBatch struct {
	db           *levelRomDB
	datsBatch    *leveldb.Batch
	crcBatch     *leveldb.Batch
	md5Batch     *leveldb.Batch
	sha1Batch    *leveldb.Batch
	crcsha1Batch *leveldb.Batch
	md5sha1Batch *leveldb.Batch
}

var rOptions *opt.ReadOptions = &opt.ReadOptions{}
var wOptions *opt.WriteOptions = &opt.WriteOptions{}

func openDb(path string) (*leveldb.DB, error) {
	stor, err := storage.OpenFile(path)
	if err != nil {
		return nil, err
	}

	db, err := leveldb.Open(stor, &opt.Options{
		Flag:         opt.OFCreateIfMissing,
		WriteBuffer:  62914560,
		MaxOpenFiles: 500,
		BlockCache:   cache.NewLRUCache(10490000),
		Filter:       filter.NewBloomFilter(16),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open db at %s: %v\n", path, err)
	}
	return db, nil
}

func dbSha1Append(db *leveldb.DB, batch *leveldb.Batch, key, sha1Bytes []byte) error {
	if key == nil {
		return nil
	}

	vBytes, err := db.Get(key, rOptions)
	if err != nil && err != errors.ErrNotFound {
		return err
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
		batch.Put(key, vBytes)
	}
	return nil
}

func init() {
	db.DBFactory = NewLevelDB
}

func NewLevelDB(path string) (db.RomDB, error) {
	lrdb := new(levelRomDB)
	lrdb.path = path

	gen, err := db.ReadGenerationFile(path)
	if err != nil {
		return nil, err
	}
	lrdb.generation = gen

	db, err := openDb(filepath.Join(path, datsDBName))
	if err != nil {
		return nil, err
	}
	lrdb.datsDB = db

	db, err = openDb(filepath.Join(path, crcDBName))
	if err != nil {
		return nil, err
	}
	lrdb.crcDB = db

	db, err = openDb(filepath.Join(path, md5DBName))
	if err != nil {
		return nil, err
	}
	lrdb.md5DB = db

	db, err = openDb(filepath.Join(path, sha1DBName))
	if err != nil {
		return nil, err
	}
	lrdb.sha1DB = db

	db, err = openDb(filepath.Join(path, crcsha1DBName))
	if err != nil {
		return nil, err
	}
	lrdb.crcsha1DB = db

	db, err = openDb(filepath.Join(path, md5sha1DBName))
	if err != nil {
		return nil, err
	}
	lrdb.md5sha1DB = db

	return lrdb, nil
}

func (lrdb *levelRomDB) IndexRom(rom *types.Rom) error {
	batch := lrdb.StartBatch()
	err := batch.IndexRom(rom)
	if err != nil {
		return err
	}
	return batch.Close()
}

func (lrdb *levelRomDB) IndexDat(dat *types.Dat, sha1Bytes []byte) error {
	batch := lrdb.StartBatch()
	err := batch.IndexDat(dat, sha1Bytes)
	if err != nil {
		return err
	}
	return batch.Close()
}

func (lrdb *levelRomDB) OrphanDats() error {
	lrdb.generation++
	err := db.WriteGenerationFile(lrdb.path, lrdb.generation)
	if err != nil {
		return err
	}
	return nil
}

func (lrdb *levelRomDB) GetDat(sha1Bytes []byte) (*types.Dat, error) {
	dBytes, err := lrdb.datsDB.Get(sha1Bytes, rOptions)
	if err != nil && err != errors.ErrNotFound {
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

func (lrdb *levelRomDB) DatsForRom(rom *types.Rom) ([]*types.Dat, error) {
	var dBytes []byte
	var err error

	if rom.Sha1 != nil {
		dBytes, err = lrdb.sha1DB.Get(rom.Sha1, rOptions)
		if err != nil && err != errors.ErrNotFound {
			return nil, err
		}
	} else if rom.Md5 != nil {
		dBytes, err = lrdb.md5DB.Get(rom.Md5, rOptions)
		if err != nil && err != errors.ErrNotFound {
			return nil, err
		}
	} else if rom.Crc != nil {
		dBytes, err = lrdb.crcDB.Get(rom.Crc, rOptions)
		if err != nil && err != errors.ErrNotFound {
			return nil, err
		}
	}

	if dBytes == nil {
		return nil, nil
	}

	var dats []*types.Dat

	for i := 0; i < len(dBytes); i += sha1.Size {
		sha1Bytes := dBytes[i : i+sha1.Size]
		dat, err := lrdb.GetDat(sha1Bytes)
		if err != nil {
			return nil, err
		}
		dats = append(dats, dat)
	}

	return dats, nil
}

func (lrdb *levelRomDB) Close() error {
	err := lrdb.datsDB.Close()
	if err != nil {
		return err
	}
	err = lrdb.crcDB.Close()
	if err != nil {
		return err
	}
	err = lrdb.md5DB.Close()
	if err != nil {
		return err
	}
	err = lrdb.sha1DB.Close()
	if err != nil {
		return err
	}
	err = lrdb.crcsha1DB.Close()
	if err != nil {
		return err
	}
	err = lrdb.md5sha1DB.Close()
	if err != nil {
		return err
	}
	return nil
}

func (lrdb *levelRomDB) StartBatch() db.RomBatch {
	return &levelRomBatch{
		db:           lrdb,
		datsBatch:    leveldb.NewBatch(batchSize),
		crcBatch:     leveldb.NewBatch(batchSize),
		md5Batch:     leveldb.NewBatch(batchSize),
		sha1Batch:    leveldb.NewBatch(batchSize),
		crcsha1Batch: leveldb.NewBatch(batchSize),
		md5sha1Batch: leveldb.NewBatch(batchSize),
	}
}

func (lrb *levelRomBatch) Flush() error {
	err := lrb.db.datsDB.Write(lrb.datsBatch, wOptions)
	if err != nil {
		return err
	}
	lrb.datsBatch.Reset()

	err = lrb.db.crcDB.Write(lrb.crcBatch, wOptions)
	if err != nil {
		return err
	}
	lrb.crcBatch.Reset()

	err = lrb.db.md5DB.Write(lrb.md5Batch, wOptions)
	if err != nil {
		return err
	}
	lrb.md5Batch.Reset()

	err = lrb.db.sha1DB.Write(lrb.sha1Batch, wOptions)
	if err != nil {
		return err
	}
	lrb.sha1Batch.Reset()

	err = lrb.db.crcsha1DB.Write(lrb.crcsha1Batch, wOptions)
	if err != nil {
		return err
	}
	lrb.crcsha1Batch.Reset()

	err = lrb.db.md5sha1DB.Write(lrb.md5sha1Batch, wOptions)
	if err != nil {
		return err
	}
	lrb.md5sha1Batch.Reset()

	return nil
}

func (lrb *levelRomBatch) Close() error {
	return lrb.Flush()
}

func (lrb *levelRomBatch) IndexRom(rom *types.Rom) error {
	dats, err := lrb.db.DatsForRom(rom)
	if err != nil {
		return err
	}

	if len(dats) > 0 {
		return nil
	}

	dat := new(types.Dat)
	dat.Artificial = true
	dat.Generation = lrb.db.generation
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

	return lrb.IndexDat(dat, hh.Sum(nil))
}

func (lrb *levelRomBatch) IndexDat(dat *types.Dat, sha1Bytes []byte) error {
	if sha1Bytes == nil {
		return fmt.Errorf("sha1 is nil for %s", dat.Path)
	}

	dat.Generation = lrb.db.generation

	var buf bytes.Buffer

	gobEncoder := gob.NewEncoder(&buf)
	err := gobEncoder.Encode(dat)
	if err != nil {
		return err
	}

	dBytes, err := lrb.db.datsDB.Get(sha1Bytes, rOptions)
	if err != nil && err != errors.ErrNotFound {
		return err
	}

	lrb.datsBatch.Put(sha1Bytes, buf.Bytes())

	if dBytes == nil {
		for _, g := range dat.Games {
			for _, r := range g.Roms {
				if r.Sha1 != nil {
					err = dbSha1Append(lrb.db.sha1DB, lrb.sha1Batch, r.Sha1, sha1Bytes)
					if err != nil {
						return err
					}
				}

				if r.Md5 != nil {
					err = dbSha1Append(lrb.db.md5DB, lrb.md5Batch, r.Md5, sha1Bytes)
					if err != nil {
						return err
					}
				}

				if r.Crc != nil {
					err = dbSha1Append(lrb.db.crcDB, lrb.crcBatch, r.Crc, sha1Bytes)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (lrb *levelRomBatch) Size() int64 {
	return int64(lrb.datsBatch.Size()) + int64(lrb.crcBatch.Size()) +
		int64(lrb.md5Batch.Size()) + int64(lrb.sha1Batch.Size()) +
		int64(lrb.crcsha1Batch.Size()) + int64(lrb.md5sha1Batch.Size())
}
