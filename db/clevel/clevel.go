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

package clevel

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"github.com/jmhodges/levigo"
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
)

type levelRomDB struct {
	generation int64
	datsDB     *levigo.DB
	crcDB      *levigo.DB
	md5DB      *levigo.DB
	sha1DB     *levigo.DB
	crcsha1DB  *levigo.DB
	md5sha1DB  *levigo.DB
	path       string
}

type levelRomBatch struct {
	db           *levelRomDB
	datsBatch    *levigo.WriteBatch
	crcBatch     *levigo.WriteBatch
	md5Batch     *levigo.WriteBatch
	sha1Batch    *levigo.WriteBatch
	crcsha1Batch *levigo.WriteBatch
	md5sha1Batch *levigo.WriteBatch
	size         int64
}

var rOptions *levigo.ReadOptions = levigo.NewReadOptions()
var wOptions *levigo.WriteOptions = levigo.NewWriteOptions()

func openDb(path string) (*levigo.DB, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	return levigo.Open(path, opts)
}

func dbSha1Append(db *levigo.DB, batch *levigo.WriteBatch, key, sha1Bytes []byte) error {
	if key == nil {
		return nil
	}

	vBytes, err := db.Get(rOptions, key)
	if err != nil {
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
	dBytes, err := lrdb.datsDB.Get(rOptions, sha1Bytes)
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

func (lrdb *levelRomDB) DatsForRom(rom *types.Rom) ([]*types.Dat, error) {
	var dBytes []byte
	var err error

	if rom.Sha1 != nil {
		dBytes, err = lrdb.sha1DB.Get(rOptions, rom.Sha1)
		if err != nil {
			return nil, err
		}
	} else if rom.Md5 != nil {
		dBytes, err = lrdb.md5DB.Get(rOptions, rom.Md5)
		if err != nil {
			return nil, err
		}
	} else if rom.Crc != nil {
		dBytes, err = lrdb.crcDB.Get(rOptions, rom.Crc)
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
		dat, err := lrdb.GetDat(sha1Bytes)
		if err != nil {
			return nil, err
		}
		dats = append(dats, dat)
	}

	return dats, nil
}

func (lrdb *levelRomDB) Close() error {
	lrdb.datsDB.Close()
	lrdb.crcDB.Close()
	lrdb.md5DB.Close()
	lrdb.sha1DB.Close()
	lrdb.crcsha1DB.Close()
	lrdb.md5sha1DB.Close()
	return nil
}

func (lrdb *levelRomDB) StartBatch() db.RomBatch {
	return &levelRomBatch{
		db:           lrdb,
		datsBatch:    levigo.NewWriteBatch(),
		crcBatch:     levigo.NewWriteBatch(),
		md5Batch:     levigo.NewWriteBatch(),
		sha1Batch:    levigo.NewWriteBatch(),
		crcsha1Batch: levigo.NewWriteBatch(),
		md5sha1Batch: levigo.NewWriteBatch(),
	}
}

func (lrb *levelRomBatch) Flush() error {
	err := lrb.db.datsDB.Write(wOptions, lrb.datsBatch)
	if err != nil {
		return err
	}
	lrb.datsBatch.Clear()

	err = lrb.db.crcDB.Write(wOptions, lrb.crcBatch)
	if err != nil {
		return err
	}
	lrb.crcBatch.Clear()

	err = lrb.db.md5DB.Write(wOptions, lrb.md5Batch)
	if err != nil {
		return err
	}
	lrb.md5Batch.Clear()

	err = lrb.db.sha1DB.Write(wOptions, lrb.sha1Batch)
	if err != nil {
		return err
	}
	lrb.sha1Batch.Clear()

	err = lrb.db.crcsha1DB.Write(wOptions, lrb.crcsha1Batch)
	if err != nil {
		return err
	}
	lrb.crcsha1Batch.Clear()

	err = lrb.db.md5sha1DB.Write(wOptions, lrb.md5sha1Batch)
	if err != nil {
		return err
	}
	lrb.md5sha1Batch.Clear()

	lrb.size = 0

	return nil
}

func (lrb *levelRomBatch) Close() error {
	lrb.datsBatch.Close()
	lrb.crcBatch.Close()
	lrb.md5Batch.Close()
	lrb.sha1Batch.Close()
	lrb.crcsha1Batch.Close()
	lrb.md5sha1Batch.Close()
	lrb.size = 0
	return nil
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

	dBytes, err := lrb.db.datsDB.Get(rOptions, sha1Bytes)
	if err != nil {
		return err
	}

	lrb.datsBatch.Put(sha1Bytes, buf.Bytes())

	lrb.size += int64(sha1.Size + buf.Len())

	if dBytes == nil {
		for _, g := range dat.Games {
			for _, r := range g.Roms {
				if r.Sha1 != nil {
					err = dbSha1Append(lrb.db.sha1DB, lrb.sha1Batch, r.Sha1, sha1Bytes)
					if err != nil {
						return err
					}
					lrb.size += int64(sha1.Size)
				}

				if r.Md5 != nil {
					err = dbSha1Append(lrb.db.md5DB, lrb.md5Batch, r.Md5, sha1Bytes)
					if err != nil {
						return err
					}
					lrb.size += int64(sha1.Size)
				}

				if r.Crc != nil {
					err = dbSha1Append(lrb.db.crcDB, lrb.crcBatch, r.Crc, sha1Bytes)
					if err != nil {
						return err
					}
					lrb.size += int64(sha1.Size)
				}
			}
		}
	}
	return nil
}

func (lrb *levelRomBatch) Size() int64 {
	return lrb.size
}
