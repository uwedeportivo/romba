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
	"crypto/md5"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"path/filepath"

	"github.com/uwedeportivo/romba/combine"

	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/util"

	"github.com/golang/glog"
)

const (
	datsDBName    = "dats_db"
	crcDBName     = "crc_db"
	md5DBName     = "md5_db"
	sha1DBName    = "sha1_db"
	crcsha1DBName = "crcsha1_db"
	md5sha1DBName = "md5sha1_db"
)

var oneValue []byte

func init() {
	oneValue = make([]byte, 1)
	oneValue[0] = 1
}

type KVStore interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	GetKeySuffixesFor(keyPrefix []byte) ([]byte, error)
	Exists(key []byte) (bool, error)
	Flush()
	Size() int64
	StartBatch() KVBatch
	WriteBatch(batch KVBatch) error
	Close() error
	BeginRefresh() error
	EndRefresh() error
	PrintStats() string
	Iterate(func(key, value []byte) (bool, error)) error
}

type KVBatch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Clear()
}

var StoreOpener func(pathPrefix string, keySize int) (KVStore, error)

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

func openDb(pathPrefix string, keySize int) (KVStore, error) {
	return StoreOpener(pathPrefix, keySize)
}

func NewKVStoreDB(path string) (RomDB, error) {
	kvdb := new(kvStore)
	kvdb.path = path

	glog.Infof("Loading Generation File")
	gen, err := ReadGenerationFile(path)
	if err != nil {
		return nil, err
	}
	kvdb.generation = gen

	glog.Infof("Loading Dats DB")
	db, err := openDb(filepath.Join(path, datsDBName), sha1.Size)
	if err != nil {
		return nil, err
	}
	kvdb.datsDB = db

	glog.Infof("Loading CRC DB")
	db, err = openDb(filepath.Join(path, crcDBName), crc32.Size+sha1.Size+8)
	if err != nil {
		return nil, err
	}
	kvdb.crcDB = db

	glog.Infof("Loading MD5 DB")
	db, err = openDb(filepath.Join(path, md5DBName), md5.Size+sha1.Size+8)
	if err != nil {
		return nil, err
	}
	kvdb.md5DB = db

	glog.Infof("Loading SHA1 DB")
	db, err = openDb(filepath.Join(path, sha1DBName), sha1.Size)
	if err != nil {
		return nil, err
	}
	kvdb.sha1DB = db

	glog.Infof("Loading CRC -> SHA1 DB")
	db, err = openDb(filepath.Join(path, crcsha1DBName), crc32.Size+sha1.Size+8)
	if err != nil {
		return nil, err
	}
	kvdb.crcsha1DB = db

	glog.Infof("Loading MD5 -> SHA1 DB")
	db, err = openDb(filepath.Join(path, md5sha1DBName), md5.Size+sha1.Size+8)
	if err != nil {
		return nil, err
	}
	kvdb.md5sha1DB = db

	return kvdb, nil
}

func init() {
	Factory = NewKVStoreDB
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

func (kvdb *kvStore) Generation() int64 {
	return kvdb.generation
}

func decodeDat(dBytes []byte) (*types.Dat, error) {
	if dBytes == nil {
		return nil, nil
	}
	buf := bytes.NewBuffer(dBytes)
	datDecoder := gob.NewDecoder(buf)

	var dat types.Dat

	err := datDecoder.Decode(&dat)
	if err != nil {
		return nil, err
	}
	return &dat, nil
}

func (kvdb *kvStore) GetDat(sha1Bytes []byte) (*types.Dat, error) {
	dBytes, err := kvdb.datsDB.Get(sha1Bytes)
	if err != nil {
		return nil, err
	}
	fmt.Printf("decoding dat at key %s \n", hex.EncodeToString(sha1Bytes))
	return decodeDat(dBytes)
}

func (kvdb *kvStore) FilteredDatsForRom(rom *types.Rom, filter func(*types.Dat) bool) ([]*types.Dat, []*types.Dat, error) {
	var dBytes []byte

	if rom.Sha1 != nil {
		bs, err := kvdb.sha1DB.GetKeySuffixesFor(rom.Sha1)
		if err != nil {
			return nil, nil, err
		}
		if bs != nil {
			dBytes = append(dBytes, bs...)
		}
	}
	if rom.Md5 != nil {
		bs, err := kvdb.md5DB.GetKeySuffixesFor(rom.Md5WithSizeKey())
		if err != nil {
			return nil, nil, err
		}
		if bs != nil {
			dBytes = append(dBytes, bs...)
		}
	}
	if rom.Crc != nil {
		bs, err := kvdb.crcDB.GetKeySuffixesFor(rom.CrcWithSizeKey())
		if err != nil {
			return nil, nil, err
		}
		if bs != nil {
			dBytes = append(dBytes, bs...)
		}
	}

	if dBytes == nil {
		return nil, nil, nil
	}

	var dats []*types.Dat
	var rejectedDats []*types.Dat

	seen := make(map[string]bool)

	for i := 0; i < len(dBytes); i += sha1.Size {
		sha1Bytes := dBytes[i : i+sha1.Size]

		if seen[string(sha1Bytes)] {
			continue
		}
		seen[string(sha1Bytes)] = true

		dat, err := kvdb.GetDat(sha1Bytes)
		if err != nil {
			return nil, nil, err
		}
		if dat != nil {
			if filter(dat) {
				dats = append(dats, dat)
			} else {
				rejectedDats = append(rejectedDats, dat)
			}
		}
	}

	return dats, rejectedDats, nil
}

func (kvdb *kvStore) DatsForRom(rom *types.Rom) ([]*types.Dat, error) {
	dats, _, err := kvdb.FilteredDatsForRom(rom, func(dat *types.Dat) bool {
		return dat.Generation == kvdb.Generation()
	})
	return dats, err
}

// CompleteRom completes the rom by adding missing hashes. If there are
// additional roms that collide with the provided crc or md5, then these
// additional roms are returned in the rom slice.
func (kvdb *kvStore) CompleteRom(rom *types.Rom) ([]*types.Rom, error) {
	if rom.Sha1 != nil {
		return nil, nil
	}

	if rom.Md5 != nil {
		dBytes, err := kvdb.md5sha1DB.GetKeySuffixesFor(rom.Md5WithSizeKey())
		if err != nil {
			return nil, err
		}
		if len(dBytes) < sha1.Size {
			return nil, nil
		}
		rom.Sha1 = dBytes[:sha1.Size]
		if len(dBytes) == sha1.Size {
			return nil, nil
		}
		var croms []*types.Rom
		for rb := dBytes[sha1.Size:]; len(rb) >= sha1.Size; rb = rb[sha1.Size:] {
			croms = append(croms, &types.Rom{
				Sha1: rb[:sha1.Size],
				Md5:  rom.Md5,
				Crc:  rom.Crc,
				Name: rom.Name,
				Size: rom.Size,
			})
		}
		return croms, nil
	}

	if rom.Crc != nil {
		dBytes, err := kvdb.crcsha1DB.GetKeySuffixesFor(rom.CrcWithSizeKey())
		if err != nil {
			return nil, err
		}
		if len(dBytes) < sha1.Size {
			return nil, nil
		}
		rom.Sha1 = dBytes[:sha1.Size]
		if len(dBytes) == sha1.Size {
			return nil, nil
		}
		var croms []*types.Rom
		for rb := dBytes[sha1.Size:]; len(rb) >= sha1.Size; rb = rb[sha1.Size:] {
			croms = append(croms, &types.Rom{
				Sha1: rb[:sha1.Size],
				Md5:  rom.Md5,
				Crc:  rom.Crc,
				Name: rom.Name,
				Size: rom.Size,
			})
		}
		return croms, nil
	}
	return nil, nil
}

func (kvdb *kvStore) Flush() {
	kvdb.datsDB.Flush()
	kvdb.crcDB.Flush()
	kvdb.md5DB.Flush()
	kvdb.sha1DB.Flush()
	kvdb.crcsha1DB.Flush()
	kvdb.md5sha1DB.Flush()
}

func (kvdb *kvStore) Close() error {
	kvdb.Flush()

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

func (kvdb *kvStore) BeginDatRefresh() error {
	return kvdb.datsDB.BeginRefresh()
}

func (kvdb *kvStore) PrintStats() string {
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "\ndatsDB stats: %s\n", kvdb.datsDB.PrintStats())
	fmt.Fprintf(buf, "crcDB stats: %s\n", kvdb.crcDB.PrintStats())
	fmt.Fprintf(buf, "md5DB stats: %s\n", kvdb.md5DB.PrintStats())
	fmt.Fprintf(buf, "sha1DB stats: %s\n", kvdb.sha1DB.PrintStats())
	fmt.Fprintf(buf, "crcsha1DB stats: %s\n", kvdb.crcsha1DB.PrintStats())
	fmt.Fprintf(buf, "md5sha1DB stats: %s\n", kvdb.md5sha1DB.PrintStats())

	return buf.String()
}

func (kvdb *kvStore) EndDatRefresh() error {
	return kvdb.datsDB.EndRefresh()
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
	if kvb.size == 0 {
		return nil
	}

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
	err := kvb.Flush()
	kvb.db = nil
	return err
}

func (kvb *kvBatch) IndexRom(rom *types.Rom) error {
	glog.V(4).Infof("indexing rom %s", rom.Name)

	if rom.Sha1 != nil {
		if rom.Crc != nil {
			glog.V(4).Infof("declaring crc %s -> sha1 %s mapping", hex.EncodeToString(rom.Crc), hex.EncodeToString(rom.Sha1))
			err := kvb.crcsha1Batch.Set(rom.CrcWithSizeAndSha1Key(nil), oneValue)
			if err != nil {
				return err
			}
			kvb.size += int64(sha1.Size)
		}
		if rom.Md5 != nil {
			glog.V(4).Infof("declaring md5 %s -> sha1 %s mapping", hex.EncodeToString(rom.Md5), hex.EncodeToString(rom.Sha1))
			err := kvb.md5sha1Batch.Set(rom.Md5WithSizeAndSha1Key(nil), oneValue)
			if err != nil {
				return err
			}
			kvb.size += int64(sha1.Size)
		}
	} else {
		glog.V(4).Infof("indexing rom %s with missing SHA1", rom.Name)
	}

	return nil
}

func (kvb *kvBatch) IndexDat(dat *types.Dat, sha1Bytes []byte) error {
	glog.V(4).Infof("indexing dat %s", dat.Name)

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

	exists, err := kvb.db.datsDB.Exists(sha1Bytes)
	if err != nil {
		return fmt.Errorf("failed to lookup sha1 indexing dats: %v", err)
	}

	kvb.datsBatch.Set(sha1Bytes, buf.Bytes())
	kvb.size += int64(sha1.Size + buf.Len())

	if !exists {
		for _, g := range dat.Games {
			glog.V(4).Infof("indexing game %s", g.Name)
			for _, r := range g.Roms {
				if r.Sha1 != nil {
					err = kvb.sha1Batch.Set(r.Sha1Sha1Key(sha1Bytes), oneValue)
					if err != nil {
						return err
					}
					kvb.size += int64(sha1.Size)
				}

				if r.Md5 != nil {
					err = kvb.md5Batch.Set(r.Md5WithSizeAndSha1Key(sha1Bytes), oneValue)
					if err != nil {
						return err
					}
					kvb.size += int64(sha1.Size)

					if r.Sha1 != nil {
						glog.V(4).Infof("declaring md5 %s -> sha1 %s mapping", hex.EncodeToString(r.Md5), hex.EncodeToString(r.Sha1))
						err = kvb.md5sha1Batch.Set(r.Md5WithSizeAndSha1Key(nil), oneValue)
						if err != nil {
							return err
						}
						kvb.size += int64(sha1.Size)
					}
				}

				if r.Crc != nil {
					err = kvb.crcBatch.Set(r.CrcWithSizeAndSha1Key(sha1Bytes), oneValue)
					if err != nil {
						return err
					}
					kvb.size += int64(sha1.Size)

					if r.Sha1 != nil {
						glog.V(4).Infof("declaring crc %s -> sha1 %s mapping", hex.EncodeToString(r.Crc), hex.EncodeToString(r.Sha1))
						err = kvb.crcsha1Batch.Set(r.CrcWithSizeAndSha1Key(nil), oneValue)
						if err != nil {
							return err
						}
						kvb.size += int64(sha1.Size)
					}
				}
			}
		}
	}
	return nil
}

func (kvb *kvBatch) Size() int64 {
	return kvb.size
}

func printSha1s(vBytes []byte) string {
	var buf bytes.Buffer

	buf.WriteString("[")
	first := true
	for i := 0; i < len(vBytes); i += sha1.Size {
		sha1 := hex.EncodeToString(vBytes[i : i+sha1.Size])
		if first {
			first = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(sha1)
	}
	buf.WriteString("]")
	return buf.String()
}

func (kvdb *kvStore) DebugGet(key []byte, size int64) string {
	var buf bytes.Buffer

	switch len(key) {
	case md5.Size:
		sizeBytes := make([]byte, 8)
		util.Int64ToBytes(size, sizeBytes)
		key = append(key, sizeBytes...)
		sha1s, err := kvdb.md5DB.GetKeySuffixesFor(key)
		if err != nil {
			glog.Errorf("error getting from md5DB: %v", err)
		} else {
			buf.WriteString(fmt.Sprintf("md5DB -> %s\n", printSha1s(sha1s)))
		}

		sha1s, err = kvdb.md5sha1DB.GetKeySuffixesFor(key)
		if err != nil {
			glog.Errorf("error getting from md5sha1DB: %v", err)
		} else {
			buf.WriteString(fmt.Sprintf("md5sha1DB -> %s\n", printSha1s(sha1s)))
		}
	case crc32.Size:
		sizeBytes := make([]byte, 8)
		util.Int64ToBytes(size, sizeBytes)
		key = append(key, sizeBytes...)

		sha1s, err := kvdb.crcDB.GetKeySuffixesFor(key)
		if err != nil {
			glog.Errorf("error getting from crcDB: %v", err)
		} else {
			buf.WriteString(fmt.Sprintf("crcDB -> %s\n", printSha1s(sha1s)))
		}

		sha1s, err = kvdb.crcsha1DB.GetKeySuffixesFor(key)
		if err != nil {
			glog.Errorf("error getting from crcsha1DB: %v", err)
		} else {
			buf.WriteString(fmt.Sprintf("crcsha1DB -> %s\n", printSha1s(sha1s)))
		}
	case sha1.Size:
		sha1s, err := kvdb.sha1DB.GetKeySuffixesFor(key)
		if err != nil {
			glog.Errorf("error getting from sha1DB: %v", err)
		} else {
			buf.WriteString(fmt.Sprintf("sha1DB -> %s\n", printSha1s(sha1s)))
		}
	default:
		glog.Errorf("found unknown hash size: %d", len(key))
		return ""
	}

	return buf.String()
}

func (kvdb *kvStore) ResolveHash(key []byte) ([]byte, error) {
	switch len(key) {
	case md5.Size:
		return kvdb.md5sha1DB.GetKeySuffixesFor(key)
	case crc32.Size:
		return kvdb.crcsha1DB.GetKeySuffixesFor(key)
	default:
		return nil, fmt.Errorf("crc or md5 hash expected, got hash size: %d", len(key))
	}
}

func (kvdb *kvStore) ForEachDat(datF func(dat *types.Dat) error) error {
	return kvdb.datsDB.Iterate(func(key, value []byte) (bool, error) {
		dat, err := decodeDat(value)
		if err != nil {
			return false, err
		}
		err = datF(dat)
		if err != nil {
			return false, err
		}
		return true, nil
	})
}

func (kvdb *kvStore) JoinCrcMd5(combiner combine.Combiner) error {
	glog.V(4).Infof("leveldb combiner processing crc mappings")
	err := kvdb.crcsha1DB.Iterate(func(key, value []byte) (bool, error) {
		rom := new(types.Rom)

		rom.Sha1 = key[crc32.Size+8:]
		rom.Crc = key[:crc32.Size]
		rom.Size = util.BytesToInt64(key[crc32.Size : crc32.Size+8])

		err := combiner.Declare(rom)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	glog.V(4).Infof("leveldb combiner processing md5 mappings")
	return kvdb.md5sha1DB.Iterate(func(key, value []byte) (bool, error) {
		rom := new(types.Rom)

		rom.Sha1 = key[md5.Size+8:]
		rom.Md5 = key[:md5.Size]
		rom.Size = util.BytesToInt64(key[md5.Size : md5.Size+8])

		err := combiner.Declare(rom)
		if err != nil {
			return false, err
		}
		return true, nil
	})
}

func (kvdb *kvStore) NumRoms() int64 {
	return kvdb.sha1DB.Size()
}
