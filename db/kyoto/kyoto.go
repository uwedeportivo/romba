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

package kyoto

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"github.com/uwedeportivo/cabinet"
	"github.com/uwedeportivo/romba/types"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

const (
	datsDBName    = "dats.kch"
	crcDBName     = "crc.kch"
	md5DBName     = "md5.kch"
	sha1DBName    = "sha1.kch"
	crcsha1DBName = "crcsha1.kch"
	md5sha1DBName = "md5sha1.kch"

	generationFilename = ".romba-generation"
)

type KyotoRomDB struct {
	generation int64
	datsDB     *cabinet.KCDB
	crcDB      *cabinet.KCDB
	md5DB      *cabinet.KCDB
	sha1DB     *cabinet.KCDB
	crcsha1DB  *cabinet.KCDB
	md5sha1DB  *cabinet.KCDB
	path       string
}

func New(path string) (*KyotoRomDB, error) {
	krdb := new(KyotoRomDB)
	krdb.path = path

	gen, err := readGenerationFile(path)
	if err != nil {
		return nil, err
	}
	krdb.generation = gen

	krdb.datsDB = cabinet.New()
	err = krdb.datsDB.Open(filepath.Join(path, datsDBName), cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	krdb.crcDB = cabinet.New()
	err = krdb.crcDB.Open(filepath.Join(path, crcDBName), cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	krdb.md5DB = cabinet.New()
	err = krdb.md5DB.Open(filepath.Join(path, md5DBName), cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	krdb.sha1DB = cabinet.New()
	err = krdb.sha1DB.Open(filepath.Join(path, sha1DBName), cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	krdb.crcsha1DB = cabinet.New()
	err = krdb.crcsha1DB.Open(filepath.Join(path, crcsha1DBName), cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	krdb.md5sha1DB = cabinet.New()
	err = krdb.md5sha1DB.Open(filepath.Join(path, md5sha1DBName), cabinet.KCOWRITER|cabinet.KCOCREATE)
	if err != nil {
		return nil, err
	}
	return krdb, nil
}

func (krdb *KyotoRomDB) IndexRom(rom *types.Rom) error {
	dats, err := krdb.DatsForRom(rom)
	if err != nil {
		return err
	}

	if len(dats) > 0 {
		return nil
	}

	dat := new(types.Dat)
	dat.Artificial = true
	dat.Generation = krdb.generation
	dat.Name = fmt.Sprintf("Artifical Dat for %s", rom.Name)
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

	return krdb.IndexDat(dat, hh.Sum(nil))
}

func (krdb *KyotoRomDB) IndexDat(dat *types.Dat, sha1Bytes []byte) error {
	dat.Generation = krdb.generation

	var buf bytes.Buffer

	gobEncoder := gob.NewEncoder(&buf)
	err := gobEncoder.Encode(dat)
	if err != nil {
		return err
	}

	dBytes, err := krdb.datsDB.Get(sha1Bytes)
	if err != nil && krdb.datsDB.Ecode() != cabinet.KCENOREC {
		return err
	}

	err = krdb.datsDB.Set(sha1Bytes, buf.Bytes())
	if err != nil {
		return err
	}

	if dBytes == nil {
		for _, g := range dat.Games {
			for _, r := range g.Roms {
				if r.Sha1 != nil {
					err = krdb.sha1DB.Append(r.Sha1, sha1Bytes)
					if err != nil {
						return err
					}
				}

				if r.Md5 != nil {
					err = krdb.md5DB.Append(r.Md5, sha1Bytes)
					if err != nil {
						return err
					}
				}

				if r.Crc != nil {
					err = krdb.crcDB.Append(r.Crc, sha1Bytes)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (krdb *KyotoRomDB) OrphanDats() error {
	krdb.generation++
	err := writeGenerationFile(krdb.path, krdb.generation)
	if err != nil {
		return err
	}
	return nil
}

func (krdb *KyotoRomDB) GetDat(sha1Bytes []byte) (*types.Dat, error) {
	dBytes, err := krdb.datsDB.Get(sha1Bytes)
	if err != nil && krdb.datsDB.Ecode() != cabinet.KCENOREC {
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

func (krdb *KyotoRomDB) DatsForRom(rom *types.Rom) ([]*types.Dat, error) {
	var dBytes []byte
	var err error

	if rom.Sha1 != nil {
		dBytes, err = krdb.sha1DB.Get(rom.Sha1)
		if err != nil {
			return nil, err
		}
	} else if rom.Md5 != nil {
		dBytes, err = krdb.md5DB.Get(rom.Md5)
		if err != nil {
			return nil, err
		}
	} else if rom.Crc != nil {
		dBytes, err = krdb.crcDB.Get(rom.Crc)
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
		dat, err := krdb.GetDat(sha1Bytes)
		if err != nil {
			return nil, err
		}
		dats = append(dats, dat)
	}

	return dats, nil
}

func (krdb *KyotoRomDB) Close() error {
	err := krdb.datsDB.Close()
	if err != nil {
		return err
	}
	err = krdb.crcDB.Close()
	if err != nil {
		return err
	}
	err = krdb.md5DB.Close()
	if err != nil {
		return err
	}
	err = krdb.sha1DB.Close()
	if err != nil {
		return err
	}
	err = krdb.crcsha1DB.Close()
	if err != nil {
		return err
	}
	err = krdb.md5sha1DB.Close()
	if err != nil {
		return err
	}
	return nil
}

func writeGenerationFile(root string, size int64) error {
	file, err := os.Create(filepath.Join(root, generationFilename))
	if err != nil {
		return err
	}
	defer file.Close()

	bw := bufio.NewWriter(file)
	defer bw.Flush()

	bw.WriteString(strconv.FormatInt(size, 10))
	return nil
}

func readGenerationFile(root string) (int64, error) {
	file, err := os.Open(filepath.Join(root, generationFilename))
	if err != nil {
		if os.IsNotExist(err) {
			err = writeGenerationFile(root, 0)
			if err != nil {
				return 0, err
			}
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	bs, err := ioutil.ReadAll(file)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(bs), 10, 64)
}
