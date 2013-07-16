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
	"encoding/hex"
	"fmt"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

const datText = `
clrmamepro (
	name "Acorn Archimedes - Applications"
	description "Acorn Archimedes - Applications (TOSEC-v2008-10-11)"
	category "Acorn Archimedes - Applications"
	version 2008-10-11
	author "C0llector - Cassiel"
)

game (
	name "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS]"
	description "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS]"
	rom ( name "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS].adf" size 819200 crc e43166b9 md5 43ee6acc0c173048f47826307c0a262e )
)

game (
	name "Afterburner (1989)(Sega)(Side A)[cr NEC]"
	description "Afterburner (1989)(Sega)(Side A)[cr NEC]"
	rom ( name "Afterburner (1989)(Sega)(Side A)[cr NEC].g64" size 333744 crc 175a3f26 md5 36ecf1371d3391c06c16f751431c932b sha1 80353cb168dc5d7cc1dce57971f4ea2640a50ac4 )
)
`

func TestDB(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "rombadb")
	if err != nil {
		t.Fatalf("cannot create temp dir for test db: %v", err)
	}

	t.Logf("creating test db in %s\n", dbDir)

	krdb, err := NewKyotoDB(dbDir)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	dat, sha1Bytes, err := parser.ParseDat(strings.NewReader(datText), "testing/dat")
	if err != nil {
		t.Fatalf("failed to parse test dat: %v", err)
	}

	err = krdb.IndexDat(dat, sha1Bytes)
	if err != nil {
		t.Fatalf("failed to index test dat: %v", err)
	}

	datFromDb, err := krdb.GetDat(sha1Bytes)
	if err != nil {
		t.Fatalf("failed to retrieve test dat: %v", err)
	}

	if !datFromDb.Equals(dat) {
		fmt.Printf("datFromDb=%s\n", string(types.PrintDat(datFromDb)))
		fmt.Printf("dat=%s\n", string(types.PrintDat(dat)))
		t.Fatalf("dat differs from db dat")
	}

	romSha1Bytes, err := hex.DecodeString("80353cb168dc5d7cc1dce57971f4ea2640a50ac4")
	if err != nil {
		t.Fatalf("failed to hex decode: %v", err)
	}

	rom := new(types.Rom)
	rom.Sha1 = romSha1Bytes

	dats, err := krdb.DatsForRom(rom)
	if err != nil {
		t.Fatalf("failed to retrieve dats for rom: %v", err)
	}

	if len(dats) != 1 {
		t.Fatalf("couldn't find dats for rom")
	}

	datFromDb = dats[0]

	if !datFromDb.Equals(dat) {
		fmt.Printf("datFromDb=%s\n", string(types.PrintDat(datFromDb)))
		fmt.Printf("dat=%s\n", string(types.PrintDat(dat)))
		t.Fatalf("dat differs from db dat")
	}

	err = krdb.Close()
	if err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	err = os.RemoveAll(dbDir)
	if err != nil {
		t.Fatalf("failed to remove test db dir %s: %v", dbDir, err)
	}
}
