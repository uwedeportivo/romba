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

package archive

import (
	"bufio"
	"os"
	"path/filepath"

	"github.com/golang/glog"

	"github.com/uwedeportivo/romba/types"
)

type romWalker struct {
	dat        *types.Dat
	sourcePath string
}

func (rw *romWalker) visit(path string, f os.FileInfo, err error) error {
	if f == nil || f.Name() == ".DS_Store" {
		return nil
	}
	if f.IsDir() {
		return nil
	}

	hh, err := HashesForFile(path)
	if err != nil {
		return err
	}

	romName, err := filepath.Rel(rw.sourcePath, path)
	if err != nil {
		return err
	}

	rom := new(types.Rom)
	rom.Name = romName
	rom.Size = f.Size()
	rom.Crc = hh.Crc
	rom.Md5 = hh.Md5
	rom.Sha1 = hh.Sha1

	game := new(types.Game)
	game.Name = romName

	game.Roms = append(game.Roms, rom)

	rw.dat.Games = append(rw.dat.Games, game)
	return nil
}

func Dir2Dat(dat *types.Dat, srcpath, outpath string) error {
	glog.Infof("composing DAT from source %s into output %s", srcpath, outpath)

	rw := &romWalker{
		dat:        dat,
		sourcePath: srcpath,
	}

	err := filepath.Walk(srcpath, rw.visit)
	if err != nil {
		return err
	}

	outf, err := os.Create(outpath)
	if err != nil {
		return err
	}
	defer outf.Close()

	outbuf := bufio.NewWriter(outf)
	defer outbuf.Flush()

	return types.ComposeCompliantDat(dat, outbuf)
}
