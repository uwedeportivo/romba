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
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/glog"

	"github.com/uwedeportivo/romba/types"
)

type gameWalker struct {
	game     *types.Game
	gamepath string
}

func (gw *gameWalker) visit(path string, f os.FileInfo, err error) error {
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

	romName, err := filepath.Rel(gw.gamepath, path)
	if err != nil {
		return err
	}

	rom := new(types.Rom)
	rom.Name = romName
	rom.Size = f.Size()
	rom.Crc = hh.Crc
	rom.Md5 = hh.Md5
	rom.Sha1 = hh.Sha1

	gw.game.Roms = append(gw.game.Roms, rom)

	return nil
}

func populateGame(srcpath string, gameDirInfo os.FileInfo) (*types.Game, error) {
	game := new(types.Game)
	baseName := gameDirInfo.Name()

	game.Name = baseName
	game.Description = baseName

	gw := &gameWalker{
		game:     game,
		gamepath: filepath.Join(srcpath, baseName),
	}

	err := filepath.Walk(gw.gamepath, gw.visit)
	if err != nil {
		return nil, err
	}

	return game, nil
}

func Dir2Dat(dat *types.Dat, srcpath, outpath string) error {
	glog.Infof("composing DAT from source %s into output dir %s", srcpath, outpath)

	fis, err := ioutil.ReadDir(srcpath)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if fi.IsDir() {
			game, err := populateGame(srcpath, fi)
			if err != nil {
				return err
			}

			dat.Games = append(dat.Games, game)
		}
	}

	outfilename := filepath.Join(outpath, dat.Name+".dat")
	outf, err := os.Create(outfilename)
	if err != nil {
		return err
	}
	defer outf.Close()

	outbuf := bufio.NewWriter(outf)
	defer outbuf.Flush()

	return types.ComposeDat(dat, outbuf)
}
