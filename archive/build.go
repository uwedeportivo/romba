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
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/glog"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/torrentzip"
	"github.com/uwedeportivo/torrentzip/cgzip"
)

func (depot *Depot) BuildDat(dat *types.Dat, outpath string) (bool, error) {
	datPath := filepath.Join(outpath, dat.Name)

	err := os.Mkdir(datPath, 0777)
	if err != nil {
		return false, err
	}

	var fixDat *types.Dat

	for _, game := range dat.Games {
		gamePath := filepath.Join(datPath, game.Name+zipSuffix)
		fixGame, foundRom, err := depot.buildGame(game, gamePath)
		if err != nil {
			return false, err
		}
		if fixGame != nil {
			if fixDat == nil {
				fixDat = new(types.Dat)
				fixDat.Name = dat.Name
				fixDat.Description = dat.Description
				fixDat.Path = dat.Path
			}
			fixDat.Games = append(fixDat.Games, fixGame)
		}
		if !foundRom {
			err = os.Remove(gamePath)
			if err != nil {
				return false, err
			}
		}
	}

	if fixDat != nil {
		fixDatPath := filepath.Join(outpath, fixPrefix+dat.Name+datSuffix)

		fixFile, err := os.Create(fixDatPath)
		if err != nil {
			return false, err
		}
		defer fixFile.Close()

		fixWriter := bufio.NewWriter(fixFile)
		defer fixWriter.Flush()

		err = types.ComposeCompliantDat(fixDat, fixWriter)
		if err != nil {
			return false, err
		}
	}

	return fixDat == nil, nil
}

func (depot *Depot) buildGame(game *types.Game, gamePath string) (*types.Game, bool, error) {
	gameFile, err := os.Create(gamePath)
	if err != nil {
		return nil, false, err
	}
	defer gameFile.Close()

	gameTorrent, err := torrentzip.NewWriter(gameFile)
	if err != nil {
		return nil, false, err
	}
	defer gameTorrent.Close()

	var fixGame *types.Game

	foundRom := false

	for _, rom := range game.Roms {
		if rom.Sha1 == nil {
			if glog.V(2) {
				glog.Warningf("game %s has rom with missing SHA1 %s", game.Name, rom.Name)
			}
			if fixGame == nil {
				fixGame = new(types.Game)
				fixGame.Name = game.Name
				fixGame.Description = game.Description
			}

			fixGame.Roms = append(fixGame.Roms, rom)
			continue
		}

		romGZ, err := depot.OpenRomGZ(rom)
		if err != nil {
			return nil, false, err
		}

		if romGZ == nil {
			if glog.V(2) {
				glog.Warningf("game %s has missing rom %s (sha1 %s)", game.Name, rom.Name, hex.EncodeToString(rom.Sha1))
			}
			if fixGame == nil {
				fixGame = new(types.Game)
				fixGame.Name = game.Name
				fixGame.Description = game.Description
			}

			fixGame.Roms = append(fixGame.Roms, rom)
			continue
		}

		foundRom = true
		src, err := cgzip.NewReader(romGZ)
		if err != nil {
			return nil, false, err
		}

		dst, err := gameTorrent.Create(rom.Name)
		if err != nil {
			return nil, false, err
		}

		_, err = io.Copy(dst, src)
		if err != nil {
			return nil, false, err
		}

		src.Close()
		romGZ.Close()
	}
	return fixGame, foundRom, nil
}
