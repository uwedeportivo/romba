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
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/glog"
	"github.com/uwedeportivo/romba/dedup"
	"github.com/uwedeportivo/romba/types"
)

type fixdatBuilder struct {
	depot     *Depot
	datPath   string
	fixDat    *types.Dat
	mutex     *sync.Mutex
	wc        chan *types.Game
	erc       chan error
	closeC    chan bool
	index     int
	deduper   dedup.Deduper
	bloomOnly bool
}

func (gb *fixdatBuilder) work() {
	glog.V(4).Infof("starting subworker %d", gb.index)
	for game := range gb.wc {
		gamePath := filepath.Join(gb.datPath, game.Name)
		fixGame, err := gb.depot.fixdatGame(game, gamePath, gb.fixDat.UnzipGames, gb.deduper, gb.bloomOnly)
		if err != nil {
			glog.Errorf("error processing %s: %v", gamePath, err)
			gb.erc <- err
			break
		}
		if fixGame != nil {
			gb.mutex.Lock()
			gb.fixDat.Games = append(gb.fixDat.Games, fixGame)
			gb.mutex.Unlock()
		}
	}
	gb.closeC <- true
	glog.V(4).Infof("exiting subworker %d", gb.index)
	return
}

func (depot *Depot) FixDat(dat *types.Dat, outpath string,
	numSubworkers int, deduper dedup.Deduper, bloomOnly bool) (bool, error) {
	datPath := filepath.Join(outpath, dat.Name)

	fixDat := new(types.Dat)
	fixDat.FixDat = true
	fixDat.Name = "fix_" + dat.Name
	fixDat.Description = dat.Description
	fixDat.Path = dat.Path
	fixDat.UnzipGames = dat.UnzipGames

	wc := make(chan *types.Game)
	erc := make(chan error)
	closeC := make(chan bool)
	mutex := new(sync.Mutex)

	for i := 0; i < numSubworkers; i++ {
		gb := new(fixdatBuilder)
		gb.depot = depot
		gb.wc = wc
		gb.erc = erc
		gb.mutex = mutex
		gb.datPath = datPath
		gb.fixDat = fixDat
		gb.index = i
		gb.deduper = deduper
		gb.closeC = closeC
		gb.bloomOnly = bloomOnly
		go gb.work()
	}

	var minionErr error

endLoop:
	for _, game := range dat.Games {
		select {
		case wc <- game:
		case err := <-erc:
			minionErr = err
			break endLoop
		}
	}
	close(wc)

	finishedSubworkers := 0

endLoop2:
	for {
		glog.V(4).Infof("builder master: finished so far %d", finishedSubworkers)

		select {
		case <-closeC:
			glog.V(4).Infof("builder master: finished another subworker")
			finishedSubworkers++
			if finishedSubworkers == numSubworkers {
				break endLoop2
			}
		case err := <-erc:
			glog.V(4).Infof("builder master: minion error")
			minionErr = err
		}
	}

	if minionErr != nil {
		return false, minionErr
	}

	if len(fixDat.Games) > 0 {
		fixDatPath := filepath.Join(outpath, fixPrefix+dat.Filename()+datSuffix)

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

	return len(fixDat.Games) > 0, nil
}

func (depot *Depot) fixdatGame(game *types.Game, gamePath string,
	unzipGame bool, deduper dedup.Deduper, bloomOnly bool) (*types.Game, error) {

	var fixGame *types.Game

	for _, rom := range game.Roms {
		croms, err := depot.RomDB.CompleteRom(rom)
		if err != nil {
			glog.Errorf("error completing rom %s: %v", rom.Name, err)
			return nil, err
		}

		if len(croms) > 0 {
			game.Roms = append(game.Roms, croms...)
		}

		if rom.Sha1 == nil {
			if rom.Size > 0 {
				if fixGame == nil {
					fixGame = new(types.Game)
					fixGame.Name = game.Name
					fixGame.Description = game.Description
				}

				fixGame.Roms = append(fixGame.Roms, rom)
			}
			continue
		}

		sha1Hex := hex.EncodeToString(rom.Sha1)
		exists, _, err := depot.RomInDepotBloom(sha1Hex, bloomOnly)
		if err != nil {
			glog.Errorf("error checking rom %s in depot: %v", rom.Name, err)
			return nil, err
		}

		if !exists {
			if glog.V(2) {
				glog.Warningf("game %s has missing rom %s (sha1 %s)", game.Name, rom.Name, hex.EncodeToString(rom.Sha1))
			}

			seenRom, err := deduper.Seen(rom)
			if err != nil {
				return nil, err
			}

			if !seenRom {
				err = deduper.Declare(rom)
				if err != nil {
					glog.Errorf("error deduping rom %s: %v", rom.Name, err)
					return nil, err
				}

				if fixGame == nil {
					fixGame = new(types.Game)
					fixGame.Name = game.Name
					fixGame.Description = game.Description
				}

				fixGame.Roms = append(fixGame.Roms, rom)
			}
			continue
		}
	}
	return fixGame, nil
}
