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
	"encoding/hex"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/golang/glog"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
)

type purgeWorker struct {
	depot *Depot
	index int
	pm    *purgeMaster
}

type purgeMaster struct {
	depot      *Depot
	numWorkers int
	pt         worker.ProgressTracker
	backupDir  string
}

type romsFromDatIterator struct {
	dat        *types.Dat
	gameCursor int
	romCursor  int
	depot      *Depot
}

func (rdi *romsFromDatIterator) inc() {
	rdi.romCursor = rdi.romCursor + 1
	for {
		if rdi.gameCursor == len(rdi.dat.Games) {
			return
		}
		g := rdi.dat.Games[rdi.gameCursor]
		if rdi.romCursor == len(g.Roms) {
			rdi.romCursor = 0
			rdi.gameCursor = rdi.gameCursor + 1
		} else {
			return
		}
	}
}

func (rdi *romsFromDatIterator) Next() (string, bool, error) {
	if rdi.gameCursor == len(rdi.dat.Games) {
		return "", false, nil
	}
	g := rdi.dat.Games[rdi.gameCursor]
	r := g.Roms[rdi.romCursor]
	rdi.inc()

	if r.Sha1 == nil {
		err := rdi.depot.romDB.CompleteRom(r)
		if err != nil {
			return "", false, err
		}
	}
	if r.Sha1 == nil {
		return "", true, nil
	}

	sha1Hex := hex.EncodeToString(r.Sha1)
	exists, rompath, err := rdi.depot.RomInDepot(sha1Hex)
	if err != nil {
		return "", false, err
	}

	if !exists {
		return "", true, nil
	}

	return rompath, true, nil
}

func (rdi *romsFromDatIterator) Reset() {
	rdi.gameCursor = 0
	rdi.romCursor = 0
}

func (depot *Depot) Purge(backupDir string, numWorkers int, workDepot string, fromDat string,
	pt worker.ProgressTracker) (string, error) {
	pm := new(purgeMaster)
	pm.depot = depot
	pm.pt = pt
	pm.numWorkers = numWorkers

	absBackupDir, err := filepath.Abs(backupDir)
	if err != nil {
		return "", err
	}

	pm.backupDir = absBackupDir

	if backupDir == "" {
		return "", errors.New("no backup dir specified")
	}

	err = os.MkdirAll(backupDir, 0777)
	if err != nil {
		return "", err
	}

	if fromDat == "" {
		wds := depot.roots
		if len(workDepot) > 0 {
			wds = []string{workDepot}
		}
		return worker.Work("purge roms", wds, pm)
	} else {
		dat, _, err := parser.Parse(fromDat)
		if err != nil {
			return "", err
		}
		rdi := &romsFromDatIterator{
			dat:   dat,
			depot: depot,
		}
		return worker.WorkPathIterator("purge roms", rdi, pm)
	}
}

func (pm *purgeMaster) Accept(path string) bool {
	return filepath.Ext(path) == gzipSuffix
}

func (pm *purgeMaster) CalculateWork() bool {
	return false
}

func (pm *purgeMaster) NewWorker(workerIndex int) worker.Worker {
	return &purgeWorker{
		depot: pm.depot,
		index: workerIndex,
		pm:    pm,
	}
}

func (pm *purgeMaster) NumWorkers() int {
	return pm.numWorkers
}

func (pm *purgeMaster) ProgressTracker() worker.ProgressTracker {
	return pm.pt
}

func (pm *purgeMaster) FinishUp() error {
	pm.depot.writeSizes()
	return nil
}

func (pm *purgeMaster) Start() error {
	return nil
}

func (pm *purgeMaster) Scanned(numFiles int, numBytes int64, commonRootPath string) {}

func (w *purgeWorker) Process(inpath string, size int64) error {
	rom, err := RomFromGZDepotFile(inpath)
	if err != nil {
		return err
	}

	_, hh, _, _, err := w.pm.depot.SHA1InDepot(hex.EncodeToString(rom.Sha1))
	if err != nil {
		return err
	}

	rom.Md5 = hh.Md5
	rom.Crc = hh.Crc

	dats, oldDats, err := w.pm.depot.romDB.FilteredDatsForRom(rom, func(dat *types.Dat) bool {
		return dat.Generation == w.pm.depot.romDB.Generation()
	})
	if err != nil {
		return err
	}

	if len(dats) == 0 {
		destPath := path.Join(w.pm.backupDir, "uncategorized", filepath.Base(inpath))

		if len(oldDats) > 0 {
			oldDat := oldDats[0]

			if oldDat != nil && oldDat.Path != "" {
				commonRoot := worker.CommonRoot(w.pm.backupDir, oldDat.Path)
				destPath = path.Join(w.pm.backupDir,
					strings.TrimSuffix(strings.TrimPrefix(oldDat.Path, commonRoot), filepath.Ext(oldDat.Path)),
					filepath.Base(inpath))
			}
		}
		glog.V(2).Infof("purging %s, moving to %s", inpath, destPath)
		err = worker.Mv(inpath, destPath)
		if err != nil {
			return err
		}
		index := -1
		for i, depotRoot := range w.pm.depot.roots {
			if strings.HasPrefix(inpath, depotRoot) {
				index = i
				break
			}
		}

		if index != -1 {
			w.pm.depot.adjustSize(index, -size)
		}
	}
	return nil
}

func (w *purgeWorker) Close() error {
	return nil
}
