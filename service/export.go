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

package service

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/uwedeportivo/romba/combine"
	"github.com/uwedeportivo/romba/config"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
	"io/ioutil"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"

	"github.com/uwedeportivo/commander"
)

const MB = 1000000

type progressCombiner struct {
	rs *RombaService
	cbr combine.Combiner
}

func (pgc *progressCombiner) Declare(rom *types.Rom) error {
	err := pgc.cbr.Declare(rom)
	pgc.rs.pt.AddBytesFromFile(int64(sha1.Size), err != nil)
	return err
}

func (pgc *progressCombiner) ForEachRom(romF func(rom *types.Rom) error) error {
	return pgc.cbr.ForEachRom(romF)
}

func (pgc *progressCombiner) Close() error {
	return pgc.cbr.Close()
}

func (rs *RombaService) exportWork(cmd *commander.Command, args []string) error {
	outPath := cmd.Flag.Lookup("out").Value.Get().(string)

	if outPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-out argument required")
		if err != nil {
			return err
		}
		return errors.New("missing out argument")
	}

	glog.Infof("export hashes into %s", outPath)

	tempPath, err := ioutil.TempDir(config.GlobalConfig.General.TmpDir, "romba_combine")
	if err != nil {
		return err
	}

	combiner, err := combine.NewLevelDBCombiner(tempPath)
	if err != nil {
		return err
	}
	defer func(){
		err := combiner.Close()
		if err != nil {
			glog.Errorf("error closing combiner leveldb: %v", err)
		}
	}()

	glog.V(4).Infof("leveldb combiner at %s", tempPath)

	pgc := &progressCombiner{
		rs: rs,
		cbr:combiner,
	}

	exportDat := new(types.Dat)
	exportDat.Name = "romba_export"
	exportDat.Description = "joins md5, crc, sha1 for each rom"
	exportDat.Path = outPath

	err = rs.depot.RomDB.JoinCrcMd5(pgc)
	if err != nil {
		return err
	}

	file, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer func(){
		err := file.Close()
		if err != nil {
			glog.Errorf("error, failed to close %s: %v", outPath, err)
		}
	}()

	writer := bufio.NewWriter(file)
	defer func(){
		err := writer.Flush()
		if err != nil {
			glog.Errorf("error, failed to flush %s: %v", outPath, err)
		}
	}()

	err = types.ComposeCompliantDat(exportDat, writer)
	if err != nil {
		return err
	}

	_, err = writer.WriteString("\n")
	if err != nil {
		return err
	}

	exportGame := new(types.Game)
	exportGame.Roms = make([]*types.Rom, 1)

	numRoms := 0

	err = pgc.ForEachRom(func(rom *types.Rom) error {
		if rom.Crc != nil && rom.Md5 != nil {
			exportGame.Roms[0] = rom
			exportGame.Name = rom.Name
			exportGame.Description = rom.Name

			err = types.ComposeGame(exportGame, writer)
			if err != nil {
				return err
			}
			numRoms++
		}
		rs.pt.AddBytesFromFile(int64(sha1.Size), false)
		return nil
	})
	if err != nil {
		return err
	}

	var endMsg string

	endMsg = fmt.Sprintf("export finished, %d roms written to exportdat file %s",
		numRoms, outPath)

	glog.Infof(endMsg)
	_, err = fmt.Fprintf(cmd.Stdout, endMsg)
	if err != nil {
		return err
	}
	rs.broadCastProgress(time.Now(), false, true, endMsg, nil)

	return nil
}

func (rs *RombaService) export(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		_, err := fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.IBytes(uint64(p.BytesSoFar)), humanize.IBytes(uint64(p.TotalBytes)))
		return err
	}

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "export"

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		stopTicker := make(chan bool)
		go func() {
			glog.Infof("starting progress broadcaster")
			for {
				select {
				case t := <-ticker.C:
					rs.broadCastProgress(t, false, false, "", nil)
				case <-stopTicker:
					glog.Info("stopped progress broadcaster")
					return
				}
			}
		}()

		err := rs.exportWork(cmd, args)
		if err != nil {
			glog.Errorf("error export: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		glog.Infof("export finished")
		rs.pt.Finished()
		rs.broadCastProgress(time.Now(), false, true, "export finished", err)
	}()

	glog.Infof("service starting export")
	_, err := fmt.Fprintf(cmd.Stdout, "started export")
	return err
}


func (rs *RombaService) imprt(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		_, err := fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.IBytes(uint64(p.BytesSoFar)), humanize.IBytes(uint64(p.TotalBytes)))
		return err
	}

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "import"

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		stopTicker := make(chan bool)
		go func() {
			glog.Infof("starting progress broadcaster")
			for {
				select {
				case t := <-ticker.C:
					rs.broadCastProgress(t, false, false, "", nil)
				case <-stopTicker:
					glog.Info("stopped progress broadcaster")
					return
				}
			}
		}()

		err := rs.importWork(cmd, args)
		if err != nil {
			glog.Errorf("error import: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		glog.Infof("import finished")
		rs.pt.Finished()
		rs.broadCastProgress(time.Now(), false, true, "import finished", err)
	}()

	glog.Infof("service starting import")
	_, err := fmt.Fprintf(cmd.Stdout, "started import")
	return err
}

type imprtParseListener struct {
	numRoms int
	rs *RombaService
	activeBatch db.RomBatch
}

func (ipl *imprtParseListener) ParsedDatStmt(dat *types.Dat) error {
	return nil
}

func (ipl *imprtParseListener) ParsedGameStmt(game *types.Game) error {
	ipl.numRoms += len(game.Roms)

	for _, r := range game.Roms {
		err := ipl.activeBatch.IndexRom(r)
		if err != nil {
			return err
		}
	}

	if ipl.activeBatch.Size() > 10 * MB {
		err := ipl.activeBatch.Close()
		if err != nil {
			return err
		}

		ipl.activeBatch = ipl.rs.depot.RomDB.StartBatch()
	}
	return nil
}

func (rs *RombaService) importWork(cmd *commander.Command, args []string) error {
	inPath := cmd.Flag.Lookup("in").Value.Get().(string)

	if inPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-in argument required")
		if err != nil {
			return err
		}
		return errors.New("missing in argument")
	}

	glog.Infof("import hashes from %s", inPath)

	ipl := &imprtParseListener{
		rs: rs,
		activeBatch: rs.depot.RomDB.StartBatch(),
	}

	_, err := parser.ParseWithListener(inPath, ipl)
	if err != nil {
		return err
	}

	err = ipl.activeBatch.Close()
	if err != nil {
		return err
	}

	var endMsg string

	endMsg = fmt.Sprintf("import finished, %d roms imported from file %s",
		ipl.numRoms, inPath)

	glog.Infof(endMsg)
	_, err = fmt.Fprintf(cmd.Stdout, endMsg)
	if err != nil {
		return err
	}
	rs.broadCastProgress(time.Now(), false, true, endMsg, nil)

	return nil
}
