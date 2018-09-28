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
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/uwedeportivo/romba/combine"
	"github.com/uwedeportivo/romba/config"
	"github.com/uwedeportivo/romba/types"
	"io/ioutil"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"

	"github.com/uwedeportivo/commander"
)


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
		fmt.Fprintf(cmd.Stdout, "-out argument required")
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
	defer combiner.Close()

	glog.V(4).Infof("leveldb combiner at %s", tempPath)

	pgc := &progressCombiner{
		rs: rs,
		cbr:combiner,
	}

	exportGame := new(types.Game)
	exportGame.Name = "wrapper"
	exportGame.Description = "exported roms"

	exportDat := new(types.Dat)
	exportDat.Name = "romba_export"
	exportDat.Description = "joins md5, crc, sha1 for each rom"
	exportDat.Path = outPath
	exportDat.Games = []*types.Game{exportGame}

	err = rs.depot.RomDB.JoinCrcMd5(pgc)
	if err != nil {
		return err
	}

	err = pgc.ForEachRom(func(rom *types.Rom) error {
		if rom.Crc != nil && rom.Md5 != nil {
			exportGame.Roms = append(exportGame.Roms, rom)
		}
		rs.pt.AddBytesFromFile(int64(sha1.Size), false)
		return nil
	})
	if err != nil {
		return err
	}

	var endMsg string

	err = writeDat(exportDat, outPath)
	if err != nil {
		return err
	}

	endMsg = fmt.Sprintf("export finished, %d roms written to exportdat file %s",
		len(exportDat.Games[0].Roms), outPath)

	glog.Infof(endMsg)
	fmt.Fprintf(cmd.Stdout, endMsg)
	rs.broadCastProgress(time.Now(), false, true, endMsg)

	return nil
}

func (rs *RombaService) export(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.IBytes(uint64(p.BytesSoFar)), humanize.IBytes(uint64(p.TotalBytes)))
		return nil
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
					rs.broadCastProgress(t, false, false, "")
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
		rs.broadCastProgress(time.Now(), false, true, "export finished")
	}()

	glog.Infof("service starting export")
	fmt.Fprintf(cmd.Stdout, "started export")

	return nil
}
