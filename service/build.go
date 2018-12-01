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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/uwedeportivo/commander"
	"github.com/uwedeportivo/romba/archive"
	"github.com/uwedeportivo/romba/dedup"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
)

type buildWorker struct {
	pm *buildMaster
}

func (pw *buildWorker) Process(path string, size int64) error {
	hashes, err := archive.HashesForFile(path)
	if err != nil {
		return err
	}

	dat, err := pw.pm.rs.romDB.GetDat(hashes.Sha1)
	if err != nil {
		return err
	}

	if dat == nil {
		glog.Warningf("did not find a DAT for %s, parsing it", path)
		dat, _, err = parser.Parse(path)
		if err != nil {
			return err
		}

		glog.V(4).Infof("parsed dat=%s", types.PrintShortDat(dat))
	}

	reldatdir, err := filepath.Rel(pw.pm.commonRootPath, filepath.Dir(path))
	if err != nil {
		return err
	}

	datdir := filepath.Join(pw.pm.outpath, reldatdir)

	glog.Infof("buildWorker processing %s, reldatdir=%s, datdir=%s", path, reldatdir, datdir)

	err = os.MkdirAll(datdir, 0777)
	if err != nil {
		return err
	}

	for _, game := range dat.Games {
		for _, rom := range game.Roms {
			err = pw.pm.rs.romDB.CompleteRom(rom)
			if err != nil {
				return err
			}
		}
	}

	datInComplete := false
	if pw.pm.fixdatOnly {
		datInComplete, err = pw.pm.rs.depot.FixDat(dat, datdir, pw.pm.numSubWorkers, pw.pm.deduper)
	} else {
		datInComplete, err = pw.pm.rs.depot.BuildDat(dat, datdir, pw.pm.numSubWorkers, pw.pm.deduper)
	}

	if err != nil {
		return err
	}

	glog.Infof("finished building dat %s in directory %s", dat.Name, datdir)
	if datInComplete {
		glog.Info("dat has missing roms")
	}
	return nil
}

func (pw *buildWorker) Close() error {
	return nil
}

type buildMaster struct {
	rs             *RombaService
	numWorkers     int
	numSubWorkers  int
	pt             worker.ProgressTracker
	commonRootPath string
	outpath        string
	fixdatOnly     bool
	deduper        dedup.Deduper
}

func (pm *buildMaster) CalculateWork() bool {
	return true
}

func (pm *buildMaster) Accept(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".dat" || ext == ".xml"
}

func (pm *buildMaster) NewWorker(workerIndex int) worker.Worker {
	return &buildWorker{
		pm: pm,
	}
}

func (pm *buildMaster) NumWorkers() int {
	return pm.numWorkers
}

func (pm *buildMaster) ProgressTracker() worker.ProgressTracker {
	return pm.pt
}

func (pm *buildMaster) FinishUp() error {
	return pm.deduper.Close()
}

func (pm *buildMaster) Start() error {
	return nil
}

func (pm *buildMaster) Scanned(numFiles int, numBytes int64, commonRootPath string) {
	glog.Infof("buildMaster common root path: %s", commonRootPath)
	pm.commonRootPath = commonRootPath
	fi, err := os.Stat(pm.commonRootPath)
	if err != nil {
		pm.commonRootPath = "/"
		return
	}
	if !fi.IsDir() {
		pm.commonRootPath = filepath.Dir(pm.commonRootPath)
	}
}

func (rs *RombaService) build(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		_, err := fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.IBytes(uint64(p.BytesSoFar)), humanize.IBytes(uint64(p.TotalBytes)))
		return err
	}

	outpath := cmd.Flag.Lookup("out").Value.Get().(string)
	if outpath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-out flag is required")
		return err
	}

	fixdatOnly := cmd.Flag.Lookup("fixdatOnly").Value.Get().(bool)

	numWorkers := cmd.Flag.Lookup("workers").Value.Get().(int)
	numSubWorkers := cmd.Flag.Lookup("subworkers").Value.Get().(int)

	if !filepath.IsAbs(outpath) {
		absoutpath, err := filepath.Abs(outpath)
		if err != nil {
			return err
		}
		outpath = absoutpath
	}

	if err := os.MkdirAll(outpath, 0777); err != nil {
		return err
	}

	deduper, err := dedup.NewLevelDBDeduper()
	if err != nil {
		return err
	}

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "build"

	go func() {
		glog.Infof("service starting build")
		rs.broadCastProgress(time.Now(), true, false, "", nil)
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

		pm := &buildMaster{
			outpath:       outpath,
			rs:            rs,
			numWorkers:    numWorkers,
			numSubWorkers: numSubWorkers,
			pt:            rs.pt,
			fixdatOnly:    fixdatOnly,
			deduper:       deduper,
		}

		endMsg, err := worker.Work("building dats", args, pm)
		if err != nil {
			glog.Errorf("error building dats: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		derr := archive.DeleteEmptyFolders(outpath)
		if derr != nil {
			glog.Errorf("error building dats: %v", derr)
		}

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		rs.broadCastProgress(time.Now(), false, true, endMsg, err)
		glog.Infof("service finished build")
	}()

	_, err = fmt.Fprintf(cmd.Stdout, "started build")
	return err
}

func (rs *RombaService) dir2dat(cmd *commander.Command, args []string) error {
	outpath := cmd.Flag.Lookup("out").Value.Get().(string)

	srcpath := cmd.Flag.Lookup("source").Value.Get().(string)
	srcInfo, err := os.Stat(srcpath)
	if err != nil {
		return err
	}

	if !srcInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", srcpath)
	}

	dat := new(types.Dat)
	dat.Name = cmd.Flag.Lookup("name").Value.Get().(string)
	dat.Description = cmd.Flag.Lookup("description").Value.Get().(string)

	err = archive.Dir2Dat(dat, srcpath, outpath)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(cmd.Stdout, "dir2dat successfully completed a DAT in %s for directory %s", outpath, srcpath)
	return err
}
