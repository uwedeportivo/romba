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
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/websocket"
	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/uwedeportivo/commander"

	"github.com/uwedeportivo/romba/archive"
	"github.com/uwedeportivo/romba/config"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
)

type ProgressNessage struct {
	TotalFiles      int32
	TotalBytes      int64
	BytesSoFar      int64
	FilesSoFar      int32
	Running         bool
	JobName         string
	Starting        bool
	Stopping        bool
	TerminalMessage string
}

type RombaService struct {
	romDB             db.RomDB
	depot             *archive.Depot
	logDir            string
	dats              string
	numWorkers        int
	pt                worker.ProgressTracker
	busy              bool
	jobMutex          *sync.Mutex
	jobName           string
	progressMutex     *sync.Mutex
	progressListeners map[string]chan *ProgressNessage
}

type TerminalRequest struct {
	CmdTxt string
}

type TerminalReply struct {
	Message string
}

func NewRombaService(romDB db.RomDB, depot *archive.Depot, cfg *config.Config) *RombaService {
	glog.Info("Service init")
	rs := new(RombaService)
	rs.romDB = romDB
	rs.depot = depot
	rs.dats = cfg.Index.Dats
	rs.logDir = cfg.General.LogDir
	rs.numWorkers = cfg.General.Workers
	rs.pt = worker.NewProgressTracker()
	rs.jobMutex = new(sync.Mutex)
	rs.progressMutex = new(sync.Mutex)
	rs.progressListeners = make(map[string]chan *ProgressNessage)
	glog.Info("Service init finished")
	return rs
}

func (rs *RombaService) registerProgressListener(s string, c chan *ProgressNessage) {
	rs.progressMutex.Lock()
	defer rs.progressMutex.Unlock()

	rs.progressListeners[s] = c
}

func (rs *RombaService) unregisterProgressListener(s string) {
	rs.progressMutex.Lock()
	defer rs.progressMutex.Unlock()

	delete(rs.progressListeners, s)
}

func (rs *RombaService) broadCastProgress(t time.Time, starting bool, stopping bool, terminalMessage string) {
	var p *worker.Progress
	var jn string

	rs.progressMutex.Lock()
	if rs.busy {
		p = rs.pt.GetProgress()
		jn = rs.jobName
	}
	rs.progressMutex.Unlock()

	pmsg := new(ProgressNessage)

	pmsg.Starting = starting
	pmsg.Stopping = stopping
	pmsg.TerminalMessage = terminalMessage

	if p != nil {
		pmsg.TotalFiles = p.TotalFiles
		pmsg.TotalBytes = p.TotalBytes
		pmsg.BytesSoFar = p.BytesSoFar
		pmsg.FilesSoFar = p.FilesSoFar
		pmsg.JobName = jn
		pmsg.Running = true
	} else {
		pmsg.Running = false
	}

	rs.progressMutex.Lock()
	defer rs.progressMutex.Unlock()

	for _, c := range rs.progressListeners {
		c <- pmsg
	}
}

func (rs *RombaService) Execute(r *http.Request, req *TerminalRequest, reply *TerminalReply) error {
	outbuf := new(bytes.Buffer)

	cmd := newCommand(outbuf, rs)

	cmdTxtSplit, err := splitIntoArgs(req.CmdTxt)
	if err != nil {
		reply.Message = fmt.Sprintf("error: splitting command failed: %v\n", err)
		return nil
	}

	err = cmd.Flag.Parse(cmdTxtSplit)
	if err != nil {
		reply.Message = fmt.Sprintf("error: parsing command failed: %v\n", err)
		return nil
	}

	args := cmd.Flag.Args()
	err = cmd.Dispatch(args)
	if err != nil {
		reply.Message = fmt.Sprintf("error: executing command failed: %v\n", err)
		glog.Errorf("error executing command %s: %v", req.CmdTxt, err)
		return nil
	}

	reply.Message = outbuf.String()
	return nil
}

func runCmd(cmd *commander.Command, args []string) error {
	fmt.Fprintf(cmd.Stdout, "command %s with args %s\n", cmd.Name, strings.Join(args, " "))
	return nil
}

func (rs *RombaService) startRefreshDats(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.Bytes(uint64(p.BytesSoFar)), humanize.Bytes(uint64(p.TotalBytes)))
		return nil
	}

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "refresh-dats"

	go func() {
		glog.Infof("service starting refresh-dats")
		rs.broadCastProgress(time.Now(), true, false, "")
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

		endMsg, err := db.Refresh(rs.romDB, rs.dats, rs.numWorkers, rs.pt)
		if err != nil {
			glog.Errorf("error refreshing dats: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		rs.broadCastProgress(time.Now(), false, true, endMsg)
		glog.Infof("service finished refresh-dats")
	}()

	fmt.Fprintf(cmd.Stdout, "started refresh dats")
	return nil
}

func (rs *RombaService) lookup(cmd *commander.Command, args []string) error {
	for _, arg := range args {
		hash, err := hex.DecodeString(arg)
		if err != nil {
			return err
		}

		if len(hash) == sha1.Size {
			dat, err := rs.romDB.GetDat(hash)
			if err != nil {
				return err
			}

			if dat != nil {
				fmt.Fprintf(cmd.Stdout, "dat with sha1 %s = %s\n", arg, types.PrintShortDat(dat))
			}
		}

		r := new(types.Rom)
		switch len(hash) {
		case md5.Size:
			r.Md5 = hash
		case crc32.Size:
			r.Crc = hash
		case sha1.Size:
			r.Sha1 = hash
		default:
			return fmt.Errorf("found unknown hash size: %d", len(hash))
		}

		dats, err := rs.romDB.DatsForRom(r)
		if err != nil {
			return err
		}

		err = rs.romDB.CompleteRom(r)
		if err != nil {
			return err
		}

		if len(dats) > 0 {
			fmt.Fprintf(cmd.Stdout, "rom in %s\n", types.PrintRomInDats(dats))
		}
	}

	return nil
}

func (rs *RombaService) progress(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		fmt.Fprintf(cmd.Stdout, "running %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.Bytes(uint64(p.BytesSoFar)), humanize.Bytes(uint64(p.TotalBytes)))
		return nil
	} else {
		fmt.Fprintf(cmd.Stdout, "nothing currently running")
	}
	return nil
}

func (rs *RombaService) ShutDown() error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		wc := make(chan bool)
		rs.pt.Stop(wc)
		<-wc
	}

	return rs.romDB.Close()
}

func (rs *RombaService) shutdown(cmd *commander.Command, args []string) error {
	fmt.Printf("shutting down now\n")

	err := rs.ShutDown()
	if err != nil {
		glog.Errorf("error shutting down: %v", err)
	}

	os.Exit(0)
	return nil
}

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
		// TODO(uwe): maybe parse it and add it to the DB
		glog.Warningf("did not find a DAT for %s, maybe a refresh is needed", path)
		return nil
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

	datComplete, err := pw.pm.rs.depot.BuildDat(dat, datdir)
	if err != nil {
		return err
	}

	glog.Infof("finished building dat %s in directory %s\n", dat.Name, datdir)
	if !datComplete {
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
	pt             worker.ProgressTracker
	commonRootPath string
	outpath        string
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
	return nil
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

		fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.Bytes(uint64(p.BytesSoFar)), humanize.Bytes(uint64(p.TotalBytes)))
		return nil
	}

	outpath := cmd.Flag.Lookup("out").Value.Get().(string)
	if outpath == "" {
		fmt.Fprintf(cmd.Stdout, "-out flag is required")
		return nil
	}

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

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "build"

	go func() {
		glog.Infof("service starting build")
		rs.broadCastProgress(time.Now(), true, false, "")
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

		pm := &buildMaster{
			outpath:    outpath,
			rs:         rs,
			numWorkers: rs.numWorkers,
			pt:         rs.pt,
		}

		endMsg, err := worker.Work("building dats", args, pm)
		if err != nil {
			glog.Errorf("error building dats: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		rs.broadCastProgress(time.Now(), false, true, endMsg)
		glog.Infof("service finished build")
	}()

	fmt.Fprintf(cmd.Stdout, "started build")
	return nil
}

func (rs *RombaService) memstats(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	debug.FreeOSMemory()

	s := new(runtime.MemStats)
	runtime.ReadMemStats(s)

	fmt.Fprintf(cmd.Stdout, "\n# runtime.MemStats\n")
	fmt.Fprintf(cmd.Stdout, "# Alloc = %s\n", humanize.Bytes(s.Alloc))
	fmt.Fprintf(cmd.Stdout, "# TotalAlloc = %s\n", humanize.Bytes(s.TotalAlloc))
	fmt.Fprintf(cmd.Stdout, "# Sys = %s\n", humanize.Bytes(s.Sys))
	fmt.Fprintf(cmd.Stdout, "# Lookups = %d\n", s.Lookups)
	fmt.Fprintf(cmd.Stdout, "# Mallocs = %d\n", s.Mallocs)
	fmt.Fprintf(cmd.Stdout, "# Frees = %d\n", s.Frees)

	fmt.Fprintf(cmd.Stdout, "# HeapAlloc = %s\n", humanize.Bytes(s.HeapAlloc))
	fmt.Fprintf(cmd.Stdout, "# HeapSys = %s\n", humanize.Bytes(s.HeapSys))
	fmt.Fprintf(cmd.Stdout, "# HeapIdle = %s\n", humanize.Bytes(s.HeapIdle))
	fmt.Fprintf(cmd.Stdout, "# HeapInuse = %s\n", humanize.Bytes(s.HeapInuse))
	fmt.Fprintf(cmd.Stdout, "# HeapReleased = %s\n", humanize.Bytes(s.HeapReleased))
	fmt.Fprintf(cmd.Stdout, "# HeapObjects = %d\n", s.HeapObjects)

	fmt.Fprintf(cmd.Stdout, "# Stack = %d / %d\n", s.StackInuse, s.StackSys)
	fmt.Fprintf(cmd.Stdout, "# MSpan = %d / %d\n", s.MSpanInuse, s.MSpanSys)
	fmt.Fprintf(cmd.Stdout, "# MCache = %d / %d\n", s.MCacheInuse, s.MCacheSys)
	fmt.Fprintf(cmd.Stdout, "# BuckHashSys = %d\n", s.BuckHashSys)

	fmt.Fprintf(cmd.Stdout, "# NextGC = %d\n", s.NextGC)
	fmt.Fprintf(cmd.Stdout, "# PauseNs = %d\n", s.PauseNs)
	fmt.Fprintf(cmd.Stdout, "# NumGC = %d\n", s.NumGC)
	fmt.Fprintf(cmd.Stdout, "# EnableGC = %v\n", s.EnableGC)
	fmt.Fprintf(cmd.Stdout, "# DebugGC = %v\n", s.DebugGC)

	return nil
}

func (rs *RombaService) dbstats(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	fmt.Fprintf(cmd.Stdout, "dbstats = %s", rs.romDB.PrintStats())
	return nil
}

func (rs *RombaService) cancel(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		fmt.Fprintf(cmd.Stdout, "cancelling %s \n", rs.jobName)
		rs.pt.Stop(nil)
		return nil
	}

	fmt.Fprintf(cmd.Stdout, "nothing running worth cancelling")
	return nil
}

func (rs *RombaService) SendProgress(ws *websocket.Conn) {
	b := make([]byte, 10)
	n, err := io.ReadFull(rand.Reader, b)

	if n != len(b) || err != nil {
		glog.Errorf("cannot generate random progress listener name: %v", err)
		return
	}

	listName := string(b)
	listC := make(chan *ProgressNessage)

	rs.registerProgressListener(listName, listC)

	for pmsg := range listC {
		err = websocket.JSON.Send(ws, *pmsg)
		if err != nil {
			glog.Infof("error sending progress: %v", err)
			break
		}
	}

	rs.unregisterProgressListener(listName)
	close(listC)
}

func (rs *RombaService) startArchive(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if len(args) == 0 {
		return nil
	}

	if rs.busy {
		p := rs.pt.GetProgress()

		fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.Bytes(uint64(p.BytesSoFar)), humanize.Bytes(uint64(p.TotalBytes)))
		return nil
	}

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "archive"

	go func() {
		glog.Infof("service starting archive")
		rs.broadCastProgress(time.Now(), true, false, "")
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

		resume := cmd.Flag.Lookup("resume").Value.Get().(string)
		includezips := cmd.Flag.Lookup("include-zips").Value.Get().(bool)
		onlyneeded := cmd.Flag.Lookup("only-needed").Value.Get().(bool)

		endMsg, err := rs.depot.Archive(args, resume, includezips, onlyneeded, rs.numWorkers, rs.logDir, rs.pt)
		if err != nil {
			glog.Errorf("error archiving: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		rs.broadCastProgress(time.Now(), false, true, endMsg)
		glog.Infof("service finished archiving")
	}()

	fmt.Fprintf(cmd.Stdout, "started archiving")
	return nil
}

func (rs *RombaService) dir2dat(cmd *commander.Command, args []string) error {
	outpath := cmd.Flag.Lookup("out").Value.Get().(string)

	if err := os.MkdirAll(outpath, 0777); err != nil {
		return err
	}

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

	fmt.Fprintf(cmd.Stdout, "dir2dat successfully completed a DAT in %s for directory %s", outpath, srcpath)
	return nil
}
