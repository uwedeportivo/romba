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
	KnowTotal       bool
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
		pmsg.KnowTotal = p.KnowTotal()
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
	fmt.Fprintf(cmd.Stdout, "command %s with args %s not implemented yet\n", cmd.Name, strings.Join(args, " "))
	return nil
}

func (rs *RombaService) lookup(cmd *commander.Command, args []string) error {
	for _, arg := range args {
		fmt.Fprintf(cmd.Stdout, "----------------------------------------\n")
		fmt.Fprintf(cmd.Stdout, "key: %s\n", arg)

		if strings.HasPrefix(arg, "0x") {
			arg = arg[2:]
		}

		hash, err := hex.DecodeString(arg)
		if err != nil {
			return err
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

		if len(hash) == sha1.Size {
			dat, err := rs.romDB.GetDat(hash)
			if err != nil {
				return err
			}

			if dat != nil {
				fmt.Fprintf(cmd.Stdout, "-----------------\n")
				fmt.Fprintf(cmd.Stdout, "dat with sha1 %s = %s\n", arg, types.PrintShortDat(dat))
			}

			inDepot, hh, err := rs.depot.SHA1InDepot(arg)
			if err != nil {
				return err
			}

			if inDepot {
				fmt.Fprintf(cmd.Stdout, "-----------------\n")
				fmt.Fprintf(cmd.Stdout, "rom file %s.gz in depot\n", arg)
				fmt.Fprintf(cmd.Stdout, "crc = %s\n", hex.EncodeToString(hh.Crc))
				fmt.Fprintf(cmd.Stdout, "md5 = %s\n", hex.EncodeToString(hh.Md5))
				r.Crc = hh.Crc
				r.Md5 = hh.Md5
			}
		}

		err = rs.romDB.CompleteRom(r)
		if err != nil {
			return err
		}

		dats, err := rs.romDB.DatsForRom(r)
		if err != nil {
			return err
		}

		if len(dats) > 0 {
			fmt.Fprintf(cmd.Stdout, "-----------------\n")
			fmt.Fprintf(cmd.Stdout, "rom found in:\n")
			for _, dat := range dats {
				dn := dat.NarrowToRom(r)
				if dn != nil {
					fmt.Fprintf(cmd.Stdout, "%s\n", types.PrintDat(dn))
				}
			}

			used := false
			var realDat *types.Dat

			for _, dat := range dats {
				if !dat.Artificial && dat.Generation == rs.romDB.Generation() {
					used = true
					realDat = dat
					break
				}
			}

			fmt.Fprintf(cmd.Stdout, "-----------------\n")
			if used {
				fmt.Fprintf(cmd.Stdout, "rom used in at least %s\n", realDat.Path)
			} else {
				fmt.Fprintf(cmd.Stdout, "rom not used\n")
			}
		}

		fmt.Fprintf(cmd.Stdout, "-----------------\n")
		fmt.Fprintf(cmd.Stdout, "DebugGet:\n%s\n", rs.romDB.DebugGet(hash))
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
