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
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/websocket"
	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/gonuts/commander"
	"github.com/gonuts/flag"

	"github.com/uwedeportivo/romba/db"
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

func NewRombaService(romDB db.RomDB, dats string, numWorkers int) *RombaService {
	rs := new(RombaService)
	rs.romDB = romDB
	rs.dats = dats
	rs.numWorkers = numWorkers
	rs.pt = worker.NewProgressTracker()
	rs.jobMutex = new(sync.Mutex)
	rs.progressMutex = new(sync.Mutex)
	rs.progressListeners = make(map[string]chan *ProgressNessage)
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

	cmd := newCommander(outbuf, rs)

	cmdTxtSplit := strings.Fields(req.CmdTxt)

	err := cmd.Flag.Parse(cmdTxtSplit)
	if err != nil {
		reply.Message = fmt.Sprintf("error: parsing command failed: %v\n", err)
		return nil
	}

	args := cmd.Flag.Args()
	err = cmd.Run(args)
	if err != nil {
		reply.Message = fmt.Sprintf("error: executing command failed: %v\n", err)
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
		rs.broadCastProgress(time.Now(), true, false, "starting refresh-dats")
		ticker := time.NewTicker(time.Second * 5)
		go func() {
			for t := range ticker.C {
				rs.broadCastProgress(t, false, false, "")
			}
		}()

		endMsg, err := db.Refresh(rs.romDB, rs.dats, rs.numWorkers, rs.pt)
		if err != nil {
			glog.Errorf("error refreshing dats: %v", err)
		}

		ticker.Stop()

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		rs.broadCastProgress(time.Now(), false, true, endMsg)
	}()

	fmt.Fprintf(cmd.Stdout, "started refresh dats")
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

func newCommander(writer io.Writer, rs *RombaService) *commander.Commander {
	cmd := new(commander.Commander)
	cmd.Name = "Romba"
	cmd.Commands = make([]*commander.Command, 11)
	cmd.Flag = flag.NewFlagSet("romba", flag.ContinueOnError)
	cmd.Stdout = writer
	cmd.Stderr = writer

	cmd.Commands[0] = &commander.Command{
		Run:       rs.startRefreshDats,
		UsageLine: "refresh-dats",
		Short:     "Refreshes the DAT index from the files in the DAT master directory tree.",
		Long: `
Refreshes the DAT index from the files in the DAT master directory tree.
Detects any changes in the DAT master directory tree and updates the DAT index
accordingly, marking deleted or overwritten dats as orphaned and updating
contents of any changed dats.`,
		Flag:   *flag.NewFlagSet("romba-refresh-dats", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[1] = &commander.Command{
		Run:       runCmd,
		UsageLine: "archive [-only-needed] <space-separated list of directories of ROM files>",
		Short:     "Adds ROM files from the specified directories to the ROM archive.",
		Long: `
Adds ROM files from the specified directories to the ROM archive.
Traverses the specified directory trees looking for zip files and normal files.
Unpacked files will be stored as individual entries. Prior to unpacking a zip
file, the external SHA1 is checked against the DAT index. 
If -only-needed is set, only those files are put in the ROM archive that
have a current entry in the DAT index.`,

		Flag:   *flag.NewFlagSet("romba-archive", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[1].Flag.Bool("only-needed", false, "only archive ROM files actually referenced by DAT files from the DAT index")
	cmd.Commands[1].Flag.String("resume", "", "resume a previously interrupted archive operation from the specified path")

	cmd.Commands[2] = &commander.Command{
		Run:       runCmd,
		UsageLine: "purge-delete <list of DAT files or folders with DAT files>",
		Short:     "Deletes DAT index entries for orphaned DATs.",
		Long: `
Deletes DAT index entries for orphaned DATs and deletes ROM files that are no
longer associated with any current DATs. Deletes ROM files that are only
associated with the specified DATs. It also deletes the specified DATs from
the DAT index.`,
		Flag:   *flag.NewFlagSet("romba-purge-delete", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[3] = &commander.Command{
		Run:       runCmd,
		UsageLine: "purge-backup -backup <backupdir> <list of DAT files or folders with DAT files>",
		Short:     "Moves DAT index entries for orphaned DATs.",
		Long: `
Deletes DAT index entries for orphaned DATs and moves ROM files that are no
longer associated with any current DATs to the specified backup folder.
Moves to the specified backup folder those ROM files that are only associated
with the specified DATs. The files will be placed in the backup location using
a folder structure according to the original DAT master directory tree
structure. It also deletes the specified DATs from the DAT index.`,
		Flag:   *flag.NewFlagSet("romba-purge-backup", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[3].Flag.String("backup", "", "backup directory where backup files are moved to")

	cmd.Commands[4] = &commander.Command{
		Run:       runCmd,
		UsageLine: "dir2dat -out <outputfile> -source <sourcedir>",
		Short:     "Creates a DAT file for the specified input directory and saves it to the -out filename.",
		Long: `
Walks the specified input directory and builds a DAT file that mirrors its
structure. Saves this DAT file in specified output filename.`,
		Flag:   *flag.NewFlagSet("romba-dir2dat", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[4].Flag.String("out", "", "output filename")
	cmd.Commands[4].Flag.String("source", "", "source directory")
	cmd.Commands[4].Flag.String("name", "", "name value in DAT header")
	cmd.Commands[4].Flag.String("description", "", "description value in DAT header")
	cmd.Commands[4].Flag.String("category", "", "category value in DAT header")
	cmd.Commands[4].Flag.String("version", "", "vesrion value in DAT header")
	cmd.Commands[4].Flag.String("author", "", "author value in DAT header")

	cmd.Commands[5] = &commander.Command{
		Run:       runCmd,
		UsageLine: "diffdat -old <datfile> -new <datfile> -out <outputfile>",
		Short:     "Creates a DAT file with those entries that are in -new DAT.",
		Long: `
Creates a DAT file with those entries that are in -new DAT file and not
in -old DAT file. Ignores those entries in -old that are not in -new.`,
		Flag:   *flag.NewFlagSet("romba-diffdat", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[5].Flag.String("out", "", "output filename")
	cmd.Commands[5].Flag.String("old", "", "old DAT file")
	cmd.Commands[5].Flag.String("new", "", "new DAT file")

	cmd.Commands[6] = &commander.Command{
		Run:       runCmd,
		UsageLine: "fixdat -out <outputdir> <list of DAT files or folders with DAT files>",
		Short:     "For each specified DAT file it creates a fix DAT.",
		Long: `
For each specified DAT file it creates a fix DAT with the missing entries for
that DAT. If nothing is missing it doesn't create a fix DAT for that
particular DAT.`,
		Flag:   *flag.NewFlagSet("romba-fixdat", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[6].Flag.String("out", "", "output dir")

	cmd.Commands[7] = &commander.Command{
		Run:       runCmd,
		UsageLine: "miss -out <outputdir> <list of DAT files or folders with DAT files>",
		Short:     "For each specified DAT file it creates a miss file and a have file.",
		Long: `
For each specified DAT file it creates a miss file and a have file in the
specified output dir. The files will be placed in the specified location using
a folder structure according to the original DAT master directory
tree structure.`,
		Flag:   *flag.NewFlagSet("romba-miss", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[7].Flag.String("out", "", "output dir")

	cmd.Commands[8] = &commander.Command{
		Run:       runCmd,
		UsageLine: "build -out <outputdir> <list of DAT files or folders with DAT files>",
		Short:     "For each specified DAT file it creates the torrentzip files.",
		Long: `
For each specified DAT file it creates the torrentzip files in the specified
output dir. The files will be placed in the specified location using a folder
structure according to the original DAT master directory tree structure.`,
		Flag:   *flag.NewFlagSet("romba-build", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[8].Flag.String("out", "", "output dir")

	cmd.Commands[9] = &commander.Command{
		Run:       runCmd,
		UsageLine: "lookup <list of hashes or files>",
		Short:     "For each specified hash or file it looks up any available information.",
		Long: `
For each specified hash it looks up any available information (dat or rom).
For each specified file it computes the three hashes crc, md5 and sha1 and
then looks up any available information.`,
		Flag:   *flag.NewFlagSet("romba-build", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}

	cmd.Commands[10] = &commander.Command{
		Run:       rs.progress,
		UsageLine: "progress",
		Short:     "Shows progress of the currently running command.",
		Long: `
Shows progress of the currently running command.`,
		Flag:   *flag.NewFlagSet("romba-progress", flag.ContinueOnError),
		Stdout: writer,
		Stderr: writer,
	}
	return cmd
}
