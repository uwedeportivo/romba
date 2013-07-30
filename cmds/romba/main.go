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

package main

import (
	"bufio"
	"code.google.com/p/gcfg"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/gonuts/commander"
	"github.com/gonuts/flag"
	"github.com/uwedeportivo/romba/archive"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/types"
	"hash/crc32"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	_ "github.com/uwedeportivo/romba/db/unq"
	_ "net/http/pprof"
)

type Config struct {
	General struct {
		LogDir    string
		TmpDir    string
		Workers   int
		Verbosity int
	}

	Depot struct {
		Root    []string
		MaxSize []int64
	}

	Index struct {
		Db   string
		Dats string
	}
}

var config *Config
var cmd *commander.Commander
var romDB db.RomDB

func init() {
	config = new(Config)

	cmd = new(commander.Commander)
	cmd.Name = os.Args[0]
	cmd.Commands = make([]*commander.Command, 10)
	cmd.Flag = flag.NewFlagSet("romba", flag.ExitOnError)

	cmd.Commands[0] = &commander.Command{
		Run:       refreshDats,
		UsageLine: "refresh-dats",
		Short:     "Refreshes the DAT index from the files in the DAT master directory tree.",
		Long: `
Refreshes the DAT index from the files in the DAT master directory tree.
Detects any changes in the DAT master directory tree and updates the DAT index
accordingly, marking deleted or overwritten dats as orphaned and updating
contents of any changed dats.`,
		Flag: *flag.NewFlagSet("romba-refresh-dats", flag.ExitOnError),
	}

	cmd.Commands[1] = &commander.Command{
		Run:       archiveRoms,
		UsageLine: "archive [-only-needed] <space-separated list of directories of ROM files>",
		Short:     "Adds ROM files from the specified directories to the ROM archive.",
		Long: `
Adds ROM files from the specified directories to the ROM archive.
Traverses the specified directory trees looking for zip files and normal files.
Unpacked files will be stored as individual entries. Prior to unpacking a zip
file, the external SHA1 is checked against the DAT index. 
If -only-needed is set, only those files are put in the ROM archive that
have a current entry in the DAT index.`,

		Flag: *flag.NewFlagSet("romba-archive", flag.ExitOnError),
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
		Flag: *flag.NewFlagSet("romba-purge-delete", flag.ExitOnError),
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
		Flag: *flag.NewFlagSet("romba-purge-backup", flag.ExitOnError),
	}

	cmd.Commands[3].Flag.String("backup", "", "backup directory where backup files are moved to")

	cmd.Commands[4] = &commander.Command{
		Run:       runCmd,
		UsageLine: "dir2dat -out <outputfile> -source <sourcedir>",
		Short:     "Creates a DAT file for the specified input directory and saves it to the -out filename.",
		Long: `
Walks the specified input directory and builds a DAT file that mirrors its
structure. Saves this DAT file in specified output filename.`,
		Flag: *flag.NewFlagSet("romba-dir2dat", flag.ExitOnError),
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
		Flag: *flag.NewFlagSet("romba-diffdat", flag.ExitOnError),
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
		Flag: *flag.NewFlagSet("romba-fixdat", flag.ExitOnError),
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
		Flag: *flag.NewFlagSet("romba-miss", flag.ExitOnError),
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
		Flag: *flag.NewFlagSet("romba-build", flag.ExitOnError),
	}

	cmd.Commands[8].Flag.String("out", "", "output dir")

	cmd.Commands[9] = &commander.Command{
		Run:       lookup,
		UsageLine: "lookup <list of hashes or files>",
		Short:     "For each specified hash or file it looks up any available information.",
		Long: `
For each specified hash it looks up any available information (dat or rom).
For each specified file it computes the three hashes crc, md5 and sha1 and
then looks up any available information.`,
		Flag: *flag.NewFlagSet("romba-build", flag.ExitOnError),
	}

}

func runCmd(cmd *commander.Command, args []string) {
	fmt.Printf("command %v with args %v\n", cmd, args)
}

func refreshDats(cmd *commander.Command, args []string) {
	logPath := filepath.Join(config.General.LogDir, fmt.Sprintf("refresh-%s.log", time.Now().Format("2006-01-02-15_04_05")))

	logfile, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log file %s: %v\n", logPath, err)
		os.Exit(1)
	}
	defer logfile.Close()

	buflog := bufio.NewWriter(logfile)
	defer buflog.Flush()

	err = db.Refresh(romDB, config.Index.Dats, log.New(buflog, "", 0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "refreshing dat index failed: %v\n", err)
		os.Exit(1)
	}
}

func archiveRoms(cmd *commander.Command, args []string) {
	if len(args) == 0 {
		return
	}

	logPath := filepath.Join(config.General.LogDir, fmt.Sprintf("archive-%s.log", time.Now().Format("2006-01-02-15_04_05")))

	logfile, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log file %s: %v\n", logPath, err)
		os.Exit(1)
	}
	defer logfile.Close()

	buflog := bufio.NewWriter(logfile)
	defer buflog.Flush()

	depot, err := archive.NewDepot(config.Depot.Root, config.Depot.MaxSize, romDB, 8)
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating depot failed: %v\n", err)
		os.Exit(1)
	}

	resumeLogPath := filepath.Join(config.General.LogDir, fmt.Sprintf("archive-resume-%s.log", time.Now().Format("2006-01-02-15_04_05")))
	resumeLogFile, err := os.Create(resumeLogPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log file %s: %v\n", resumeLogPath, err)
		os.Exit(1)
	}
	defer resumeLogFile.Close()

	bufresumelog := bufio.NewWriter(resumeLogFile)
	defer bufresumelog.Flush()

	resume := cmd.Flag.Lookup("resume").Value.Get().(string)

	err = depot.Archive(args, "", log.New(bufresumelog, resume, 0), log.New(buflog, "", 0))

	if err != nil {
		fmt.Fprintf(os.Stderr, "archiving failed: %v\n", err)
		os.Exit(1)
	}
}

func lookupByHash(hash []byte) (bool, error) {
	found := false
	if len(hash) == sha1.Size {
		dat, err := romDB.GetDat(hash)
		if err != nil {
			return false, err
		}

		if dat != nil {
			fmt.Printf("dat = %s\n", types.PrintDat(dat))
			found = true
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
		return false, fmt.Errorf("found unknown hash size: %d", len(hash))
	}

	dats, err := romDB.DatsForRom(r)
	if err != nil {
		return false, err
	}

	for _, dat := range dats {
		fmt.Printf("dat = %s\n", types.PrintDat(dat))
	}

	found = found || len(dats) > 0
	return found, nil
}

func lookup(cmd *commander.Command, args []string) {
	if len(args) == 0 {
		return
	}

	for _, arg := range args {
		exists, err := archive.PathExists(arg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to stat %s: %v\n", arg, err)
			os.Exit(1)
		}
		if exists {
			hh, err := archive.HashesForFile(arg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to compute hashes for %s: %v\n", arg, err)
				os.Exit(1)
			}

			found, err := lookupByHash(hh.Sha1)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to lookup by sha1 hash for %s: %v\n", arg, err)
				os.Exit(1)
			}
			if found {
				continue
			}
			found, err = lookupByHash(hh.Md5)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to lookup by md5 hash for %s: %v\n", arg, err)
				os.Exit(1)
			}
			if found {
				continue
			}

			found, err = lookupByHash(hh.Crc)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to lookup by crc hash for %s: %v\n", arg, err)
				os.Exit(1)
			}
		} else {
			hash, err := hex.DecodeString(arg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to decode hex string %s: %v\n", arg, err)
				os.Exit(1)
			}
			_, err = lookupByHash(hash)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to lookup by hash for %s: %v\n", arg, err)
				os.Exit(1)
			}
		}
	}

}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := gcfg.ReadFileInto(config, "romba.ini")
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}

	for i := 0; i < len(config.Depot.MaxSize); i++ {
		config.Depot.MaxSize[i] *= int64(archive.GB)
	}

	romDB, err = db.New(config.Index.Db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening db failed: %v\n", err)
		os.Exit(1)
	}
	defer romDB.Close()

	err = cmd.Flag.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing cmd line flags failed: %v\n", err)
		os.Exit(1)
	}

	args := cmd.Flag.Args()
	err = cmd.Run(args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
