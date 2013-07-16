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
	"code.google.com/p/gcfg"
	"flag"
	"fmt"
	"github.com/uwedeportivo/romba/archive"
	"github.com/uwedeportivo/romba/db"
	"log"
	"os"
)

const (
	versionStr = "1.0"
)

func usage() {
	fmt.Fprintf(os.Stderr, "%s version %s, Copyright (c) 2013 Uwe Hoffmann. All rights reserved.\n", os.Args[0], versionStr)
	fmt.Fprintf(os.Stderr, "\t                 %s <dirs to archive>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nFlag defaults:\n")
	flag.PrintDefaults()
}

type Config struct {
	Depot struct {
		Root    []string
		MaxSize []int64
	}
}

func main() {
	flag.Usage = usage

	help := flag.Bool("help", false, "show this message")
	version := flag.Bool("version", false, "show version")
	resume := flag.String("resume", "", "resume path")

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *version {
		fmt.Fprintf(os.Stderr, "%s version %s, Copyright (c) 2013 Uwe Hoffmann. All rights reserved.\n", os.Args[0], versionStr)
		os.Exit(0)
	}

	config := new(Config)
	err := gcfg.ReadFileInto(config, "depot.ini")
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading depot.ini failed: %v\n", err)
		os.Exit(1)
	}

	for i := 0; i < len(config.Depot.MaxSize); i++ {
		config.Depot.MaxSize[i] *= int64(archive.GB)
	}

	depot, err := archive.NewDepot(config.Depot.Root, config.Depot.MaxSize, new(db.NoOpDB), 8)
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating depot failed: %v\n", err)
		os.Exit(1)
	}

	resumeLoggerFile, err := os.Create("archive-resume.log")
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating archive-resume.log failed: %v\n", err)
		os.Exit(1)
	}
	defer resumeLoggerFile.Close()

	archiveLoggerFile, err := os.Create("archive-work.log")
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating archive-work.log failed: %v\n", err)
		os.Exit(1)
	}
	defer archiveLoggerFile.Close()

	err = depot.Archive(flag.Args(), *resume, log.New(resumeLoggerFile, "", 0), log.New(archiveLoggerFile, "", 0))

	if err != nil {
		fmt.Fprintf(os.Stderr, "archiving failed: %v\n", err)
		os.Exit(1)
	}
}
