// Copyright (c) 2013 Uwe Hoffmann. All rights reserved.

package main

import (
	"flag"
	"fmt"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/worker"
	"log"
	"os"
	"path/filepath"
)

const (
	versionStr = "1.0"
)

func usage() {
	fmt.Fprintf(os.Stderr, "%s version %s, Copyright (c) 2013 Uwe Hoffmann. All rights reserved.\n", os.Args[0], versionStr)
	fmt.Fprintf(os.Stderr, "\t                 %s <path to tosec dir>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nFlag defaults:\n")
	flag.PrintDefaults()
}

type parseWorker struct{}

func (pw *parseWorker) Process(path string, size int64, logger *log.Logger) error {
	_, _, err := parser.Parse(path)
	return err
}

type parseMaster struct{}

func (pm *parseMaster) Accept(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".dat" || ext == ".xml"
}

func (pm *parseMaster) NewWorker(workerIndex int) worker.Worker {
	return new(parseWorker)
}

func (pm *parseMaster) NumWorkers() int {
	return 8
}

func main() {
	flag.Usage = usage

	help := flag.Bool("help", false, "show this message")
	version := flag.Bool("version", false, "show version")

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *version {
		fmt.Fprintf(os.Stderr, "%s version %s, Copyright (c) 2013 Uwe Hoffmann. All rights reserved.\n", os.Args[0], versionStr)
		os.Exit(0)
	}

	err := worker.Work(flag.Args(), new(parseMaster), nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, " error: %v\n", err)
		os.Exit(1)
	}
}
