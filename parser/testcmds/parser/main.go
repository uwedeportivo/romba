// Copyright (c) 2013 Uwe Hoffmann. All rights reserved.

package main

import (
	"flag"
	"fmt"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/worker"
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

func parse(path string) error {
	_, _, err := parser.Parse(path)
	return err
}

func accept(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".dat" || ext == ".xml"
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

	err := worker.Work(flag.Args(), accept, parse, 8, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, " error: %v\n", err)
		os.Exit(1)
	}
}
