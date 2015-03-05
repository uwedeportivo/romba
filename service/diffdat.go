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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/uwedeportivo/commander"
	"github.com/uwedeportivo/romba/dedup"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
)

func (rs *RombaService) diffdat(cmd *commander.Command, args []string) error {
	oldDatPath := cmd.Flag.Lookup("old").Value.Get().(string)
	newDatPath := cmd.Flag.Lookup("new").Value.Get().(string)
	outPath := cmd.Flag.Lookup("out").Value.Get().(string)
	givenName := cmd.Flag.Lookup("name").Value.Get().(string)
	givenDescription := cmd.Flag.Lookup("description").Value.Get().(string)

	if oldDatPath == "" {
		fmt.Fprintf(cmd.Stdout, "-old argument required")
		return errors.New("missing old argument")
	}
	if newDatPath == "" {
		fmt.Fprintf(cmd.Stdout, "-new argument required")
		return errors.New("missing new argument")
	}
	if outPath == "" {
		fmt.Fprintf(cmd.Stdout, "-out argument required")
		return errors.New("missing out argument")
	}

	glog.Infof("diffdat new dat %s and old dat %s into %s", newDatPath, oldDatPath, outPath)

	oldDat, _, err := parser.Parse(oldDatPath)
	if err != nil {
		return err
	}

	newDat, _, err := parser.Parse(newDatPath)
	if err != nil {
		return err
	}

	if givenName == "" {
		givenName = strings.TrimSuffix(filepath.Base(outPath), filepath.Ext(outPath))
	}

	if givenDescription == "" {
		givenDescription = givenName
	}

	dd, err := dedup.NewLevelDBDeduper()
	if err != nil {
		return err
	}
	defer dd.Close()

	err = dedup.Declare(oldDat, dd)
	if err != nil {
		return err
	}

	diffDat, err := dedup.Dedup(newDat, dd)
	if err != nil {
		return err
	}

	diffDat = diffDat.FilterRoms(func(r *types.Rom) bool {
		return r.Size > 0
	})

	var endMsg string

	if diffDat != nil {
		diffDat.Name = givenName
		diffDat.Description = givenDescription
		diffDat.Path = outPath

		diffFile, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer diffFile.Close()

		diffWriter := bufio.NewWriter(diffFile)
		defer diffWriter.Flush()

		err = types.ComposeCompliantDat(diffDat, diffWriter)
		if err != nil {
			return err
		}

		endMsg = fmt.Sprintf("diffdat finished, %d games with diffs found, written diffdat file %s",
			len(diffDat.Games), outPath)
	} else {
		endMsg = "diffdat finished, no diffs found, no diffdat file written"

	}

	glog.Infof(endMsg)
	fmt.Fprintf(cmd.Stdout, endMsg)
	rs.broadCastProgress(time.Now(), false, true, endMsg)

	return nil
}

func (rs *RombaService) ediffdat(cmd *commander.Command, args []string) error {
	oldDatPath := cmd.Flag.Lookup("old").Value.Get().(string)
	newDatPath := cmd.Flag.Lookup("new").Value.Get().(string)
	outPath := cmd.Flag.Lookup("out").Value.Get().(string)

	if oldDatPath == "" {
		fmt.Fprintf(cmd.Stdout, "-old argument required")
		return errors.New("missing old argument")
	}
	if newDatPath == "" {
		fmt.Fprintf(cmd.Stdout, "-new argument required")
		return errors.New("missing new argument")
	}
	if outPath == "" {
		fmt.Fprintf(cmd.Stdout, "-out argument required")
		return errors.New("missing out argument")
	}

	err := os.MkdirAll(outPath, 0777)
	if err != nil {
		return err
	}

	glog.Infof("ediffdat new dat %s and old dat %s into %s", newDatPath, oldDatPath, outPath)

	dd, err := dedup.NewLevelDBDeduper()
	if err != nil {
		return err
	}
	defer dd.Close()

	err = filepath.Walk(oldDatPath, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		if ext == ".dat" || ext == ".xml" {
			oldDat, _, err := parser.Parse(path)
			if err != nil {
				return err
			}

			err = dedup.Declare(oldDat, dd)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	err = filepath.Walk(newDatPath, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		if ext == ".dat" || ext == ".xml" {
			newDat, _, err := parser.Parse(path)
			if err != nil {
				return err
			}

			oneDiffDat, err := dedup.Dedup(newDat, dd)
			if err != nil {
				return err
			}

			if oneDiffDat != nil {
				oneDiffDat = oneDiffDat.FilterRoms(func(r *types.Rom) bool {
					return r.Size > 0
				})
			}

			if oneDiffDat != nil {
				return writeDiffDat(oneDiffDat, filepath.Join(outPath, oneDiffDat.Name+".dat"))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	glog.Infof("ediffdat finished")
	fmt.Fprintf(cmd.Stdout, "ediffdat finished")
	rs.broadCastProgress(time.Now(), false, true, "ediffdat finished")

	return nil
}

func writeDiffDat(diffDat *types.Dat, outPath string) error {
	diffDat.Path = outPath

	diffFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer diffFile.Close()

	diffWriter := bufio.NewWriter(diffFile)
	defer diffWriter.Flush()

	return types.ComposeCompliantDat(diffDat, diffWriter)
}
