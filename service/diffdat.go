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

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/karrick/godirwalk"

	"github.com/uwedeportivo/commander"
	"github.com/uwedeportivo/romba/dedup"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/worker"
)

func (rs *RombaService) diffdat(cmd *commander.Command, args []string) error {
	oldDatPath := cmd.Flag.Lookup("old").Value.Get().(string)
	newDatPath := cmd.Flag.Lookup("new").Value.Get().(string)
	outPath := cmd.Flag.Lookup("out").Value.Get().(string)
	givenName := cmd.Flag.Lookup("name").Value.Get().(string)
	givenDescription := cmd.Flag.Lookup("description").Value.Get().(string)

	if oldDatPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-old argument required")
		if err != nil {
			return err
		}
		return errors.New("missing old argument")
	}
	if newDatPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-new argument required")
		if err != nil {
			return err
		}
		return errors.New("missing new argument")
	}
	if outPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-out argument required")
		if err != nil {
			return err
		}
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
	defer func() {
		err := dd.Close()
		if err != nil {
			glog.Errorf("error closing dedup leveldb: %v", err)
		}
	}()

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
		defer func() {
			err := diffFile.Close()
			if err != nil {
				glog.Errorf("error closing diff file %s: %v", outPath, err)
			}
		}()

		diffWriter := bufio.NewWriter(diffFile)
		defer func() {
			err := diffWriter.Flush()
			if err != nil {
				glog.Errorf("error flushing diff file %s: %v", outPath, err)
			}
		}()

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
	_, err = fmt.Fprintf(cmd.Stdout, endMsg)
	if err != nil {
		return err
	}
	rs.broadCastProgress(time.Now(), false, true, endMsg, nil)
	return nil
}

type declareParseListener struct {
	dd dedup.Deduper
}

func (ipl *declareParseListener) ParsedDatStmt(dat *types.Dat) error {
	return nil
}

func (ipl *declareParseListener) ParsedGameStmt(game *types.Game) error {
	for _, r := range game.Roms {
		err := ipl.dd.Declare(r)
		if err != nil {
			return err
		}
	}
	return nil
}

type dedupParseListener struct {
	dd         dedup.Deduper
	oneDiffDat *types.Dat
}

func (ipl *dedupParseListener) ParsedDatStmt(dat *types.Dat) error {
	ipl.oneDiffDat.CopyHeader(dat)
	return nil
}

func (ipl *dedupParseListener) ParsedGameStmt(game *types.Game) error {
	gc := new(types.Game)
	gc.CopyHeader(game)
	for _, r := range game.Roms {
		if !r.Valid() {
			continue
		}
		seen, err := ipl.dd.Seen(r)
		if err != nil {
			return err
		}
		if !seen {
			gc.Roms = append(gc.Roms, r)
			err = ipl.dd.Declare(r)
			if err != nil {
				return err
			}
		}
	}
	if len(gc.Roms) > 0 {
		ipl.oneDiffDat.Games = append(ipl.oneDiffDat.Games, gc)
	}
	return nil
}

func (rs *RombaService) ediffdatWork(cmd *commander.Command, args []string) error {
	oldDatPath := cmd.Flag.Lookup("old").Value.Get().(string)
	newDatPath := cmd.Flag.Lookup("new").Value.Get().(string)
	outPath := cmd.Flag.Lookup("out").Value.Get().(string)

	if oldDatPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-old argument required")
		if err != nil {
			return err
		}
		return errors.New("missing old argument")
	}
	if newDatPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-new argument required")
		if err != nil {
			return err
		}
		return errors.New("missing new argument")
	}
	if outPath == "" {
		_, err := fmt.Fprintf(cmd.Stdout, "-out argument required")
		if err != nil {
			return err
		}
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
	defer func() {
		err := dd.Close()
		if err != nil {
			glog.Errorf("error closing leveldb deduper: %v", err)
		}
	}()

	ipl := new(declareParseListener)
	ipl.dd = dd

	err = godirwalk.Walk(oldDatPath, &godirwalk.Options{
		Unsorted: true,
		Callback: func(path string, info *godirwalk.Dirent) error {
			if info.IsDir() {
				return nil
			}

			ext := filepath.Ext(path)
			if ext == ".dat" || ext == ".xml" {
				rs.pt.DeclareFile(path)

				_, err := parser.ParseWithListener(path, ipl)
				if err != nil {
					return err
				}

				fi, err := os.Stat(path)
				if err != nil {
					return err
				}

				rs.pt.AddBytesFromFile(fi.Size(), false)
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	err = godirwalk.Walk(newDatPath, &godirwalk.Options{
		Unsorted: true,
		Callback: func(path string, info *godirwalk.Dirent) error {
			if info.IsDir() {
				return nil
			}

			ext := filepath.Ext(path)
			if ext == ".dat" || ext == ".xml" {
				rs.pt.DeclareFile(path)

				ipl := new(dedupParseListener)
				ipl.dd = dd
				ipl.oneDiffDat = new(types.Dat)

				_, err := parser.ParseWithListener(path, ipl)
				if err != nil {
					return err
				}

				oneDiffDat := ipl.oneDiffDat

				if len(oneDiffDat.Games) > 0 {
					oneDiffDat = oneDiffDat.FilterRoms(func(r *types.Rom) bool {
						return r.Size > 0
					})
					if oneDiffDat != nil {
						commonRoot := worker.CommonRoot(path, outPath)
						destDir := filepath.Join(outPath, filepath.Dir(strings.TrimPrefix(path, commonRoot)))
						err := os.MkdirAll(destDir, 0777)
						if err != nil {
							glog.Errorf("error mkdir %s: %v", destDir, err)
							return err
						}

						err = writeDat(oneDiffDat, filepath.Join(destDir, oneDiffDat.Name+".dat"))
					}
				}

				fi, serr := os.Stat(path)
				if err != nil {
					return serr
				}

				rs.pt.AddBytesFromFile(fi.Size(), err != nil)
				return err
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (rs *RombaService) ediffdat(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	if rs.busy {
		p := rs.pt.GetProgress()

		_, err := fmt.Fprintf(cmd.Stdout, "still busy with %s: (%d of %d files) and (%s of %s) \n", rs.jobName,
			p.FilesSoFar, p.TotalFiles, humanize.IBytes(uint64(p.BytesSoFar)), humanize.IBytes(uint64(p.TotalBytes)))
		return err
	}

	rs.pt.Reset()
	rs.busy = true
	rs.jobName = "ediffdat"

	go func() {
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

		err := rs.ediffdatWork(cmd, args)
		if err != nil {
			glog.Errorf("error ediffdats: %v", err)
		}

		ticker.Stop()
		stopTicker <- true

		rs.jobMutex.Lock()
		rs.busy = false
		rs.jobName = ""
		rs.jobMutex.Unlock()

		glog.Infof("ediffdat finished")
		rs.pt.Finished()
		rs.broadCastProgress(time.Now(), false, true, "ediffdat finished", err)
	}()

	glog.Infof("service starting ediffdat")
	_, err := fmt.Fprintf(cmd.Stdout, "started ediffdat")
	return err
}

func writeDat(dat *types.Dat, outPath string) error {
	dat.Path = outPath

	file, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			glog.Errorf("error closing file %s: %v", outPath, err)
		}
	}()

	writer := bufio.NewWriter(file)
	defer func() {
		err := writer.Flush()
		if err != nil {
			glog.Errorf("error flushing file %s: %v", outPath, err)
		}
	}()

	return types.ComposeCompliantDat(dat, writer)
}
