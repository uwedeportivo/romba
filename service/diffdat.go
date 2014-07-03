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

	"github.com/golang/glog"

	"github.com/uwedeportivo/commander"
	"github.com/uwedeportivo/romba/parser"
	"github.com/uwedeportivo/romba/types"
)

func diffRoms(oldCrcs, oldMd5s, oldSha1s map[string]bool, og, ng *types.Game) *types.Game {
	diffGame := new(types.Game)
	diffGame.Name = ng.Name
	diffGame.Description = ng.Description

	ko, kn := 0, 0

	for ko < len(og.Roms) && kn < len(ng.Roms) {
		or, nr := og.Roms[ko], ng.Roms[kn]

		if or.Name < nr.Name {
			// old rom not in new, ignore
			ko++
		} else if or.Name > nr.Name {
			// new rom not in old, import wholesale
			glog.V(2).Infof("rom %s in new game and not in old game", nr.Name)
			if filterRom(oldCrcs, oldMd5s, oldSha1s, nr) {
				diffGame.Roms = append(diffGame.Roms, nr)
			}
			kn++
		} else {
			// rom in both
			kn++
			ko++
		}
	}

	for kn < len(ng.Roms) {
		nr := ng.Roms[kn]

		glog.V(2).Infof("rom %s in new game and not in old game", nr.Name)
		if filterRom(oldCrcs, oldMd5s, oldSha1s, nr) {
			diffGame.Roms = append(diffGame.Roms, nr)
		}
		kn++
	}

	if len(diffGame.Roms) > 0 {
		return diffGame
	}
	return nil
}

func filterRom(oldCrcs, oldMd5s, oldSha1s map[string]bool, r *types.Rom) bool {
	if r.Size > 0 && len(r.Crc) == 0 && len(r.Md5) == 0 && len(r.Sha1) == 0 {
		return false
	}

	if len(r.Crc) > 0 && oldCrcs[string(r.Crc)] {
		return false
	}

	if len(r.Md5) > 0 && oldMd5s[string(r.Md5)] {
		return false
	}

	if len(r.Sha1) > 0 && oldSha1s[string(r.Sha1)] {
		return false
	}

	return true
}

func filterGame(oldCrcs, oldMd5s, oldSha1s map[string]bool, g *types.Game) *types.Game {
	filteredGame := new(types.Game)
	filteredGame.Name = g.Name
	filteredGame.Description = g.Description

	for _, r := range g.Roms {
		if filterRom(oldCrcs, oldMd5s, oldSha1s, r) {
			filteredGame.Roms = append(filteredGame.Roms, r)
		}
	}

	return filteredGame
}

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

	newDat = newDat.Narrow()

	if givenName == "" {
		givenName = strings.TrimSuffix(filepath.Base(outPath), filepath.Ext(outPath))
	}

	if givenDescription == "" {
		givenDescription = givenName
	}

	diffDat := new(types.Dat)
	diffDat.Name = givenName
	diffDat.Description = givenDescription
	diffDat.Path = newDat.Path
	diffDat.UnzipGames = newDat.UnzipGames

	oldCrcs := make(map[string]bool)
	oldMd5s := make(map[string]bool)
	oldSha1s := make(map[string]bool)

	var key string

	for _, og := range oldDat.Games {
		for _, or := range og.Roms {
			if len(or.Crc) > 0 {
				key = string(or.Crc)
				oldCrcs[key] = true
			}
			if len(or.Md5) > 0 {
				key = string(or.Md5)
				oldMd5s[key] = true
			}
			if len(or.Sha1) > 0 {
				key = string(or.Sha1)
				oldSha1s[key] = true
			}
		}
	}

	ko, kn := 0, 0

	for ko < len(oldDat.Games) && kn < len(newDat.Games) {
		og, ng := oldDat.Games[ko], newDat.Games[kn]

		if og.Name < ng.Name {
			// old game not in new, ignore
			ko++
		} else if og.Name > ng.Name {
			glog.V(2).Infof("game %s in new dat and not in old dat", ng.Name)
			filteredGame := filterGame(oldCrcs, oldMd5s, oldSha1s, ng)
			if len(filteredGame.Roms) > 0 {
				diffDat.Games = append(diffDat.Games, filteredGame)
			}
			kn++
		} else {
			// game in both, diff it, keeping only new roms
			diffRom := diffRoms(oldCrcs, oldMd5s, oldSha1s, og, ng)
			if diffRom != nil {
				diffDat.Games = append(diffDat.Games, diffRom)
			}
			kn++
			ko++
		}
	}

	for kn < len(newDat.Games) {
		ng := newDat.Games[kn]

		glog.V(2).Infof("game %s in new dat and not in old dat", ng.Name)
		filteredGame := filterGame(oldCrcs, oldMd5s, oldSha1s, ng)
		if len(filteredGame.Roms) > 0 {
			diffDat.Games = append(diffDat.Games, filteredGame)
		}
		kn++
	}

	if len(diffDat.Games) > 0 {
		glog.Infof("diffdat finished, %d games with diffs found, writing diffdat file %s",
			len(diffDat.Games), outPath)
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
	} else {
		glog.Infof("diffdat finished, no diffs found, no diffdat file written")
	}

	return nil
}
