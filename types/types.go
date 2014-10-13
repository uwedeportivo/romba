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

package types

import (
	"bytes"
	"sort"
	"strings"
)

type Clrmamepro struct {
	ForcePacking string `xml:"forcepacking,attr"`
	ForceZipping string `xml:"forcezipping,attr"`
}

type Dat struct {
	Name        string      `xml:"header>name"`
	Description string      `xml:"header>description"`
	Clr         *Clrmamepro `xml:"header>clrmamepro"`
	Games       GameSlice   `xml:"game"`
	Generation  int64
	Path        string
	Software    GameSlice `xml:"software"`
	UnzipGames  bool
	FixDat      bool
}

type Game struct {
	Name        string   `xml:"name,attr"`
	Description string   `xml:"description"`
	Roms        RomSlice `xml:"rom"`
	Parts       RomSlice `xml:"part>dataarea>rom"`
	Regions     RomSlice `xml:"region>rom"`
}

type GameSlice []*Game

type Rom struct {
	Name   string `xml:"name,attr"`
	Size   int64  `xml:"size,attr"`
	Crc    []byte `xml:"crc,attr"`
	Md5    []byte `xml:"md5,attr"`
	Sha1   []byte `xml:"sha1,attr"`
	Status string `xml:"status,attr"`
	Path   string
}

type RomSlice []*Rom

func (ar *Rom) HashesMatch(br *Rom) bool {
	return (ar.Crc != nil && bytes.Equal(ar.Crc, br.Crc) && ar.Size == br.Size) ||
		(ar.Md5 != nil && bytes.Equal(ar.Md5, br.Md5) && ar.Size == br.Size) ||
		(ar.Sha1 != nil && bytes.Equal(ar.Sha1, br.Sha1))
}

func (ar *Rom) Equals(br *Rom) bool {
	if ar.Name != br.Name {
		return false
	}

	if ar.Size != br.Size {
		return false
	}

	if !ar.HashesMatch(br) {
		return false
	}
	return true
}

func (s GameSlice) Len() int           { return len(s) }
func (s GameSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s GameSlice) Less(i, j int) bool { return s[i].Name < s[j].Name }

func (s RomSlice) Len() int           { return len(s) }
func (s RomSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s RomSlice) Less(i, j int) bool { return s[i].Name < s[j].Name }

// assumes slices are sorted
func (as GameSlice) Equals(bs GameSlice) bool {
	if len(as) != len(bs) {
		return false
	}

	for i, ag := range as {
		if !bs[i].Equals(ag) {
			return false
		}
	}
	return true
}

// assumes slices are sorted
func (as RomSlice) Equals(bs RomSlice) bool {
	if len(as) != len(bs) {
		return false
	}

	for i, ag := range as {
		if !bs[i].Equals(ag) {
			return false
		}
	}
	return true
}

func (ag *Game) Equals(bg *Game) bool {
	if ag.Name != bg.Name {
		return false
	}

	if ag.Description != bg.Description {
		return false
	}

	if !ag.Roms.Equals(bg.Roms) {
		return false
	}
	return true
}

func (ad *Dat) Equals(bd *Dat) bool {
	if ad.Name != bd.Name {
		return false
	}

	if ad.Description != bd.Description {
		return false
	}

	if !ad.Games.Equals(bd.Games) {
		return false
	}

	if ad.UnzipGames != bd.UnzipGames {
		return false
	}
	return true
}

func (d *Dat) Normalize() {
	if d.Clr != nil && (d.Clr.ForcePacking == "unzip" || d.Clr.ForcePacking == "false" || d.Clr.ForcePacking == "no" ||
		d.Clr.ForceZipping == "unzip" || d.Clr.ForceZipping == "false" || d.Clr.ForceZipping == "no") {
		d.UnzipGames = true
	}
	if d.Software != nil {
		d.Games = append(d.Games, d.Software...)
		d.Software = nil
	}
	sort.Sort(d.Games)

	for _, g := range d.Games {
		g.Name = strings.Replace(g.Name, "\\", "/", -1)

		if g.Parts != nil {
			g.Roms = append(g.Roms, g.Parts...)
			g.Parts = nil
		}
		if g.Regions != nil {
			g.Roms = append(g.Roms, g.Regions...)
			g.Regions = nil
		}
		sort.Sort(g.Roms)

		filteredRoms := make([]*Rom, 0, len(g.Roms))

		for _, r := range g.Roms {
			r.Name = strings.Replace(r.Name, "\\", "/", -1)

			if r.Valid() {
				filteredRoms = append(filteredRoms, r)
			}
		}

		g.Roms = filteredRoms
	}
}

func (d *Dat) NarrowToRom(rom *Rom) *Dat {
	dc := new(Dat)
	dc.Name = d.Name
	dc.Path = d.Path
	dc.Description = d.Description
	dc.FixDat = d.FixDat
	dc.Generation = d.Generation
	dc.UnzipGames = d.UnzipGames

	for _, g := range d.Games {
		gc := new(Game)
		gc.Name = g.Name
		gc.Description = g.Description
		for _, r := range g.Roms {
			if r.HashesMatch(rom) {
				gc.Roms = append(gc.Roms, r)
			}
		}
		if len(gc.Roms) > 0 {
			dc.Games = append(dc.Games, gc)
		}
	}

	if len(dc.Games) > 0 {
		return dc
	}
	return nil
}

func (d *Dat) CopyHeader(src *Dat) {
	d.Name = src.Name
	d.Path = src.Path
	d.Description = src.Description
	d.FixDat = src.FixDat
	d.Generation = src.Generation
	d.UnzipGames = src.UnzipGames
}

func (g *Game) CopyHeader(src *Game) {
	g.Name = src.Name
	g.Description = src.Description
}

func (r *Rom) Valid() bool {
	return !(r.Size > 0 && len(r.Crc) == 0 && len(r.Md5) == 0 && len(r.Sha1) == 0) && r.Status != "nodump"
}
