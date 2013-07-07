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
)

type Dat struct {
	Name        string    `xml:"header>name"`
	Description string    `xml:"header>description"`
	Games       GameSlice `xml:"game"`
	Generation  int64
	Path        string
	Software    GameSlice `xml:"software"`
}

type Game struct {
	Name        string   `xml:"name,attr"`
	Description string   `xml:"description"`
	Roms        RomSlice `xml:"rom"`
	Disks       RomSlice `xml:"disk"`
	Parts       RomSlice `xml:"part>dataarea>rom"`
	Regions     RomSlice `xml:"region>rom"`
}

type GameSlice []*Game

type Rom struct {
	Name string `xml:"name,attr"`
	Size int    `xml:"size,attr"`
	Crc  []byte `xml:"crc,attr"`
	Md5  []byte `xml:"md5,attr"`
	Sha1 []byte `xml:"sha1,attr"`
}

type RomSlice []*Rom

func (ar *Rom) Equals(br *Rom) bool {
	if ar.Name != br.Name {
		return false
	}

	if ar.Size != br.Size {
		return false
	}

	if !bytes.Equal(ar.Crc, br.Crc) {
		return false
	}

	if !bytes.Equal(ar.Md5, br.Md5) {
		return false
	}

	if !bytes.Equal(ar.Sha1, br.Sha1) {
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
	return true
}

func (d *Dat) Normalize() {
	if d.Software != nil {
		d.Games = append(d.Games, d.Software...)
		d.Software = nil
	}
	sort.Sort(d.Games)

	for _, g := range d.Games {
		if g.Disks != nil {
			g.Roms = append(g.Roms, g.Disks...)
			g.Disks = nil
		}
		if g.Parts != nil {
			g.Roms = append(g.Roms, g.Parts...)
			g.Parts = nil
		}
		if g.Regions != nil {
			g.Roms = append(g.Roms, g.Regions...)
			g.Regions = nil
		}
		sort.Sort(g.Roms)
	}
}
