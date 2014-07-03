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

package dedup

import "github.com/uwedeportivo/romba/types"

type Deduper interface {
	Declare(rom *types.Rom) error
	Seen(rom *types.Rom) (bool, error)
	Close() error
}

func Declare(d *types.Dat, deduper Deduper) error {
	for _, g := range d.Games {
		for _, r := range g.Roms {
			if r.Valid() {
				err := deduper.Declare(r)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func Dedup(d *types.Dat, deduper Deduper) (*types.Dat, error) {
	dc := new(types.Dat)
	dc.CopyHeader(d)

	for _, g := range d.Games {
		gc := new(types.Game)
		gc.CopyHeader(g)
		for _, r := range g.Roms {
			if !r.Valid() {
				continue
			}
			seen, err := deduper.Seen(r)
			if err != nil {
				return nil, err
			}
			if !seen {
				gc.Roms = append(gc.Roms, r)
				err = deduper.Declare(r)
				if err != nil {
					return nil, err
				}
			}
		}
		if len(gc.Roms) > 0 {
			dc.Games = append(dc.Games, gc)
		}
	}

	if len(dc.Games) > 0 {
		return dc, nil
	}
	return nil, nil
}
