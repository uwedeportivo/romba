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
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"strings"

	"github.com/uwedeportivo/commander"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/util"
	"github.com/uwedeportivo/romba/worker"
)

func (rs *RombaService) lookupRom(cmd *commander.Command, r *types.Rom, outpath string) error {
	croms, err := rs.romDB.CompleteRom(r)
	if err != nil {
		return err
	}

	if r.Sha1 != nil {
		sha1Str := hex.EncodeToString(r.Sha1)

		inDepot, hh, rompath, size, err := rs.depot.SHA1InDepot(sha1Str)
		if err != nil {
			return err
		}

		if inDepot {
			fmt.Fprintf(cmd.Stdout, "-----------------\n")
			fmt.Fprintf(cmd.Stdout, "rom file %s in depot\n", rompath)
			fmt.Fprintf(cmd.Stdout, "crc = %s\n", hex.EncodeToString(hh.Crc))
			fmt.Fprintf(cmd.Stdout, "md5 = %s\n", hex.EncodeToString(hh.Md5))
			fmt.Fprintf(cmd.Stdout, "size = %d\n", size)
			r.Crc = hh.Crc
			r.Md5 = hh.Md5

			if outpath != "" {
				worker.Cp(rompath, filepath.Join(outpath, filepath.Base(rompath)))
			}
		}
	}

	for _, crom := range croms {
		sha1Str := hex.EncodeToString(crom.Sha1)

		inDepot, hh, rompath, size, err := rs.depot.SHA1InDepot(sha1Str)
		if err != nil {
			return err
		}

		if inDepot {
			fmt.Fprintf(cmd.Stdout, "-----------------\n")
			fmt.Fprintf(cmd.Stdout, "collision rom file %s in depot\n", rompath)
			fmt.Fprintf(cmd.Stdout, "crc = %s\n", hex.EncodeToString(hh.Crc))
			fmt.Fprintf(cmd.Stdout, "md5 = %s\n", hex.EncodeToString(hh.Md5))
			fmt.Fprintf(cmd.Stdout, "size = %d\n", size)
			crom.Crc = hh.Crc
			crom.Md5 = hh.Md5

			if outpath != "" {
				worker.Cp(rompath, filepath.Join(outpath, filepath.Base(rompath)))
			}
		}
	}

	dats, err := rs.romDB.DatsForRom(r)
	if err != nil {
		return err
	}

	if len(dats) > 0 {
		fmt.Fprintf(cmd.Stdout, "-----------------\n")
		fmt.Fprintf(cmd.Stdout, "rom found in:\n")
		for _, dat := range dats {
			dn := dat.NarrowToRom(r)
			if dn != nil {
				fmt.Fprintf(cmd.Stdout, "%s\n", types.PrintDat(dn))
			}
		}
	}
	return nil
}

func (rs *RombaService) lookup(cmd *commander.Command, args []string) error {
	size := cmd.Flag.Lookup("size").Value.Get().(int64)
	outpath := cmd.Flag.Lookup("out").Value.Get().(string)

	for _, arg := range args {
		fmt.Fprintf(cmd.Stdout, "----------------------------------------\n")
		fmt.Fprintf(cmd.Stdout, "key: %s\n", arg)

		if strings.HasPrefix(arg, "0x") {
			arg = arg[2:]
		}

		hash, err := hex.DecodeString(arg)
		if err != nil {
			return err
		}

		if len(hash) == sha1.Size {
			dat, err := rs.romDB.GetDat(hash)
			if err != nil {
				return err
			}

			if dat != nil {
				fmt.Fprintf(cmd.Stdout, "-----------------\n")
				fmt.Fprintf(cmd.Stdout, "dat with sha1 %s = %s\n", arg, types.PrintShortDat(dat))
			}
		}

		if size != -1 || len(hash) == sha1.Size {
			r := new(types.Rom)
			r.Size = size
			switch len(hash) {
			case md5.Size:
				r.Md5 = hash
			case crc32.Size:
				r.Crc = hash
			case sha1.Size:
				r.Sha1 = hash
			default:
				return fmt.Errorf("found unknown hash size: %d", len(hash))
			}

			err = rs.lookupRom(cmd, r, outpath)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.Stdout, "-----------------\n")
			fmt.Fprintf(cmd.Stdout, "DebugGet:\n%s\n", rs.romDB.DebugGet(hash, size))

			fmt.Fprintf(cmd.Stdout, "-----------------\n")
			if len(hash) == sha1.Size {
				fmt.Fprintf(cmd.Stdout, "bloom filter hits: %v", rs.depot.DebugBloom(arg))
			}
		} else {
			suffixes, err := rs.romDB.ResolveHash(hash)
			if err != nil {
				return err
			}

			for i := 0; i < len(suffixes); i += sha1.Size + 8 {
				r := new(types.Rom)
				r.Size = util.BytesToInt64(suffixes[i : i+8])
				switch len(hash) {
				case md5.Size:
					r.Md5 = hash
				case crc32.Size:
					r.Crc = hash
				default:
					return fmt.Errorf("found unknown hash size: %d", len(hash))
				}
				r.Sha1 = suffixes[i+8 : i+8+sha1.Size]

				err = rs.lookupRom(cmd, r, outpath)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
