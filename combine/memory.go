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

package combine

import (
	"encoding/hex"
	"github.com/golang/glog"
	"sync"

	"github.com/uwedeportivo/romba/types"
)

type memoryCombiner struct {
	sha1s  map[string]*types.Rom

	mutex sync.Mutex
}

func NewMemoryCombiner() Combiner {
	return &memoryCombiner{
		sha1s: make(map[string]*types.Rom),
	}
}

func (mc *memoryCombiner) Declare(rom *types.Rom) error {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	glog.V(4).Infof("combining rom %s", rom.Name)

	if rom.Sha1 != nil {

		seenRom, ok := mc.sha1s[string(rom.Sha1)]
		if !ok {
			seenRom = new(types.Rom)
			seenRom.Copy(rom)
			mc.sha1s[string(rom.Sha1)] = seenRom
		}

		if rom.Crc != nil {
			glog.V(4).Infof("declaring crc %s -> sha1 %s mapping", hex.EncodeToString(rom.Crc), hex.EncodeToString(rom.Sha1))

			seenRom.Crc = rom.Crc
		}
		if rom.Md5 != nil {
			glog.V(4).Infof("declaring md5 %s -> sha1 %s mapping", hex.EncodeToString(rom.Md5), hex.EncodeToString(rom.Sha1))

			seenRom.Md5 = rom.Md5
		}
	} else {
		glog.V(4).Infof("combining rom %s with missing SHA1", rom.Name)
	}

	return nil
}

func (mc *memoryCombiner) ForEachRom(romF func(rom *types.Rom) error) error {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	for _, rom := range mc.sha1s {
		err := romF(rom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mc *memoryCombiner) Close() error {
	return nil
}
