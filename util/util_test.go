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

package util

import (
	"math"
	"math/rand"
	"testing"
)

func checkUint64ToBytes(t *testing.T, v uint64) {
	buffer := make([]byte, 8)

	Uint64ToBytes(v, buffer)
	ov := BytesToUint64(buffer)

	if v != ov {
		t.Fatalf("expected %d, got %d", v, ov)
	}
}

func TestUint64ToBytes(t *testing.T) {
	v := uint64(math.MaxUint64)
	checkUint64ToBytes(t, v)

	checkUint64ToBytes(t, 23423525)
	checkUint64ToBytes(t, 235463656547)
	checkUint64ToBytes(t, 46235645645665456)

	for a := 0; a < 1000; a++ {
		v = uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
		checkUint64ToBytes(t, v)
	}
}

func checkInt64ToBytes(t *testing.T, v int64) {
	buffer := make([]byte, 8)

	Int64ToBytes(v, buffer)
	ov := BytesToInt64(buffer)

	if v != ov {
		t.Fatalf("expected %d, got %d", v, ov)
	}
}

func TestInt64ToBytes(t *testing.T) {
	v := int64(math.MaxInt64)
	checkInt64ToBytes(t, v)

	checkInt64ToBytes(t, 23423525)
	checkInt64ToBytes(t, -235463656547)
	checkInt64ToBytes(t, 46235645645665456)

	for a := 0; a < 1000; a++ {
		v = rand.Int63()
		checkInt64ToBytes(t, v)
		checkInt64ToBytes(t, -v)
	}
}
