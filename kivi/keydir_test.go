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

package kivi

import (
	"crypto/rand"
	"io"
	"testing"
)

func randomBytes(t *testing.T, keySize int) []byte {
	key := make([]byte, keySize)

	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}

	return key
}

func TestPut(t *testing.T) {
	kd := newKeydir(keySizeSha1)

	kde := &keydirEntry{
		fileId: 23,
		vpos:   20,
		vsize:  30,
	}
	key := randomBytes(t, keySizeSha1)

	kd.put(key, kde)

	kdes := kd.get(key)

	if len(kdes) != 1 {
		t.Fatalf("values wrong length: %d", len(kdes))
	}

	if kdes[0] != kde {
		t.Fatal("wrong value")
	}
}

func TestPutAppend(t *testing.T) {
	kd := newKeydir(keySizeSha1)

	kde1 := &keydirEntry{
		fileId: 23,
		vpos:   20,
		vsize:  30,
	}

	kde2 := &keydirEntry{
		fileId: 46,
		vpos:   40,
		vsize:  60,
	}

	key := randomBytes(t, keySizeSha1)

	kd.put(key, kde1)
	kd.append(key, kde2)

	kdes := kd.get(key)

	if len(kdes) != 2 {
		t.Fatalf("values wrong length: %d", len(kdes))
	}

	if kdes[0] != kde1 && kdes[0] != kde2 {
		t.Fatal("wrong value")
	}

	if kdes[1] != kde1 && kdes[1] != kde2 {
		t.Fatal("wrong value")
	}
}

func TestAppendAppend(t *testing.T) {
	kd := newKeydir(keySizeSha1)

	kde1 := &keydirEntry{
		fileId: 23,
		vpos:   20,
		vsize:  30,
	}

	kde2 := &keydirEntry{
		fileId: 46,
		vpos:   40,
		vsize:  60,
	}

	key := randomBytes(t, keySizeSha1)

	kd.append(key, kde1)
	kd.append(key, kde2)

	kdes := kd.get(key)

	if len(kdes) != 2 {
		t.Fatalf("values wrong length: %d", len(kdes))
	}

	if kdes[0] != kde1 && kdes[0] != kde2 {
		t.Fatal("wrong value")
	}

	if kdes[1] != kde1 && kdes[1] != kde2 {
		t.Fatal("wrong value")
	}
}
