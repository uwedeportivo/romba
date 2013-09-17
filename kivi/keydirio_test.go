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
	"io/ioutil"
	"testing"
)

func TestSaveOpen(t *testing.T) {
	kd := newKeydir(keySizeSha1)

	for i := 0; i < 10000; i++ {
		fileId := int16(i / 100)

		kde := &keydirEntry{
			fileId: fileId,
			vpos:   int32(i),
			vsize:  256,
		}

		key := randomBytes(t, keySizeSha1)

		kd.put(key, kde)
	}

	root, err := ioutil.TempDir("", "kivi_test")
	if err != nil {
		t.Fatalf("cannot open tempdir: %v", err)
	}

	serialId := int64(34234)

	err = saveKeydir(root, kd, serialId)
	if err != nil {
		t.Fatalf("failed to save keydir: %v", err)
	}

	savedKd, err := openKeydir(root, serialId)
	if err != nil {
		t.Fatalf("failed to open keydir: %v", err)
	}

	if savedKd == nil {
		t.Fatalf("nothing read back")
	}

	if savedKd.keySize != kd.keySize {
		t.Fatalf("key size differ: original %d, from file %d", kd.keySize, savedKd.keySize)
	}

	if savedKd.orphaned != kd.orphaned {
		t.Fatalf("orphaned differ: original %d, from file %d", kd.orphaned, savedKd.orphaned)
	}

	if savedKd.size() != kd.size() {
		t.Fatalf("total differ: original %d, from file %d", kd.size(), savedKd.size())
	}

	for k := 0; k < numParts; k++ {
		p := kd.parts[k]

		for key, kdes := range p.mSha1 {
			kde := kdes[0]

			skdes := savedKd.get(key[:])
			if len(skdes) != 1 {
				t.Fatal("keydir entry missing")
			}

			skde := skdes[0]

			if skde.fileId != kde.fileId || skde.vpos != kde.vpos || skde.vsize != kde.vsize {
				t.Fatal("keydir entry differs")
			}
		}
	}
}
