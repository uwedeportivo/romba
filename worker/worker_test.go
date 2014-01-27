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

package worker

import (
	"testing"
)

func executeTestCommonRoot(pa, pb, expected string, t *testing.T) {
	c := commonRoot(pa, pb)

	if c != expected {
		t.Fatalf("expected = %s, got = %s;     a = %s, b = %s", expected, c, pa, pb)
	}
}

func TestCommonRoot(t *testing.T) {
	executeTestCommonRoot("/a/b/c/d/e/f", "/a/b/v", "/a/b", t)
	executeTestCommonRoot("/a/b/v", "/a/b/c/d/e/f", "/a/b", t)
	executeTestCommonRoot("/a", "/b", "/", t)
	executeTestCommonRoot("/a/b", "/a/b", "/a/b", t)
	executeTestCommonRoot("/a/b/c", "/a/b/d", "/a/b", t)
	executeTestCommonRoot("/a", "/a", "/a", t)
	executeTestCommonRoot("/a/c", "/a/b", "/a", t)
	executeTestCommonRoot("/a/b/c", "/a/b", "/a/b", t)
	executeTestCommonRoot("/a/b", "/a/b/c", "/a/b", t)
	executeTestCommonRoot("/a/b/v/", "/a/b/v", "/a/b/v", t)
	executeTestCommonRoot("/a", "/", "/", t)
	executeTestCommonRoot("/", "", "", t)
	executeTestCommonRoot("/Users/uwe/romba/dats/AgeMAME/AgeMameRoms.dat", "/Users/uwe/romba/dats/AgeMAME",
		"/Users/uwe/romba/dats/AgeMAME", t)
}
