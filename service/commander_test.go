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
	"strings"
	"testing"
)

func doTest(testData []string, t *testing.T) {
	testResult, err := splitIntoArgs(strings.Join(testData, " "))
	if err != nil {
		t.Fatalf("failed to split: %v", err)
	}

	if len(testData) != len(testResult) {
		t.Fatalf("expected (%d) and actual (%d) lengths differ", len(testData), len(testResult))
	}

	for k, v := range testData {
		if strings.HasPrefix(v, "'") {
			v = v[1 : len(v)-1]
		}
		if testResult[k] != v {
			t.Fatalf("expected (%s) and actual (%s) values differ", v, testResult[k])
		}
	}
}

var testCases [][]string = [][]string{
	[]string{
		"abc",
		"cdefg",
		"hijk",
	},
	[]string{
		"abc",
		"'foo bar   tar'",
		"hi\\ there",
	},
	[]string{
		"lonely",
	},
	[]string{
		"one",
		"two",
		"they\\'re",
		"'three and four'",
		"five\\ and\\ six",
	},
}

func TestSplits(t *testing.T) {
	for _, testData := range testCases {
		doTest(testData, t)
	}
}

func TestArchiveResume(t *testing.T) {
	argLine := "archive -resume='/home/thejay/romba/logs/archive-resume-2014-03-13-12_08_32.log' '/mnt/roms/4'"

	rr, err := splitIntoArgs(argLine)
	if err != nil {
		t.Fatalf("failed to split: %v", err)
	}

	for i, v := range rr {
		t.Logf("rr[%d]=%s", i, v)
	}
}

func TestOpenQuote(t *testing.T) {
	testData := "one two 'foo bar   "

	testResult, err := splitIntoArgs(testData)
	if err == nil {
		t.Fatalf("didn't detect open quote: %s", strings.Join(testResult, "~"))
	}
}
