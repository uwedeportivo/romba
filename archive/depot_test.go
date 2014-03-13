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

package archive

import (
	"testing"
)

func TestExtractResumePoint(t *testing.T) {
	expectedResumePoint := "/mnt/roms/3/Official US PlayStation Magazine - Volume 3 Issue 1 (1999-10)(Ziff Davis)(US).zip"
	resumePath := "testdata/resume.log"

	resumePoint, err := extractResumePoint(resumePath, 5)
	if err != nil {
		t.Errorf("extracting resume point from %s failed: %v", resumePath, err)
	}

	if resumePoint != expectedResumePoint {
		t.Errorf("expected resume point %s, got %s", expectedResumePoint, resumePoint)
	}
}

func TestShortResumePoint(t *testing.T) {
	expectedResumePoint := "/mnt/roms/4/NAM-1975 (1994)(SNK)(JP-US)[!].zip"
	resumePath := "testdata/resume2.log"

	resumePoint, err := extractResumePoint(resumePath, 5)
	if err != nil {
		t.Errorf("extracting resume point from %s failed: %v", resumePath, err)
	}

	if resumePoint != expectedResumePoint {
		t.Errorf("expected resume point %s, got %s", expectedResumePoint, resumePoint)
	}
}
