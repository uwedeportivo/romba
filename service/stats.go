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
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/dustin/go-humanize"
	"github.com/uwedeportivo/commander"
)

func (rs *RombaService) memstats(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	debug.FreeOSMemory()

	s := new(runtime.MemStats)
	runtime.ReadMemStats(s)

	fmt.Fprintf(cmd.Stdout, "\n# runtime.MemStats\n")
	fmt.Fprintf(cmd.Stdout, "# Alloc = %s\n", humanize.Bytes(s.Alloc))
	fmt.Fprintf(cmd.Stdout, "# TotalAlloc = %s\n", humanize.Bytes(s.TotalAlloc))
	fmt.Fprintf(cmd.Stdout, "# Sys = %s\n", humanize.Bytes(s.Sys))
	fmt.Fprintf(cmd.Stdout, "# Lookups = %d\n", s.Lookups)
	fmt.Fprintf(cmd.Stdout, "# Mallocs = %d\n", s.Mallocs)
	fmt.Fprintf(cmd.Stdout, "# Frees = %d\n", s.Frees)

	fmt.Fprintf(cmd.Stdout, "# HeapAlloc = %s\n", humanize.Bytes(s.HeapAlloc))
	fmt.Fprintf(cmd.Stdout, "# HeapSys = %s\n", humanize.Bytes(s.HeapSys))
	fmt.Fprintf(cmd.Stdout, "# HeapIdle = %s\n", humanize.Bytes(s.HeapIdle))
	fmt.Fprintf(cmd.Stdout, "# HeapInuse = %s\n", humanize.Bytes(s.HeapInuse))
	fmt.Fprintf(cmd.Stdout, "# HeapReleased = %s\n", humanize.Bytes(s.HeapReleased))
	fmt.Fprintf(cmd.Stdout, "# HeapObjects = %d\n", s.HeapObjects)

	fmt.Fprintf(cmd.Stdout, "# Stack = %d / %d\n", s.StackInuse, s.StackSys)
	fmt.Fprintf(cmd.Stdout, "# MSpan = %d / %d\n", s.MSpanInuse, s.MSpanSys)
	fmt.Fprintf(cmd.Stdout, "# MCache = %d / %d\n", s.MCacheInuse, s.MCacheSys)
	fmt.Fprintf(cmd.Stdout, "# BuckHashSys = %d\n", s.BuckHashSys)

	fmt.Fprintf(cmd.Stdout, "# NextGC = %d\n", s.NextGC)
	fmt.Fprintf(cmd.Stdout, "# PauseNs = %d\n", s.PauseNs)
	fmt.Fprintf(cmd.Stdout, "# NumGC = %d\n", s.NumGC)
	fmt.Fprintf(cmd.Stdout, "# EnableGC = %v\n", s.EnableGC)
	fmt.Fprintf(cmd.Stdout, "# DebugGC = %v\n", s.DebugGC)

	return nil
}

func (rs *RombaService) dbstats(cmd *commander.Command, args []string) error {
	rs.jobMutex.Lock()
	defer rs.jobMutex.Unlock()

	fmt.Fprintf(cmd.Stdout, "dbstats = %s", rs.romDB.PrintStats())
	return nil
}
