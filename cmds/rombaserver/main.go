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

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"

	"code.google.com/p/gcfg"
	"code.google.com/p/go.net/websocket"
	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"

	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/service"

	_ "expvar"
	_ "github.com/uwedeportivo/romba/db/kivia"
	_ "net/http/pprof"
)

type Config struct {
	General struct {
		LogDir    string
		TmpDir    string
		Workers   int
		Verbosity int
	}

	Depot struct {
		Root    []string
		MaxSize []int64
	}

	Index struct {
		Db   string
		Dats string
	}

	Server struct {
		Port int
	}
}

func main() {
	config := new(Config)

	err := gcfg.ReadFileInto(config, "romba.ini")
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}

	runtime.GOMAXPROCS(config.General.Workers)

	flag.Set("log_dir", config.General.LogDir)

	romDB, err := db.New(config.Index.Db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening db failed: %v\n", err)
		os.Exit(1)
	}

	rs := service.NewRombaService(romDB, config.Index.Dats, config.General.Workers)

	s := rpc.NewServer()
	s.RegisterCodec(json2.NewCustomCodec(&rpc.CompressionSelector{}), "application/json")
	s.RegisterService(rs, "")
	http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir("./web"))))
	http.Handle("/jsonrpc/", s)
	http.Handle("/progress", websocket.Handler(rs.SendProgress))

	fmt.Printf("starting romba server at localhost:%d/romba.html\n", config.Server.Port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", config.Server.Port), nil))
}
