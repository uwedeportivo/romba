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
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"

	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/scalingdata/gcfg"
	"golang.org/x/net/websocket"

	"github.com/golang/glog"

	"github.com/uwedeportivo/romba/archive"
	"github.com/uwedeportivo/romba/config"
	"github.com/uwedeportivo/romba/db"
	"github.com/uwedeportivo/romba/service"

	_ "expvar"
	_ "net/http/pprof"

	_ "github.com/uwedeportivo/romba/db/clevel"
)

func signalCatcher(rs *service.RombaService) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	<-ch
	glog.Info("CTRL-C; exiting")

	err := rs.ShutDown()
	if err != nil {
		glog.Errorf("error shutting down: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func findINI() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", err
	}
	path := "romba.ini"
	exists, err := archive.PathExists(path)
	if err != nil {
		return "", err
	}
	if exists {
		return path, nil
	}

	path = filepath.Join(u.HomeDir, ".romba", "romba.ini")
	exists, err = archive.PathExists(path)
	if err != nil {
		return "", err
	}
	if exists {
		return path, nil
	}
	return "", fmt.Errorf("couldn't find romba.ini")
}

func main() {
	cfg := new(config.Config)

	iniPath, err := findINI()
	if err != nil {
		fmt.Fprintf(os.Stderr, "finding romba ini failed: %v\n", err)
		os.Exit(1)
	}

	err = gcfg.ReadFileInto(cfg, iniPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini from %s failed: %v\n", iniPath, err)
		os.Exit(1)
	}

	for i := 0; i < len(cfg.Depot.MaxSize); i++ {
		cfg.Depot.MaxSize[i] *= int64(archive.GB)
	}

	cfg.General.LogDir, err = filepath.Abs(cfg.General.LogDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}
	cfg.General.TmpDir, err = filepath.Abs(cfg.General.TmpDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}
	for i, pv := range cfg.Depot.Root {
		cfg.Depot.Root[i], err = filepath.Abs(pv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
			os.Exit(1)
		}
	}
	cfg.Index.Dats, err = filepath.Abs(cfg.Index.Dats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}
	cfg.Index.Db, err = filepath.Abs(cfg.Index.Db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}
	cfg.General.WebDir, err = filepath.Abs(cfg.General.WebDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}
	cfg.General.BadDir, err = filepath.Abs(cfg.General.BadDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading romba ini failed: %v\n", err)
		os.Exit(1)
	}

	config.GlobalConfig = cfg

	runtime.GOMAXPROCS(cfg.General.Cores)

	flag.Set("log_dir", cfg.General.LogDir)
	flag.Set("alsologtostderr", "true")
	flag.Set("v", strconv.Itoa(cfg.General.Verbosity))

	flag.Parse()

	romDB, err := db.New(cfg.Index.Db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening db failed: %v\n", err)
		os.Exit(1)
	}

	depot, err := archive.NewDepot(cfg.Depot.Root, cfg.Depot.MaxSize, romDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating depot failed: %v\n", err)
		os.Exit(1)
	}

	rs := service.NewRombaService(romDB, depot, cfg)

	go signalCatcher(rs)

	s := rpc.NewServer()
	s.RegisterCodec(json2.NewCustomCodec(&rpc.CompressionSelector{}), "application/json")
	s.RegisterService(rs, "")
	http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(cfg.General.WebDir))))
	http.Handle("/jsonrpc/", s)
	http.Handle("/progress", websocket.Handler(rs.SendProgress))

	fmt.Printf("starting romba server version %s at localhost:%d/romba.html\n", service.Version, cfg.Server.Port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port), nil))
}
