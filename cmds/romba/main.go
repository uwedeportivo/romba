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
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/rpc/v2/json2"
)

type Reply struct {
	Message string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "not enough arguments\n")
		os.Exit(1)
	}

	serverStr := os.Args[1]

	params := make(map[string]string)
	params["cmdTxt"] = strings.Join(os.Args[2:], " ")
	params["cmdOrigin"] = "terminal"

	buf, err := json2.EncodeClientRequest("RombaService.Execute", params)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to encode json2 client request: %v\n", err)
		os.Exit(1)
	}
	body := bytes.NewBuffer(buf)
	resp, err := http.Post("http://"+serverStr+"/jsonrpc/", "application/json", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to issue client request: %v\n", err)
		os.Exit(1)
	}

	reply := new(Reply)
	err = json2.DecodeClientResponse(resp.Body, reply)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to decode response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s\n", reply.Message)
}
