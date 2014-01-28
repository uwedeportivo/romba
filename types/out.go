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

package types

import (
	"bytes"
	"encoding/hex"
	"io"
	"text/template"
)

const datTemplate = `
dat (
	name "{{.Name}}"
	description "{{.Description}}"
	path "{{.Path}}"
)
{{with .Games}}{{range .}}
game (
	name "{{.Name}}"
	description "{{.Description}}"
	{{with .Roms}}{{range .}}
	rom ( name "{{.Name}}" size {{.Size}}{{hexcrc .Crc}}{{hexmd5 .Md5}}{{hexsha1 .Sha1}}){{end}}{{end}}
){{end}}{{end}}
`

const compliantDatTemplate = `clrmamepro (
	name "{{.Name}}"
	description "{{.Description}}"
){{with .Games}}{{range .}}
game (
	name "{{.Name}}"
	description "{{.Description}}"
	{{with .Roms}}{{range .}}
	rom ( name "{{.Name}}" size {{.Size}}{{hexcrc .Crc}}{{hexmd5 .Md5}}{{hexsha1 .Sha1}}){{end}}{{end}}
){{end}}{{end}}
`

const datShortTemplate = `
dat (
	name "{{.Name}}"
	description "{{.Description}}"
	path "{{.Path}}"
)
{{with .Games}}{{range .}}
game (
	name "{{.Name}}"
	description "{{.Description}}"
){{end}}{{end}}
`
const datsTemplate = `
{{range .}}
dat (
	name "{{.Name}}"
	description "{{.Description}}"
	path "{{.Path}}"
)
{{end}}
`

func hexstr(which string, bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	return " " + which + " " + hex.EncodeToString(bs)
}

func crcstr(bs []byte) string {
	return hexstr("crc", bs)
}

func md5str(bs []byte) string {
	return hexstr("md5", bs)
}

func sha1str(bs []byte) string {
	return hexstr("sha1", bs)
}

var ff = template.FuncMap{
	"hexcrc":  crcstr,
	"hexmd5":  md5str,
	"hexsha1": sha1str,
}

var dt = template.Must(template.New("datout").Funcs(ff).Parse(datTemplate))
var cdt = template.Must(template.New("compliantdatout").Funcs(ff).Parse(compliantDatTemplate))
var sdt = template.Must(template.New("datshortout").Funcs(ff).Parse(datShortTemplate))
var dts = template.Must(template.New("datsout").Funcs(ff).Parse(datsTemplate))

func PrintDat(d *Dat) []byte {
	buf := new(bytes.Buffer)

	err := dt.Execute(buf, d)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func ComposeDat(d *Dat, w io.Writer) error {
	return dt.Execute(w, d)
}

func PrintCompliantDat(d *Dat) []byte {
	buf := new(bytes.Buffer)

	err := cdt.Execute(buf, d)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func ComposeCompliantDat(d *Dat, w io.Writer) error {
	return cdt.Execute(w, d)
}

func PrintShortDat(d *Dat) []byte {
	buf := new(bytes.Buffer)

	err := sdt.Execute(buf, d)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func PrintRomInDats(dats []*Dat) []byte {
	buf := new(bytes.Buffer)

	err := dts.Execute(buf, dats)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}
