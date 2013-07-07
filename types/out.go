// Copyright (c) 2013 Uwe Hoffmann. All rights reserved.

package types

import (
	"bytes"
	"encoding/hex"
	"text/template"
)

const datTemplate = `
dat (
	name "{{.Name}}"
	description "{{.Description}}"
	path "{{.Path}}"
	generation {{.Generation}}
)
{{with .Games}}{{range .}}
game (
	name "{{.Name}}"
	description "{{.Description}}"
	{{with .Roms}}{{range .}}rom ( name "{{.Name}}" size {{.Size}} crc {{hex .Crc}} md5 {{hex .Md5}} sha1 {{hex .Sha1}} ){{end}}{{end}}
){{end}}{{end}}
`

var ff = template.FuncMap{
	"hex": hex.EncodeToString,
}

var dt = template.Must(template.New("datout").Funcs(ff).Parse(datTemplate))

func PrintDat(d *Dat) []byte {
	buf := new(bytes.Buffer)

	err := dt.Execute(buf, d)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}
