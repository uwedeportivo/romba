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

package parser

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/spacemonkeygo/errors"

	"github.com/golang/glog"
	"github.com/uwedeportivo/romba/types"
)

const (
	xmlPrefix        = "<?xml"
	xmlPrefixWithBOM = "\xef\xbb\xbf<?xml"
)

type parser struct {
	ll *lexer
	d  *types.Dat
	pl ParseListener
}

var (
	ParseError    = errors.NewClass("DAT Parse Error")
	XMLParseError = errors.NewClass("XML DAT Parse Error")

	lineNumberErrorKey = errors.GenSym()
	filePathErrorKey   = errors.GenSym()
)

func ErrorLineNumber(err error) int {
	v, ok := errors.GetData(err, lineNumberErrorKey).(int)
	if !ok {
		return -1
	}
	return v
}

func ErrorFilePath(err error) string {
	v, ok := errors.GetData(err, filePathErrorKey).(string)
	if !ok {
		return ""
	}
	return v
}

func setErrorLineNumber(lnr int) errors.ErrorOption {
	return errors.SetData(lineNumberErrorKey, lnr)
}

func setErrorFilePath(path string) errors.ErrorOption {
	return errors.SetData(filePathErrorKey, path)
}

func (p *parser) consumeStringValue() (string, error) {
	i := p.ll.nextItem()
	switch {
	case i.typ == itemQuotedString:
		return i.val[1 : len(i.val)-1], nil
	case i.typ == itemValue:
		return i.val, nil
	case i.typ > itemValue:
		return i.val, nil
	default:
		return "", fmt.Errorf("expected quoted string or value, got %v", i)
	}
}

func stringValue2Int(input string) (int64, error) {
	if input == "-" {
		return 0, nil
	}
	return strconv.ParseInt(input, 10, 64)
}

func stringValue2Bool(input string) (bool, error) {
	if input == "-" {
		return false, nil
	}
	val, err := strconv.ParseBool(input)
	if err != nil {
		input = strings.ToLower(input)
		switch input {
		case "yes":
			return true, nil
		case "no":
			return false, nil
		default:
			return false, err
		}
	}
	return val, nil
}

func stringValue2Forcezipping(input string) (bool, error) {
	if input == "-" {
		return false, nil
	}
	val, err := strconv.ParseBool(input)
	if err != nil {
		input = strings.ToLower(input)
		switch input {
		case "yes":
			return true, nil
		case "no":
			return false, nil
		case "zip":
			return true, nil
		case "unzip":
			return false, nil
		default:
			return false, err
		}
	}
	return val, nil
}

func stringValue2Bytes(input string, expectedLength int) ([]byte, error) {
	if input == "-" || input == "" {
		return nil, nil
	}

	input = strings.TrimSpace(input)

	if strings.HasPrefix(input, "0x") {
		input = input[2:]
	}

	if len(input) < expectedLength {
		input = strings.Repeat("0", expectedLength-len(input)) + input
	}

	return hex.DecodeString(input)
}

func (p *parser) consumeIntegerValue() (int64, error) {
	i := p.ll.nextItem()
	if i.typ == itemValue {
		return stringValue2Int(i.val)
	}
	if i.typ == itemQuotedString {
		return stringValue2Int(i.val[1 : len(i.val)-1])
	}
	return 0, fmt.Errorf("expected value, got %v", i)
}

func (p *parser) consumeHexBytes(expectedLength int) ([]byte, error) {
	i := p.ll.nextItem()
	if i.typ == itemValue {
		return stringValue2Bytes(i.val, expectedLength)
	}
	if i.typ == itemQuotedString {
		return stringValue2Bytes(i.val[1:len(i.val)-1], expectedLength)
	}
	return nil, fmt.Errorf("expected value, got %v", i)
}

func (p *parser) consumeBoolValue() (bool, error) {
	i := p.ll.nextItem()
	if i.typ == itemValue {
		return stringValue2Bool(i.val)
	}
	if i.typ == itemQuotedString {
		return stringValue2Bool(i.val[1 : len(i.val)-1])
	}
	return false, fmt.Errorf("expected value, got %v", i)
}

func (p *parser) consumeForceZipping() (bool, error) {
	i := p.ll.nextItem()
	if i.typ == itemValue {
		return stringValue2Forcezipping(i.val)
	}
	if i.typ == itemQuotedString {
		return stringValue2Forcezipping(i.val[1 : len(i.val)-1])
	}
	return false, fmt.Errorf("expected value, got %v", i)
}

func (p *parser) datStmt() error {
	i := p.ll.nextItem()
	err := p.match(i, itemOpenBrace)
	if err != nil {
		return err
	}

	for i = p.ll.nextItem(); i.typ != itemCloseBrace && i.typ != itemEOF && i.typ != itemError; i = p.ll.nextItem() {
		switch {
		case i.typ == itemName:
			p.d.Name, err = p.consumeStringValue()
			if err != nil {
				return err
			}
		case i.typ == itemDescription:
			p.d.Description, err = p.consumeStringValue()
			if err != nil {
				return err
			}
		case i.typ == itemForceZipping || i.typ == itemForcePacking:
			bv, err := p.consumeForceZipping()
			if err != nil {
				return err
			}
			p.d.UnzipGames = !bv
		}
	}

	if i.typ == itemEOF {
		return fmt.Errorf("unexpected end of input")
	}
	if i.typ == itemError {
		return lexError(i)
	}
	return nil
}

func lexError(i item) error {
	return fmt.Errorf("lexer error: %v", i)
}

func (p *parser) gameStmt() (*types.Game, error) {
	i := p.ll.nextItem()
	err := p.match(i, itemOpenBrace)
	if err != nil {
		return nil, err
	}

	g := &types.Game{}

	for i = p.ll.nextItem(); i.typ != itemCloseBrace && i.typ != itemEOF && i.typ != itemError; i = p.ll.nextItem() {
		switch {
		case i.typ == itemName:
			g.Name, err = p.consumeStringValue()
			if err != nil {
				return nil, err
			}
		case i.typ == itemDescription:
			g.Description, err = p.consumeStringValue()
			if err != nil {
				return nil, err
			}
		case i.typ == itemRom:
			r, err := p.romStmt()
			if err != nil {
				return nil, err
			}

			if r != nil {
				g.Roms = append(g.Roms, r)

				if r.Sha1 == nil {
					p.d.MissingSha1s = true
				}
			}
		}
	}

	if i.typ == itemEOF {
		return nil, fmt.Errorf("unexpected end of input")
	}
	if i.typ == itemError {
		return nil, lexError(i)
	}
	return g, nil
}

func (p *parser) romStmt() (*types.Rom, error) {
	i := p.ll.nextItem()
	err := p.match(i, itemOpenBrace)
	if err != nil {
		return nil, err
	}

	r := &types.Rom{}

	for i = p.ll.nextItem(); i.typ != itemCloseBrace && i.typ != itemEOF && i.typ != itemError; i = p.ll.nextItem() {
		switch {
		case i.typ == itemName:
			r.Name, err = p.consumeStringValue()
			if err != nil {
				return nil, err
			}
		case i.typ == itemFlags:
			r.Status, err = p.consumeStringValue()
			if err != nil {
				return nil, err
			}
		case i.typ == itemSize:
			r.Size, err = p.consumeIntegerValue()
			if err != nil {
				return nil, err
			}
		case i.typ == itemMd5:
			r.Md5, err = p.consumeHexBytes(32)
			if err != nil {
				glog.Errorf("failed to decode md5 for rom %s in file %s: %v", r.Name, p.ll.name, err)
				return nil, nil
			}
		case i.typ == itemCrc:
			r.Crc, err = p.consumeHexBytes(8)
			if err != nil {
				glog.Errorf("failed to decode crc for rom %s in file %s: %v", r.Name, p.ll.name, err)
				return nil, nil
			}
		case i.typ == itemSha1:
			r.Sha1, err = p.consumeHexBytes(40)
			if err != nil {
				glog.Errorf("failed to decode sha1 for rom %s in file %s: %v", r.Name, p.ll.name, err)
				return nil, nil
			}
		}
	}

	if i.typ == itemEOF {
		return nil, fmt.Errorf("unexpected end of input")
	}
	if i.typ == itemError {
		return nil, lexError(i)
	}
	return r, nil
}

func (p *parser) parse() error {
	var i item

	for i = p.ll.nextItem(); i.typ != itemEOF && i.typ != itemError; i = p.ll.nextItem() {
		switch {
		case i.typ == itemClrMamePro:
			err := p.datStmt()
			if err != nil {
				return err
			}
			if p.pl != nil {
				p.d.Normalize()
				err = p.pl.ParsedDatStmt(p.d)
				if err != nil {
					return err
				}
			}
		case i.typ == itemGame:
			g, err := p.gameStmt()
			if err != nil {
				return err
			}
			if g != nil {
				if p.pl != nil {
					g.Normalize()
					err = p.pl.ParsedGameStmt(g)
					if err != nil {
						return err
					}
				} else {
					p.d.Games = append(p.d.Games, g)
				}
			}
		}
	}
	if i.typ == itemError {
		return lexError(i)
	}
	return nil
}

func (p *parser) match(i item, typ itemType) error {
	if i.typ == typ {
		return nil
	}
	return fmt.Errorf("expected token of type %v, got %v instead", typ, i)
}

type ParseListener interface {
	ParsedDatStmt(dat *types.Dat) error
	ParsedGameStmt(game *types.Game) error
}

func ParseDatWithListener(r io.Reader, path string, pl ParseListener) ([]byte, error) {
	hr := hashingReader{
		ir: r,
		h:  sha1.New(),
	}

	ll, err := lex("dat - "+path, hr)
	if err != nil {
		return nil, err
	}

	p := &parser{
		ll: ll,
		d:  &types.Dat{},
		pl: pl,
	}

	p.d.Path = path
	err = p.parse()
	if err != nil {
		derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, p.ll.lineNumber(), err)
		derr := ParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(p.ll.lineNumber()))
		return nil, derr
	}
	return hr.h.Sum(nil), nil
}

func ParseDat(r io.Reader, path string) (*types.Dat, []byte, error) {
	hr := hashingReader{
		ir: r,
		h:  sha1.New(),
	}

	ll, err := lex("dat - "+path, hr)
	if err != nil {
		return nil, nil, err
	}

	p := &parser{
		ll: ll,
		d:  &types.Dat{},
	}

	p.d.Path = path
	err = p.parse()
	if err != nil {
		derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, p.ll.lineNumber(), err)
		derr := ParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(p.ll.lineNumber()))
		return nil, nil, derr
	}
	p.d.Normalize()
	return p.d, hr.h.Sum(nil), nil
}

type hashingReader struct {
	ir io.Reader
	h  hash.Hash
}

func (r hashingReader) Read(buf []byte) (int, error) {
	n, err := r.ir.Read(buf)
	if err == nil {
		r.h.Write(buf[:n])
	}
	return n, err
}

type lineCountingReader struct {
	ir   io.Reader
	line int
}

func (r lineCountingReader) Read(buf []byte) (int, error) {
	n, err := r.ir.Read(buf)
	if err == nil {
		for _, b := range buf[:n] {
			if b == '\n' {
				r.line++
			}
		}
	}
	return n, err
}

func isXML(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			glog.Errorf("error, failed to close file %s: %v", path, err)
		}
	}()

	lr := io.LimitedReader{
		R: file,
		N: 21,
	}

	snippet, err := ioutil.ReadAll(&lr)
	if err != nil {
		return false, err
	}

	ss := string(snippet)

	return strings.HasPrefix(ss, xmlPrefix) || strings.HasPrefix(ss, xmlPrefixWithBOM), nil
}

func Parse(path string) (*types.Dat, []byte, error) {
	isXML, err := isXML(path)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			glog.Errorf("error, failed to close file %s: %v", path, err)
		}
	}()

	if isXML {
		return ParseXml(file, path)
	}
	return ParseDat(file, path)
}

func ParseWithListener(path string, pl ParseListener) ([]byte, error) {
	isXML, err := isXML(path)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			glog.Errorf("error, failed to close file %s: %v", path, err)
		}
	}()

	if isXML {
		return ParseXmlWithListener(file, path, pl)
	}
	return ParseDatWithListener(file, path, pl)
}

func fixHashes(rom *types.Rom) {
	if rom.Crc != nil {
		strV := string(rom.Crc)
		if strV != "" {
			v, err := hex.DecodeString(string(rom.Crc))
			if err != nil {
				rom.Crc = nil
			}
			rom.Crc = v
		} else {
			rom.Crc = nil
		}
	}
	if rom.Md5 != nil {
		strV := string(rom.Md5)
		if strV != "" {
			v, err := hex.DecodeString(string(rom.Md5))
			if err != nil {
				rom.Md5 = nil
			}
			rom.Md5 = v
		} else {
			rom.Md5 = nil
		}
	}
	if rom.Sha1 != nil {
		strV := string(rom.Sha1)
		if strV != "" {
			v, err := hex.DecodeString(string(rom.Sha1))
			if err != nil {
				rom.Sha1 = nil
			}
			rom.Sha1 = v
		} else {
			rom.Sha1 = nil
		}
	}
}

func ParseXml(r io.Reader, path string) (*types.Dat, []byte, error) {
	br := bufio.NewReader(r)

	hr := hashingReader{
		ir: br,
		h:  sha1.New(),
	}

	lr := lineCountingReader{
		ir: hr,
	}

	d := new(types.Dat)
	decoder := xml.NewDecoder(lr)

	err := decoder.Decode(d)
	if err != nil {
		derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, lr.line, err)
		derr := XMLParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(lr.line))
		return nil, nil, derr
	}

	for _, g := range d.Games {
		for _, rom := range g.Roms {
			fixHashes(rom)
		}
		for _, rom := range g.Parts {
			fixHashes(rom)
		}
		for _, rom := range g.Regions {
			fixHashes(rom)
		}
	}

	for _, g := range d.Software {
		for _, rom := range g.Roms {
			fixHashes(rom)
		}
		for _, rom := range g.Parts {
			fixHashes(rom)
		}
		for _, rom := range g.Regions {
			fixHashes(rom)
		}
	}

	for _, g := range d.Machines {
		for _, rom := range g.Roms {
			fixHashes(rom)
		}
		for _, rom := range g.Parts {
			fixHashes(rom)
		}
		for _, rom := range g.Regions {
			fixHashes(rom)
		}
	}

	d.Normalize()
	d.Path = path
	return d, hr.h.Sum(nil), nil
}

type xmlDatHeader struct {
	Name        string            `xml:"name"`
	Description string            `xml:"description"`
	Clr         *types.Clrmamepro `xml:"clrmamepro"`
}

func ParseXmlWithListener(r io.Reader, path string, pl ParseListener) ([]byte, error) {
	br := bufio.NewReader(r)

	hr := hashingReader{
		ir: br,
		h:  sha1.New(),
	}

	lr := lineCountingReader{
		ir: hr,
	}

	decoder := xml.NewDecoder(lr)

	var inElement string
	for {
		t, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, lr.line, err)
				derr := XMLParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(lr.line))
				return nil, derr
			}
		}
		if t == nil {
			break
		}
		switch se := t.(type) {
		case xml.StartElement:
			inElement = se.Name.Local
			if inElement == "header" {
				d := new(types.Dat)
				d.Path = path
				var hdr xmlDatHeader
				err = decoder.DecodeElement(&hdr, &se)
				if err != nil {
					derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, lr.line, err)
					derr := XMLParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(lr.line))
					return nil, derr
				}

				d.Name = hdr.Name
				d.Description = hdr.Description
				d.Clr = hdr.Clr

				d.Normalize()

				err = pl.ParsedDatStmt(d)
				if err != nil {
					derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, lr.line, err)
					derr := XMLParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(lr.line))
					return nil, derr
				}
			} else if inElement == "game" || inElement == "software" || inElement == "machine" {
				g := new(types.Game)
				err = decoder.DecodeElement(g, &se)
				if err != nil {
					derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, lr.line, err)
					derr := XMLParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(lr.line))
					return nil, derr
				}
				for _, rom := range g.Roms {
					fixHashes(rom)
				}
				for _, rom := range g.Parts {
					fixHashes(rom)
				}
				for _, rom := range g.Regions {
					fixHashes(rom)
				}
				g.Normalize()

				err = pl.ParsedGameStmt(g)
				if err != nil {
					derrStr := fmt.Sprintf("error in file %s on line %d: %v", path, lr.line, err)
					derr := XMLParseError.NewWith(derrStr, setErrorFilePath(path), setErrorLineNumber(lr.line))
					return nil, derr
				}
			}
		default:
		}
	}

	return hr.h.Sum(nil), nil
}
