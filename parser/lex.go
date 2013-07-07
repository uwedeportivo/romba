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
	"fmt"
	"io"
	"strings"
	"unicode"
)

type itemType int

const (
	itemError itemType = iota
	itemEOF
	itemOpenBrace
	itemCloseBrace
	itemQuotedString
	itemValue
	itemGame
	itemName
	itemDescription
	itemRom
	itemSize
	itemCrc
	itemMd5
	itemSha1
	itemCategory
	itemVersion
	itemAuthor
	itemClrMamePro
)

var itemTypePrettyPrint = map[itemType]string{
	itemError:        "error",
	itemEOF:          "EOF",
	itemOpenBrace:    "(",
	itemCloseBrace:   ")",
	itemQuotedString: "quoted string",
	itemValue:        "integer, date or hexbytes",
}

func (i itemType) String() string {
	s := itemTypePrettyPrint[i]
	if s == "" {
		return fmt.Sprintf("item%d", int(i))
	}
	return s
}

type item struct {
	typ itemType
	val string
}

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case i.typ > itemValue:
		return fmt.Sprintf("<%s>", i.val)
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

var key = map[string]itemType{
	"game":        itemGame,
	"name":        itemName,
	"description": itemDescription,
	"rom":         itemRom,
	"size":        itemSize,
	"crc":         itemCrc,
	"md5":         itemMd5,
	"sha1":        itemSha1,
	"category":    itemCategory,
	"version":     itemVersion,
	"author":      itemAuthor,
	"clrmamepro":  itemClrMamePro,
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r':
		return true
	}
	return false
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return r == '-' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func isNotSpaceOrEOF(r rune) bool {
	return !(isSpace(r) || r == eof)
}

const (
	eof         = -1
	readErrRune = -2
)

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

// lexer holds the state of the scanner.
type lexer struct {
	name     string        // the name of the input; used only for error reports.
	state    stateFn       // the next lexing function to enter.
	items    chan item     // channel of scanned items.
	br       *bufio.Reader // the buffered reader we're reading items from
	tk       []rune        // accumulates the current token value
	err      error         // last read error
	ln       int           // line number
	lastRune rune          // last read rune
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	switch r, _, err := l.br.ReadRune(); {
	case err == nil:
		l.lastRune = r
		if r == '\n' {
			l.ln++
		}
		l.tk = append(l.tk, r)
		return r
	case err == io.EOF:
		return eof
	default:
		l.err = err
		return readErrRune
	}
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r, _, err := l.br.ReadRune()
	switch err {
	case nil:
		l.br.UnreadRune()
		return r
	case io.EOF:
		return eof
	default:
		l.err = err
		return readErrRune
	}
}

// backup steps back one rune. Can only be called once per call of next.
func (l *lexer) backup() {
	if l.err == nil {
		l.tk = l.tk[:len(l.tk)-1]
		if l.lastRune == '\n' {
			l.ln--
		}
		l.br.UnreadRune()
	}
}

// emit passes an item back to the client.
func (l *lexer) emit(t itemType) {
	l.items <- item{t, string(l.tk)}
	l.tk = nil
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.tk = nil
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
}

func (l *lexer) lineNumber() int {
	// start counting at 1
	return l.ln + 1
}

// error returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, fmt.Sprintf(format, args...)}
	return nil
}

// nextItem returns the next item from the input.
func (l *lexer) nextItem() item {
	for {
		select {
		case item := <-l.items:
			return item
		default:
			l.state = l.state(l)
		}
	}
	panic("not reached")
}

// lex creates a new scanner for the input string.
func lex(name string, rd io.Reader) *lexer {
	l := &lexer{
		name:  name,
		br:    bufio.NewReader(rd),
		state: lexDefault,
		items: make(chan item, 2), // Two items of buffering is sufficient for all state functions
	}
	return l
}

// state functions

// lexQuote scans a quoted string.
func lexQuote(l *lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case readErrRune:
			return l.errorf("error reading: %v", l.err)
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				break
			}
			fallthrough
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case '"':
			break Loop
		}
	}
	l.emit(itemQuotedString)
	return lexDefault
}

func lexAlpha(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case r == readErrRune:
			return l.errorf("error reading: %v", l.err)
		case isNotSpaceOrEOF(r):
			//absorb
		default:
			l.backup()
			word := string(l.tk)

			switch {
			case key[word] > itemValue:
				l.emit(key[word])
			default:
				l.emit(itemValue)
			}
			return lexDefault
		}
	}
	return lexDefault
}

func lexDefault(l *lexer) stateFn {
	switch r := l.next(); {
	case r == readErrRune:
		return l.errorf("error reading: %v", l.err)
	case r == eof:
		l.emit(itemEOF)
		return lexDefault
	case isSpace(r):
		l.ignore()
	case r == '"':
		return lexQuote
	case r == '(' && isSpace(l.peek()):
		l.emit(itemOpenBrace)
	case r == ')':
		l.emit(itemCloseBrace)
	default:
		l.backup()
		return lexAlpha
	}
	return lexDefault
}
