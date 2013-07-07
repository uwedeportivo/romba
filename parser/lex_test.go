// Copyright (c) 2013 Uwe Hoffmann. All rights reserved.

package parser

import (
	"os"
	"testing"
)

func TestLexerGoesThrough(t *testing.T) {
	file, err := os.Open("testdata/example.dat")
	if err != nil {
		t.Fatalf("error opening test data: %v", err)
	}
	defer file.Close()

	ll := lex("test", file)

	for i := ll.nextItem(); i.typ != itemEOF; i = ll.nextItem() {
	}
}
