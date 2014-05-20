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
	"fmt"
	"strings"
	"testing"

	"github.com/uwedeportivo/romba/types"
)

func TestParserDatGoesThrough(t *testing.T) {
	_, _, err := Parse("testdata/example.dat")
	if err != nil {
		t.Fatalf("error parsing test data: %v", err)
	}
}

func TestParserXmlGoesThrough(t *testing.T) {
	_, _, err := Parse("testdata/example.xml")
	if err != nil {
		t.Fatalf("error parsing test data: %v", err)
	}
}

const datText = `
clrmamepro (
	name "Acorn Archimedes - Applications"
	description "Acorn Archimedes - Applications (TOSEC-v2008-10-11)"
	category "Acorn Archimedes - Applications"
	version 2008-10-11
	author "C0llector - Cassiel"
)

game (
	name "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS]"
	description "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS]"
	rom ( name "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS].adf" size 819200 crc e43166b9 md5 43ee6acc0c173048f47826307c0a262e )
)

game (
	name "Afterburner (1989)(Sega)(Side A)[cr NEC]"
	description "Afterburner (1989)(Sega)(Side A)[cr NEC]"
	rom ( name "Afterburner (1989)(Sega)(Side A)[cr NEC].g64" size 333744 crc 0x175a3f26 md5 36ecf1371d3391c06c16f751431c932b sha1 80353cb168dc5d7cc1dce57971f4ea2640a50ac4 )
)
`

func TestParseDat(t *testing.T) {
	dat, _, err := ParseDat(strings.NewReader(datText), "testing/dat")

	if err != nil {
		t.Fatalf("error parsing test data: %v", err)
	}

	datGolden := &types.Dat{
		Name:        "Acorn Archimedes - Applications",
		Description: "Acorn Archimedes - Applications (TOSEC-v2008-10-11)",
		Games: []*types.Game{
			&types.Game{
				Name:        "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS]",
				Description: "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS]",
				Roms: []*types.Rom{
					&types.Rom{
						Name: "Acorn Archimedes RISC OS Application Suite v1.00 (19xx)(Acorn)(Disk 1 of 2)[a][Req RISC OS].adf",
						Size: 819200,
						Crc:  []byte{0xe4, 0x31, 0x66, 0xb9},
						Md5:  []byte{0x43, 0xee, 0x6a, 0xcc, 0xc, 0x17, 0x30, 0x48, 0xf4, 0x78, 0x26, 0x30, 0x7c, 0xa, 0x26, 0x2e},
					},
				},
			},
			&types.Game{
				Name:        "Afterburner (1989)(Sega)(Side A)[cr NEC]",
				Description: "Afterburner (1989)(Sega)(Side A)[cr NEC]",
				Roms: []*types.Rom{
					&types.Rom{
						Name: "Afterburner (1989)(Sega)(Side A)[cr NEC].g64",
						Size: 333744,
						Crc:  []byte{0x17, 0x5a, 0x3f, 0x26},
						Md5:  []byte{0x36, 0xec, 0xf1, 0x37, 0x1d, 0x33, 0x91, 0xc0, 0x6c, 0x16, 0xf7, 0x51, 0x43, 0x1c, 0x93, 0x2b},
						Sha1: []byte{0x80, 0x35, 0x3c, 0xb1, 0x68, 0xdc, 0x5d, 0x7c, 0xc1, 0xdc, 0xe5, 0x79, 0x71, 0xf4, 0xea, 0x26, 0x40, 0xa5, 0xa, 0xc4},
					},
				},
			},
		},
	}

	datGolden.Normalize()

	if !datGolden.Equals(dat) {
		fmt.Printf("datGolden=%s\n", string(types.PrintDat(datGolden)))
		fmt.Printf("dat=%s\n", string(types.PrintDat(dat)))
		t.Fatalf("parsed dat differs from golden dat")
	}
}

const xmlText = `
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE datafile PUBLIC "-//Logiqx//DTD ROM Management Datafile//EN" "http://www.logiqx.com/Dats/datafile.dtd">

<datafile>
	<header>
		<name>AgeMame Artwork</name>
		<description>AgeMame Artwork</description>
		<category>Standard DatFile</category>
		<version>0.134</version>
		<date>Sep 16 2009</date>
		<author>-insert author-</author>
		<email>-insert email-</email>
		<homepage>AGEMAME HQ</homepage>
		<url>http://agemame.mameworld.info/</url>
		<comment>-insert comment-</comment>
		<clrmamepro/>
	</header>
	<game name="bfmdrwho">
		<description>bfmdrwho</description>
		<rom name="alloff.png" size="398080" crc="4ae02749" md5="ce234f01d8068aaab7075c3a42fe523d" sha1="f6389b4afc932ae40202c575a6c5ba25deaaeef4"/>
		<rom name="bfmdrwho.lay" size="66185" crc="90b98b40" md5="0c92bd59c804d4e35170208205166576" sha1="ff0c0e7dedeaf8461e115062092a106aa0d58452"/>
    </game>
    <software name="megaman7p" cloneof="megaman7">
    	<!-- lostlevels.org -->
    	<!-- Notes: identical to the final release -->
    	<description>Mega Man 7 (USA, Final Prototype)</description>
    	<year>1995</year>
    	<publisher>Capcom</publisher>
    	<part name="cart" interface="snes_cart">
    		<feature name="pcb" value="SHVC-4PV5B-01" />
    		<feature name="u1" value="U1 EPROM" />
    		<feature name="u2" value="U2 EPROM" />
    		<feature name="u3" value="U3 EPROM" />
    		<feature name="u4" value="U4 EPROM" />
    		<feature name="u5" value="U5 SRAM" /> <!-- empty socket -->
    		<feature name="u6" value="U6 PLD" />
    		<feature name="u7" value="U7 74LS157" />
    		<feature name="u8" value="U8 CIC" />
    		<feature name="lockout" value="" />
    		<feature name="battery" value="BATT CR2032" />
    		<feature name="cart_model" value="no shell" />
    		<feature name="slot" value="hirom" />
    		<dataarea name="rom" size="2097152">
    			<rom name="rom 0.u1" size="524288" crc="8742aa77" sha1="60e7a83620efacfef9821f13c83679fa2413fdd2" offset="0x000000" />
    			<rom name="rom 1.u2" size="524288" crc="25eec90a" sha1="2bedac3c3dde6780389a98750ac05ca1ee41caf5" offset="0x080000" />
    		</dataarea>
    	</part>
    </software>
    <game name="10yard" board="Irem M62">
		<description>10-Yard Fight (World, set 1)</description>
		<year>1983</year>
		<manufacturer>Irem</manufacturer>
		<m1data default="21" stop="0" min="1" max="64"/>
		<region type="cpu1" size="65536">
			<rom name="yf-s.3b" size="8192" crc="0392a60c" sha1="68030504eafc58db250099edd3c3323bdb9eff6b" offset="8000"/>
			<rom name="yf-s.1b" size="8192" crc="6588f41a" sha1="209305efc68171886427216b9a0b37333f40daa8" offset="a000"/>
		</region>
		<region type="cpu1" size="131072">
			<rom name="41_9.12b" size="65536" crc="0f9d8527" offset="0"/>
		</region>
	</game>
	<game name="Games - Prototype (US)\A.E. (US) [-] [N.A.]">
		<description>A.E. (US) [-] [N.A.]</description>
		<rom name="Media\A.E. (1982)(Atari)(proto).bin" size="16384" crc="35484751" md5="a47fcb4eedab9418ea098bb431a407aa" sha1=""/>
	</game>
</datafile>
`

func TestParseXml(t *testing.T) {
	dat, _, err := ParseXml(strings.NewReader(xmlText), "testing/xml")

	if err != nil {
		t.Fatalf("error parsing test data: %v", err)
	}

	datGolden := &types.Dat{
		Name:        "AgeMame Artwork",
		Description: "AgeMame Artwork",
		Games: []*types.Game{
			&types.Game{
				Name:        "bfmdrwho",
				Description: "bfmdrwho",
				Roms: []*types.Rom{
					&types.Rom{
						Name: "alloff.png",
						Size: 398080,
						Crc:  []byte{0x4a, 0xe0, 0x27, 0x49},
						Md5:  []byte{0xce, 0x23, 0x4f, 0x1, 0xd8, 0x6, 0x8a, 0xaa, 0xb7, 0x7, 0x5c, 0x3a, 0x42, 0xfe, 0x52, 0x3d},
						Sha1: []byte{0xf6, 0x38, 0x9b, 0x4a, 0xfc, 0x93, 0x2a, 0xe4, 0x2, 0x2, 0xc5, 0x75, 0xa6, 0xc5, 0xba, 0x25, 0xde, 0xaa, 0xee, 0xf4},
					},
					&types.Rom{
						Name: "bfmdrwho.lay",
						Size: 66185,
						Crc:  []byte{0x90, 0xb9, 0x8b, 0x40},
						Md5:  []byte{0xc, 0x92, 0xbd, 0x59, 0xc8, 0x4, 0xd4, 0xe3, 0x51, 0x70, 0x20, 0x82, 0x5, 0x16, 0x65, 0x76},
						Sha1: []byte{0xff, 0xc, 0xe, 0x7d, 0xed, 0xea, 0xf8, 0x46, 0x1e, 0x11, 0x50, 0x62, 0x9, 0x2a, 0x10, 0x6a, 0xa0, 0xd5, 0x84, 0x52},
					},
				},
			},
			&types.Game{
				Name:        "megaman7p",
				Description: "Mega Man 7 (USA, Final Prototype)",
				Roms: []*types.Rom{
					&types.Rom{
						Name: "rom 0.u1",
						Size: 524288,
						Crc:  []byte{0x87, 0x42, 0xaa, 0x77},
						Sha1: []byte{0x60, 0xe7, 0xa8, 0x36, 0x20, 0xef, 0xac, 0xfe, 0xf9, 0x82, 0x1f, 0x13, 0xc8, 0x36, 0x79, 0xfa, 0x24, 0x13, 0xfd, 0xd2},
					},
					&types.Rom{
						Name: "rom 1.u2",
						Size: 524288,
						Crc:  []byte{0x25, 0xee, 0xc9, 0xa},
						Sha1: []byte{0x2b, 0xed, 0xac, 0x3c, 0x3d, 0xde, 0x67, 0x80, 0x38, 0x9a, 0x98, 0x75, 0xa, 0xc0, 0x5c, 0xa1, 0xee, 0x41, 0xca, 0xf5},
					},
				},
			},
			&types.Game{
				Name:        "10yard",
				Description: "10-Yard Fight (World, set 1)",
				Roms: []*types.Rom{
					&types.Rom{
						Name: "yf-s.3b",
						Size: 8192,
						Crc:  []byte{0x3, 0x92, 0xa6, 0xc},
						Sha1: []byte{0x68, 0x3, 0x5, 0x4, 0xea, 0xfc, 0x58, 0xdb, 0x25, 0x0, 0x99, 0xed, 0xd3, 0xc3, 0x32, 0x3b, 0xdb, 0x9e, 0xff, 0x6b},
					},
					&types.Rom{
						Name: "yf-s.1b",
						Size: 8192,
						Crc:  []byte{0x65, 0x88, 0xf4, 0x1a},
						Sha1: []byte{0x20, 0x93, 0x5, 0xef, 0xc6, 0x81, 0x71, 0x88, 0x64, 0x27, 0x21, 0x6b, 0x9a, 0xb, 0x37, 0x33, 0x3f, 0x40, 0xda, 0xa8},
					},
					&types.Rom{
						Name: "41_9.12b",
						Size: 65536,
						Crc:  []byte{0xf, 0x9d, 0x85, 0x27},
					},
				},
			},
			&types.Game{
				Name:        "Games - Prototype (US)\\A.E. (US) [-] [N.A.]",
				Description: "A.E. (US) [-] [N.A.]",
				Roms: []*types.Rom{
					&types.Rom{
						Name: "Media\\A.E. (1982)(Atari)(proto).bin",
						Size: 16384,
						Crc:  []byte{0x35, 0x48, 0x47, 0x51},
						Md5:  []byte{0xa4, 0x7f, 0xcb, 0x4e, 0xed, 0xab, 0x94, 0x18, 0xea, 0x9, 0x8b, 0xb4, 0x31, 0xa4, 0x7, 0xaa},
					},
				},
			},
		},
	}

	datGolden.Normalize()

	if !datGolden.Equals(dat) {
		fmt.Printf("datGolden=%s\n", string(types.PrintDat(datGolden)))
		fmt.Printf("dat=%s\n", string(types.PrintDat(dat)))
		t.Fatalf("parsed dat differs from golden dat")
	}
}

const datForceZipText = `
clrmamepro (
	name "Acorn Archimedes - Applications"
	description "Acorn Archimedes - Applications (TOSEC-v2008-10-11)"
	category "Acorn Archimedes - Applications"
	version 2008-10-11
	author "C0llector - Cassiel"
	forcezipping "no"
)
`

func TestParseForceZipDat(t *testing.T) {
	dat, _, err := ParseDat(strings.NewReader(datForceZipText), "testing/dat")

	if err != nil {
		t.Fatalf("error parsing test data: %v", err)
	}

	datGolden := &types.Dat{
		Name:        "Acorn Archimedes - Applications",
		Description: "Acorn Archimedes - Applications (TOSEC-v2008-10-11)",
		UnzipGames:  true,
	}

	datGolden.Normalize()

	if !datGolden.Equals(dat) {
		fmt.Printf("datGolden=%s\n", string(types.PrintDat(datGolden)))
		fmt.Printf("dat=%s\n", string(types.PrintDat(dat)))
		t.Fatalf("parsed dat differs from golden dat")
	}
}

const xmlForceZipText = `
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE datafile PUBLIC "-//Logiqx//DTD ROM Management Datafile//EN" "http://www.logiqx.com/Dats/datafile.dtd">

<datafile>
	<header>
		<name>AgeMame Artwork</name>
		<description>AgeMame Artwork</description>
		<category>Standard DatFile</category>
		<version>0.134</version>
		<date>Sep 16 2009</date>
		<author>-insert author-</author>
		<email>-insert email-</email>
		<homepage>AGEMAME HQ</homepage>
		<url>http://agemame.mameworld.info/</url>
		<comment>-insert comment-</comment>
		<clrmamepro forcepacking="unzip"/>
	</header>
</datafile>
`

func TestParseForceZipXml(t *testing.T) {
	dat, _, err := ParseXml(strings.NewReader(xmlForceZipText), "testing/xml")

	if err != nil {
		t.Fatalf("error parsing test data: %v", err)
	}

	datGolden := &types.Dat{
		Name:        "AgeMame Artwork",
		Description: "AgeMame Artwork",
		UnzipGames:  true,
	}

	datGolden.Normalize()

	if !datGolden.Equals(dat) {
		fmt.Printf("datGolden=%s\n", string(types.PrintDat(datGolden)))
		fmt.Printf("dat=%s\n", string(types.PrintDat(dat)))
		t.Fatalf("parsed dat differs from golden dat")
	}
}
