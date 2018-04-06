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

package torrentzip

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/uwedeportivo/torrentzip/czip"
)

const (
	fileHeaderSignature      = 0x04034b50
	directoryHeaderSignature = 0x02014b50
	directoryEndSignature    = 0x06054b50
	fileHeaderLen            = 30 // + filename + extra
	directoryHeaderLen       = 46 // + filename + extra + comment
	directoryEndLen          = 22 // + comment

	creatorFAT = 0

	zipVersion20 = 20 // 2.0
	zipVersion45 = 45 // 4.5 (reads and writes zip64 archives)

	directory64LocSignature = 0x07064b50
	directory64EndSignature = 0x06064b50
	directory64LocLen       = 20 //
	directory64EndLen       = 56

	// limits for non zip64 files
	uint16max = (1 << 16) - 1
	uint32max = (1 << 32) - 1

	// extra header id's
	zip64ExtraId = 0x0001 // zip64 Extended Information Extra Field
)

type Writer struct {
	uw   *czip.Writer
	sink io.Writer
	tf   *os.File
	bf   *bufio.Writer
}

func NewWriter(w io.Writer) (*Writer, error) {
	return NewWriterWithTemp(w, "")
}

func NewWriterWithTemp(w io.Writer, tempDir string) (*Writer, error) {
	r := new(Writer)

	tf, err := ioutil.TempFile(tempDir, "torrentzip")
	if err != nil {
		return nil, err
	}
	r.tf = tf
	r.bf = bufio.NewWriter(r.tf)

	r.sink = w
	r.uw = czip.NewWriter(r.bf)
	return r, nil
}

type fileIndex struct {
	name   string
	index  int
	offset int64
}

type fileIndices []*fileIndex

func (fis fileIndices) Len() int {
	return len(fis)
}

func (fis fileIndices) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis fileIndices) Less(i, j int) bool {
	return strings.ToLower(fis[i].name) < strings.ToLower(fis[j].name)
}

func torrentCanonicalName(name string) string {
	if filepath.Separator != '/' {
		name = strings.Replace(name, string(filepath.Separator), "/", -1)
	}
	return name
}

func (w *Writer) Close() error {
	err := w.uw.Close()
	if err != nil {
		return err
	}

	err = w.bf.Flush()
	if err != nil {
		return err
	}

	err = w.tf.Close()
	if err != nil {
		return err
	}

	r, err := czip.OpenReader(w.tf.Name())
	if err != nil {
		return err
	}

	fis := make(fileIndices, len(r.File))

	for k, zf := range r.File {
		fis[k] = &fileIndex{
			name:  torrentCanonicalName(zf.Name),
			index: k,
		}
	}

	sort.Sort(fis)

	cw := &countWriter{
		w: w.sink,
	}

	for _, fi := range fis {
		fh := r.File[fi.index]
		fh.Extra = nil
		fi.offset = cw.count
		err = writeHeader(cw, fh, fi.name)
		if err != nil {
			return err
		}
		fr, err := fh.OpenRaw()
		if err != nil {
			return err
		}

		_, err = io.Copy(cw, fr)
		if err != nil {
			return err
		}
	}

	dircrc := crc32.NewIEEE()
	mw := io.MultiWriter(cw, dircrc)
	start := cw.count

	for _, fi := range fis {
		fh := r.File[fi.index]
		fh.Extra = nil
		err = writeCentralHeader(mw, fh, fi.name, fi.offset)
		if err != nil {
			return err
		}
	}
	end := cw.count

	records := uint64(len(fis))
	size := uint64(end - start)
	offset := uint64(start)

	if records > uint16max || size > uint32max || offset > uint32max {
		var buf [directory64EndLen + directory64LocLen]byte
		b := writeBuf(buf[:])

		// zip64 end of central directory record
		b.uint32(directory64EndSignature)
		b.uint64(directory64EndLen - 12)
		b.uint16(zipVersion45) // version made by
		b.uint16(zipVersion45) // version needed to extract
		b.uint32(0)            // number of this disk
		b.uint32(0)            // number of the disk with the start of the central directory
		b.uint64(records)      // total number of entries in the central directory on this disk
		b.uint64(records)      // total number of entries in the central directory
		b.uint64(size)         // size of the central directory
		b.uint64(offset)       // offset of start of central directory with respect to the starting disk number

		// zip64 end of central directory locator
		b.uint32(directory64LocSignature)
		b.uint32(0)           // number of the disk with the start of the zip64 end of central directory
		b.uint64(uint64(end)) // relative offset of the zip64 end of central directory record
		b.uint32(1)           // total number of disks

		if _, err := cw.Write(buf[:]); err != nil {
			return err
		}

		// store max values in the regular end record to signal that
		// that the zip64 values should be used instead
		if records > uint16max {
			records = uint16max
		}
		if size > uint32max {
			size = uint32max
		}
		if offset > uint32max {
			offset = uint32max
		}
	}

	var buf [directoryEndLen]byte
	b := writeBuf(buf[:])
	b.uint32(uint32(directoryEndSignature))
	b.uint16(0)
	b.uint16(0)
	b.uint16(uint16(records))
	b.uint16(uint16(records))
	b.uint32(uint32(size))
	b.uint32(uint32(offset))
	b.uint16(22)

	zipcomment := "TORRENTZIPPED-" + strings.ToUpper(hex.EncodeToString(dircrc.Sum(nil)))

	if _, err := cw.Write(buf[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(cw, zipcomment); err != nil {
		return err
	}

	if err := r.Close(); err != nil {
		return err
	}

	if err := os.Remove(w.tf.Name()); err != nil {
		return err
	}

	return nil
}

func writeHeader(w io.Writer, h *czip.File, canonicalName string) error {
	var buf [fileHeaderLen]byte
	b := writeBuf(buf[:])
	b.uint32(uint32(fileHeaderSignature))
	if isZip64(h) {
		b.uint16(zipVersion45)
	} else {
		b.uint16(zipVersion20)
	}
	b.uint16(2)
	b.uint16(8)
	b.uint16(48128)
	b.uint16(8600)
	b.uint32(h.CRC32)
	if isZip64(h) {
		// the file needs a zip64 header. store maxint in both
		// 32 bit size fields to signal that the
		// zip64 extra header should be used.
		b.uint32(uint32max) // compressed size
		b.uint32(uint32max) // uncompressed size

		// append a zip64 extra block to Extra
		var buf [20]byte // 2x uint16 + 2x uint64
		eb := writeBuf(buf[:])
		eb.uint16(zip64ExtraId)
		eb.uint16(16) // size = 2x uint64
		eb.uint64(h.UncompressedSize64)
		eb.uint64(h.CompressedSize64)
		h.Extra = append(h.Extra, buf[:]...)
	} else {
		b.uint32(h.CompressedSize)
		b.uint32(h.UncompressedSize)
	}
	b.uint16(uint16(len(canonicalName)))
	b.uint16(uint16(len(h.Extra)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, canonicalName); err != nil {
		return err
	}
	_, err := w.Write(h.Extra)
	return err
}

func writeCentralHeader(w io.Writer, h *czip.File, canonicalName string, offset int64) error {
	var buf [directoryHeaderLen]byte
	b := writeBuf(buf[:])
	b.uint32(uint32(directoryHeaderSignature))
	b.uint16(creatorFAT)
	if isZip64(h) || offset > uint32max {
		b.uint16(zipVersion45)
	} else {
		b.uint16(zipVersion20)
	}
	b.uint16(2)
	b.uint16(8)
	b.uint16(48128)
	b.uint16(8600)
	b.uint32(h.CRC32)

	if h.CompressedSize64 > uint32max {
		b.uint32(uint32max)
	} else {
		b.uint32(uint32(h.CompressedSize64))
	}

	if h.UncompressedSize64 > uint32max {
		b.uint32(uint32max)
	} else {
		b.uint32(uint32(h.UncompressedSize64))
	}

	b.uint16(uint16(len(canonicalName)))
	if isZip64(h) || offset > uint32max {
		var esize uint16

		if h.CompressedSize64 > uint32max {
			esize += 8
		}

		if h.UncompressedSize64 > uint32max {
			esize += 8
		}

		if offset > uint32max {
			esize += 8
		}

		var b1 [4]byte
		eb := writeBuf(b1[:])
		eb.uint16(zip64ExtraId)
		eb.uint16(esize)
		h.Extra = append(h.Extra, b1[:]...)

		if h.UncompressedSize64 > uint32max {
			var b2 [8]byte
			eb := writeBuf(b2[:])
			eb.uint64(h.UncompressedSize64)
			h.Extra = append(h.Extra, b2[:]...)
		}

		if h.CompressedSize64 > uint32max {
			var b2 [8]byte
			eb := writeBuf(b2[:])
			eb.uint64(h.CompressedSize64)
			h.Extra = append(h.Extra, b2[:]...)
		}

		if offset > uint32max {
			var b3 [8]byte
			eb := writeBuf(b3[:])
			eb.uint64(uint64(offset))
			h.Extra = append(h.Extra, b3[:]...)
		}
	}
	b.uint16(uint16(len(h.Extra)))
	b.uint16(0)
	b.uint16(0)
	b.uint16(0)
	b.uint32(0)
	if offset > uint32max {
		b.uint32(uint32max)
	} else {
		b.uint32(uint32(offset))
	}
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, canonicalName); err != nil {
		return err
	}

	_, err := w.Write(h.Extra)
	return err
}

func isZip64(h *czip.File) bool {
	return h.CompressedSize64 > uint32max || h.UncompressedSize64 > uint32max
}

func (w *Writer) Create(name string) (io.Writer, error) {
	return w.uw.Create(name)
}

type writeBuf []byte

func (b *writeBuf) uint16(v uint16) {
	binary.LittleEndian.PutUint16(*b, v)
	*b = (*b)[2:]
}

func (b *writeBuf) uint32(v uint32) {
	binary.LittleEndian.PutUint32(*b, v)
	*b = (*b)[4:]
}

func (b *writeBuf) uint64(v uint64) {
	binary.LittleEndian.PutUint64(*b, v)
	*b = (*b)[8:]
}

type countWriter struct {
	w     io.Writer
	count int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.count += int64(n)
	return n, err
}
