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

package archive

import (
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/crc32"
	"github.com/uwedeportivo/romba/types"
	"github.com/uwedeportivo/romba/util"
)

const (
	zipSuffix      = ".zip"
	gzipSuffix     = ".gz"
	sevenzipSuffix = ".7z"
	datSuffix      = ".dat"
	fixPrefix      = "fix-"
)

type Hashes struct {
	Crc  []byte
	Md5  []byte
	Sha1 []byte
	Size int64
}

func newHashes() *Hashes {
	rs := new(Hashes)
	rs.Crc = make([]byte, 0, crc32.Size)
	rs.Md5 = make([]byte, 0, md5.Size)
	rs.Sha1 = make([]byte, 0, sha1.Size)
	return rs
}

func (hh *Hashes) forFile(inpath string) error {
	file, err := os.Open(inpath)
	if err != nil {
		return err
	}
	defer file.Close()

	return hh.forReader(file)
}

func (hh *Hashes) forReader(in io.Reader) error {
	br := bufio.NewReader(in)

	hSha1 := sha1.New()
	hMd5 := md5.New()
	hCrc := crc32.NewIEEE()

	w := io.MultiWriter(hSha1, hMd5, hCrc)
	cw := &countWriter{
		w: w,
	}

	_, err := io.Copy(cw, br)
	if err != nil {
		return err
	}

	hh.Crc = hCrc.Sum(hh.Crc[0:0])
	hh.Md5 = hMd5.Sum(hh.Md5[0:0])
	hh.Sha1 = hSha1.Sum(hh.Sha1[0:0])
	hh.Size = cw.count

	return nil
}

func HashesForGZFile(inpath string) (*Hashes, error) {
	file, err := os.Open(inpath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	return hashesForReader(gzipReader)
}

func RomFromGZDepotFile(inpath string) (*types.Rom, error) {
	rom := new(types.Rom)
	fileName := filepath.Base(inpath)
	sha1Hex := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	sha1, err := hex.DecodeString(sha1Hex)
	if err != nil {
		return nil, err
	}
	rom.Sha1 = sha1
	return rom, nil
}

func HashesForFile(inpath string) (*Hashes, error) {
	file, err := os.Open(inpath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return hashesForReader(file)
}

func HashesFromGZHeader(inpath string, md5crcBuffer []byte) (*Hashes, int64, error) {
	romGZ, err := os.Open(inpath)
	if err != nil {
		return nil, 0, err
	}
	defer romGZ.Close()

	gzr, err := gzip.NewReader(romGZ)
	if err != nil {
		return nil, 0, err
	}
	defer gzr.Close()

	md5crcBuffer = gzr.Header.Extra

	var hh *Hashes
	var size int64

	if len(md5crcBuffer) == md5.Size+crc32.Size+8 {
		hh = HashesFromMd5crcBuffer(md5crcBuffer)
		size = hh.Size
	}
	return hh, size, nil
}

func HashesFromMd5crcBuffer(md5crcBuffer []byte) *Hashes {
	hh := new(Hashes)
	hh.Md5 = make([]byte, md5.Size)
	copy(hh.Md5, md5crcBuffer[:md5.Size])
	hh.Crc = make([]byte, crc32.Size)
	copy(hh.Crc, md5crcBuffer[md5.Size:md5.Size+crc32.Size])
	hh.Size = util.BytesToInt64(md5crcBuffer[md5.Size+crc32.Size:])
	return hh
}

func hashesForReader(in io.Reader) (*Hashes, error) {
	hSha1 := sha1.New()
	hMd5 := md5.New()
	hCrc := crc32.NewIEEE()

	w := io.MultiWriter(hSha1, hMd5, hCrc)

	_, err := io.Copy(w, in)
	if err != nil {
		return nil, err
	}

	res := new(Hashes)
	res.Crc = hCrc.Sum(nil)
	res.Md5 = hMd5.Sum(nil)
	res.Sha1 = hSha1.Sum(nil)

	return res, nil
}

func sha1ForFile(inpath string) ([]byte, error) {
	file, err := os.Open(inpath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return sha1ForReader(file)
}

func sha1ForReader(in io.Reader) ([]byte, error) {
	h := sha1.New()

	_, err := io.Copy(h, in)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func pathFromSha1HexEncoding(root, hexStr, suffix string) string {
	prefix := hexStr[0:8]
	pieces := make([]string, 6)

	pieces[0] = root
	for i := 0; i < 4; i++ {
		pieces[i+1] = prefix[2*i : 2*i+2]
	}
	pieces[5] = hexStr + suffix

	return filepath.Join(pieces...)
}

func PathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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

func DeleteEmptyFolders(root string) error {
	fi, err := os.Lstat(root)
	if err != nil {
		return err
	}

	if !fi.IsDir() {
		return nil
	}

	return deleteEmptyFoldersImpl(root, 0)
}

func deleteEmptyFoldersImpl(root string, level int) error {
	fis, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}

	foundPlain := false

	for _, sfi := range fis {
		if sfi.IsDir() {
			err = deleteEmptyFoldersImpl(filepath.Join(root, sfi.Name()), level+1)
			if err != nil {
				return err
			}
		} else {
			foundPlain = true
		}
	}

	if !foundPlain && level > 0 {
		fis, err = ioutil.ReadDir(root)
		if err != nil {
			return err
		}

		if len(fis) == 0 {
			err = os.Remove(root)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
