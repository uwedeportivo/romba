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

package kivi

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

const (
	keydirFilename     = "keydir"
	keydirSha1Filename = "keydir-sha1"
)

func writeKeydir(w io.Writer, kd *keydir, serialId int64) ([]byte, error) {
	hh := sha1.New()
	mw := io.MultiWriter(w, hh)

	count := kd.size()

	err := binary.Write(mw, binary.BigEndian, serialId)
	if err != nil {
		return nil, err
	}

	err = binary.Write(mw, binary.BigEndian, count)
	if err != nil {
		return nil, err
	}

	err = binary.Write(mw, binary.BigEndian, kd.orphaned)
	if err != nil {
		return nil, err
	}

	err = binary.Write(mw, binary.BigEndian, int16(kd.keySize))
	if err != nil {
		return nil, err
	}

	for i := 0; i < numParts; i++ {
		switch kd.keySize {
		case keySizeCrc:
			for key, kdes := range kd.parts[i].mCrc {
				_, err = mw.Write(key[:])
				if err != nil {
					return nil, err
				}

				err = binary.Write(mw, binary.BigEndian, int32(len(kdes)))
				if err != nil {
					return nil, err
				}

				for _, kde := range kdes {
					err = binary.Write(mw, binary.BigEndian, kde.fileId)
					if err != nil {
						return nil, err
					}
					err = binary.Write(mw, binary.BigEndian, kde.vpos)
					if err != nil {
						return nil, err
					}
					err = binary.Write(mw, binary.BigEndian, kde.vsize)
					if err != nil {
						return nil, err
					}
				}
			}
		case keySizeMd5:
			for key, kdes := range kd.parts[i].mMd5 {
				_, err = mw.Write(key[:])
				if err != nil {
					return nil, err
				}

				err = binary.Write(mw, binary.BigEndian, int32(len(kdes)))
				if err != nil {
					return nil, err
				}

				for _, kde := range kdes {
					err = binary.Write(mw, binary.BigEndian, kde.fileId)
					if err != nil {
						return nil, err
					}
					err = binary.Write(mw, binary.BigEndian, kde.vpos)
					if err != nil {
						return nil, err
					}
					err = binary.Write(mw, binary.BigEndian, kde.vsize)
					if err != nil {
						return nil, err
					}
				}
			}
		case keySizeSha1:
			for key, kdes := range kd.parts[i].mSha1 {
				_, err = mw.Write(key[:])
				if err != nil {
					return nil, err
				}

				err = binary.Write(mw, binary.BigEndian, int32(len(kdes)))
				if err != nil {
					return nil, err
				}

				for _, kde := range kdes {
					err = binary.Write(mw, binary.BigEndian, kde.fileId)
					if err != nil {
						return nil, err
					}
					err = binary.Write(mw, binary.BigEndian, kde.vpos)
					if err != nil {
						return nil, err
					}
					err = binary.Write(mw, binary.BigEndian, kde.vsize)
					if err != nil {
						return nil, err
					}
				}
			}
		}

	}
	return hh.Sum(nil), nil
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

func readKeydir(r io.Reader) (*keydir, int64, []byte, error) {
	hr := hashingReader{
		ir: r,
		h:  sha1.New(),
	}

	var count, serialId, orphaned int64
	var keySize int16

	err := binary.Read(hr, binary.BigEndian, &serialId)
	if err != nil {
		return nil, 0, nil, err
	}

	err = binary.Read(hr, binary.BigEndian, &count)
	if err != nil {
		return nil, 0, nil, err
	}

	err = binary.Read(hr, binary.BigEndian, &orphaned)
	if err != nil {
		return nil, 0, nil, err
	}

	err = binary.Read(hr, binary.BigEndian, &keySize)
	if err != nil {
		return nil, 0, nil, err
	}

	kd := newKeydir(int(keySize))

	key := make([]byte, keySize)

	var i int64
	for i = 0; i < count; i++ {
		_, err = io.ReadFull(hr, key)
		if err != nil {
			return nil, 0, nil, err
		}

		var kc int32

		err = binary.Read(hr, binary.BigEndian, &kc)
		if err != nil {
			return nil, 0, nil, err
		}

		var j int32
		for j = 0; j < kc; j++ {
			kde := new(keydirEntry)

			var fileId int16
			err = binary.Read(hr, binary.BigEndian, &fileId)
			if err != nil {
				return nil, 0, nil, err
			}
			kde.fileId = fileId

			var v int32
			err = binary.Read(hr, binary.BigEndian, &v)
			if err != nil {
				return nil, 0, nil, err
			}
			kde.vpos = v
			err = binary.Read(hr, binary.BigEndian, &v)
			if err != nil {
				return nil, 0, nil, err
			}
			kde.vsize = v

			kd.append(key, kde)
		}
	}

	return kd, serialId, hr.h.Sum(nil), nil
}

func saveKeydir(root string, kd *keydir, serialId int64) error {
	f, err := os.Create(filepath.Join(root, keydirFilename))
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	sha1Bytes, err := writeKeydir(bw, kd, serialId)
	if err != nil {
		return err
	}

	fsha, err := os.Create(filepath.Join(root, keydirSha1Filename))
	if err != nil {
		return err
	}
	defer fsha.Close()

	bwsha := bufio.NewWriter(fsha)
	defer bwsha.Flush()

	_, err = bwsha.Write(sha1Bytes)
	return err
}

func openKeydir(root string, goldenSerialId int64) (*keydir, error) {
	kdfileName := filepath.Join(root, keydirFilename)

	present, err := pathExists(kdfileName)
	if err != nil {
		return nil, err
	}

	if !present {
		return nil, nil
	}

	f, err := os.Open(kdfileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	br := bufio.NewReader(f)

	kd, serialId, sha1Bytes, err := readKeydir(br)
	if err != nil {
		return nil, err
	}

	if goldenSerialId != serialId {
		return nil, fmt.Errorf("serial Id in keydir file %d differs from requested serial id %d", serialId, goldenSerialId)
	}

	fsha, err := os.Open(filepath.Join(root, keydirSha1Filename))
	if err != nil {
		return nil, err
	}
	defer fsha.Close()

	brsha := bufio.NewReader(fsha)

	goldenSha1Bytes := make([]byte, sha1.Size)

	_, err = io.ReadFull(brsha, goldenSha1Bytes)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(sha1Bytes, goldenSha1Bytes) {
		return nil, fmt.Errorf("sha1 of keydir file differs from saved sha1")
	}

	return kd, nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
