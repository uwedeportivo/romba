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
	"github.com/uwedeportivo/romba/util"
)

const (
	KeySizeCrc  = 4
	KeySizeMd5  = 16
	KeySizeSha1 = 20
)

func (ar *Rom) CrcWithSizeKey() []byte {
	if ar.Crc == nil {
		return nil
	}

	n := KeySizeCrc
	key := make([]byte, n+8)
	copy(key[:n], ar.Crc)
	util.Int64ToBytes(ar.Size, key[n:])
	return key
}

func (ar *Rom) Md5WithSizeKey() []byte {
	if ar.Md5 == nil {
		return nil
	}

	n := KeySizeMd5
	key := make([]byte, n+8)
	copy(key[:n], ar.Md5)
	util.Int64ToBytes(ar.Size, key[n:])
	return key
}

func (ar *Rom) CrcWithSizeAndSha1Key(sha1Bytes []byte) []byte {
	if sha1Bytes == nil {
		sha1Bytes = ar.Sha1
	}

	if ar.Crc == nil || sha1Bytes == nil {
		return nil
	}

	key := make([]byte, KeySizeCrc+8+KeySizeSha1)
	copy(key[:KeySizeCrc], ar.Crc)
	util.Int64ToBytes(ar.Size, key[KeySizeCrc:KeySizeCrc+8])
	copy(key[KeySizeCrc+8:], sha1Bytes)
	return key
}

func (ar *Rom) Md5WithSizeAndSha1Key(sha1Bytes []byte) []byte {
	if sha1Bytes == nil {
		sha1Bytes = ar.Sha1
	}

	if ar.Md5 == nil || sha1Bytes == nil {
		return nil
	}

	key := make([]byte, KeySizeMd5+8+KeySizeSha1)
	copy(key[:KeySizeMd5], ar.Md5)
	util.Int64ToBytes(ar.Size, key[KeySizeMd5:KeySizeMd5+8])
	copy(key[KeySizeMd5+8:], sha1Bytes)
	return key
}

func (ar *Rom) Sha1Sha1Key(sha1Bytes []byte) []byte {
	if ar.Sha1 == nil || sha1Bytes == nil {
		return nil
	}

	key := make([]byte, KeySizeSha1*2)
	copy(key[:KeySizeSha1], ar.Sha1)
	copy(key[KeySizeSha1:], sha1Bytes)
	return key
}
