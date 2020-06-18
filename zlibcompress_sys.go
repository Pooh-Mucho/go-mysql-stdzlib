// Copyright 2020 huiyi<yi.webmaster@hotmail.com>. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
// $Id: zlibcompress_sys.go

package mysql

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"io"
)

// Compress & decompress mysql packet using golang zlib

type sysZLibCompressor struct {
	zlibWriter *zlib.Writer
}

type sysZLibDecompressor struct {
	bytesReader    bytes.Reader
	bufferedReader bufio.Reader // Make zlib.flate.makeReader happy, but 1 more time memory copy!!
	zlibReader     io.ReadCloser
}

func (zc *sysZLibCompressor) compress(input []byte, output *bytes.Buffer) (int, error) {
	var err error
	var lenSave int

	// Initialize zlib writer
	if zc.zlibWriter == nil {
		zc.zlibWriter, err = zlib.NewWriterLevel(output, defaultPacketCompressLevel)
	} else {
		// Reuse zlib writer
		zc.zlibWriter.Reset(output)
	}
	if err != nil {
		return 0, err
	}

	// Save old length
	lenSave = output.Len()

	// Compress data
	for len(input) > 0 {
		var n int
		n, err = zc.zlibWriter.Write(input)
		if err != nil {
			return 0, err
		}
		input = input[n:]
	}
	err = zc.zlibWriter.Close()
	if err != nil {
		return 0, err
	}

	// Returns compressed length
	return output.Len() - lenSave, nil
}

// In mysql compress protocol, we always know the decompressed length for input
func (zd *sysZLibDecompressor) decompress(input []byte, output []byte) error {
	var err error

	// Reuse bytes.Buffer
	zd.bytesReader.Reset(input)

	// Initialize zlib reader
	if zd.zlibReader == nil {
		zd.bufferedReader = *bufio.NewReaderSize(&zd.bytesReader, 512)
		zd.zlibReader, err = zlib.NewReader(&zd.bufferedReader)
	} else {
		// Reuse zlib reader
		zd.bufferedReader.Reset(&zd.bytesReader)
		err = zd.zlibReader.(zlib.Resetter).Reset(&zd.bufferedReader, nil)
	}
	if  err != nil {
		return err
	}

	// n of io.ReadFull = len(output) when err = null, so the return value can be ignored
	_, err = io.ReadFull(zd.zlibReader, output)
	if err != nil {
		return err
	}

	return nil
}
