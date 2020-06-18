// Copyright 2020 huiyi<yi.webmaster@hotmail.com>. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
// $Id: packettransceiver.go

package mysql

import (
	"bytes"
	"compress/zlib"
	"database/sql/driver"
	"net"
	"time"
)

// Packets documentation:
// http://dev.mysql.com/doc/internals/en/client-server-protocol.html

// Compression documentation:
// https://dev.mysql.com/doc/internals/en/compression.html

// MySql system variables related to packet size and compression:
//   net_buffer_length     : The packet message buffer bytes size is initialized to.
//   max_allow_packet  	   : The packet message buffer bytes size can grow up to.
//   net_compression_level : Zlib compression level for server.
//   protocol_compression_algorithms : The compression protocal, MySql 8.0 supports zlib and zstd.
// See: https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html

const (
	// Initialize buffer size for compression
	defaultCompressBufferSize = 1024 * 16
	// Initialize buffer size for decompression
	defaultDecompressBufferSize = 1024 * 16
	// Only affects send data
	defaultPacketCompressLevel = zlib.DefaultCompression
	// if packet size < minCompressSize, do not compress
	minCompressSize = 100
	// [0..2] length [3]byte; [3] sequence
	packetHeaderSize = 4
	// [0..2] payload [3]byte, [3] sync compression sequence; [4..6] uncompressed payload
	compressedHeaderSize = 7
)

/*
var (
	// Flag to enable / disable using zlib cgo
	UseZLibCgo = false
)
 */

// [0..2] payload length [3]byte, in little endian
// [3] sync compression sequence
// [4..6] uncompressed payload length [3]byte, 0 means uncompressed
type compressedPacketHeader [7]byte

type zlibCompressor interface {
	// Returns compressed length
	compress(input []byte, output *bytes.Buffer) (int, error)
}

type zlibDecompressor interface {
	// In mysql compress protocol, we always know the decompressed length for input
	decompress(input []byte, output []byte) error
}

type packetDecompressor struct {
	buffer       []byte // store decompressed bytes
	index        int
	decompressor zlibDecompressor
}

type packetCompressor struct {
	buffer     bytes.Buffer // store compressed bytes
	compressor zlibCompressor
}

type packetTransceiver struct {
	compressor   packetCompressor
	decompressor packetDecompressor
}

func sendToNetwork(conn net.Conn, data []byte) (int, error) {
	var err error
	var index int = 0

	for index < len(data) {
		var n int

		n, err = conn.Write(data[index:])
		index += n
		if err != nil {
			return index, err
		}
	}
	return len(data), nil
}

func (h compressedPacketHeader) payload() int {
	return int(uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16)
}

func (h compressedPacketHeader) sequence() uint8 {
	return uint8(h[3])
}

func (h compressedPacketHeader) uncompressedLength() int {
	return int(uint32(h[4]) | uint32(h[5])<<8 | uint32(h[6])<<16)
}

func (h *compressedPacketHeader) setPayload(payload int) {
	h[0] = byte(payload)
	h[1] = byte(payload >> 8)
	h[2] = byte(payload >> 16)
}

func (h *compressedPacketHeader) setSequence(sequence uint8) {
	h[3] = sequence
}

func (h *compressedPacketHeader) setUncompressedLength(uncompressedPayload int) {
	h[4] = byte(uncompressedPayload)
	h[5] = byte(uncompressedPayload >> 8)
	h[6] = byte(uncompressedPayload >> 16)
}

func (h *compressedPacketHeader) reset(header []byte) {
	copy(h[:], header)
}

func (pd *packetDecompressor) readNext(mc *mysqlConn, need int) ([]byte, error) {
	if len(pd.buffer)-pd.index < need {
		for {
			var err = pd.decompressPacket(mc)
			if err != nil {
				return nil, err
			}
			if len(pd.buffer)-pd.index >= need {
				break
			}
		}
	}
	var result []byte = pd.buffer[pd.index : pd.index+need]
	pd.index += need
	return result, nil
}

// Decompress one packet, and append the decompressed data to buffer
func (pd *packetDecompressor) decompressPacket(mc *mysqlConn) error {
	var err error
	var headerBuf []byte
	var header compressedPacketHeader
	var payload int
	var length int  // uncompressed length
	var data []byte // packet body
	// var decompressedData []byte

	// Initialize buffer
	if pd.buffer == nil {
		pd.buffer = make([]byte, 0, defaultDecompressBufferSize)
	}

	// If no data in the buffer, shrink it
	if pd.index > 0 && pd.index == len(pd.buffer) {
		pd.index = 0
		pd.buffer = pd.buffer[:0]
	}

	// Parse Header
	headerBuf, err = mc.buf.readNext(7)
	if err != nil {
		return err
	}
	header.reset(headerBuf)

	// Check sync sequence
	if header.sequence() != mc.compressionSequence {
		if header.sequence() > mc.compressionSequence {
			return ErrPktSyncMul
		}
		return ErrPktSync
	}
	mc.compressionSequence++

	// Read compressed packet data
	payload, length = header.payload(), header.uncompressedLength()
	data, err = mc.buf.readNext(payload)
	if err != nil {
		return err
	}

	// If payload is not compressed
	if length == 0 {
		pd.grow(payload)
		pd.buffer = append(pd.buffer, data...)
		return nil
	}

	// Ensure enough buffer for decompression
	pd.grow(length)

	// Initialize decompressor
	if pd.decompressor == nil {
		pd.decompressor = &sysZLibDecompressor{}
		/*
		if UseZLibCgo {
			pd.decompressor = &cgoZLibDecompressor{}
		} else {
			pd.decompressor = &sysZLibDecompressor{}
		}
		 */
	}

	// Decompress data
	err = pd.decompressor.decompress(data, pd.buffer[len(pd.buffer):len(pd.buffer)+length])
	if err != nil {
		return err
	}

	// Reset buffer length
	pd.buffer = pd.buffer[:len(pd.buffer)+length]
	return nil
}

// Ensures enough space for append packet data to buffer
func (pd *packetDecompressor) grow(dataLength int) {
	// If there's no room for packet data
	if len(pd.buffer)+dataLength > cap(pd.buffer) {
		// Buffer capacity is enough, shrink it
		if len(pd.buffer)-pd.index+dataLength <= cap(pd.buffer) {
			var newLength int = len(pd.buffer) - pd.index
			copy(pd.buffer[:newLength], pd.buffer[pd.index:])
			pd.buffer = pd.buffer[:newLength]
			pd.index = 0
		} else {
			// Allocate new buffer
			var newBuffer = make([]byte, 0, len(pd.buffer)-pd.index+dataLength)
			pd.buffer = append(newBuffer, pd.buffer[pd.index:len(pd.buffer)]...)
			pd.index = 0
		}
	}
}

// The very very rarely case is len(packet) = len(packet header) + len(packet data) > 0xFFFFFF, but can not be
// compressed. In this case, the origin packet will be splited into 2 compressed packets:
// Packet 1: {
//         compressed payload: 0xffffff; compressed sequence; uncompressed payload: 0;  data part 1
// }
// Packet 2: {
//         compressed payload: len(packet) - 0xffffff; compressed sequence; uncompressed payload: 0;  data part 2
// }
func (pc *packetCompressor) writePacket(mc *mysqlConn, packet []byte) (int, error) {
	if len(packet) == 0 {
		return 0, nil
	}

	var index = 0

	for index < len(packet) {
		var err error
		var remain = len(packet) - index
		var bytesWritten int

		bytesWritten, err = pc.writeToBuffer(packet[index:], mc.compressionSequence, remain < minCompressSize)
		if err != nil {
			return index, err
		}

		if mc.writeTimeout > 0 {
			if err := mc.netConn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
				return index, err
			}
		}

		// Send compressed packet
		n, err := sendToNetwork(mc.netConn, pc.buffer.Bytes())
		if err != nil {
			return index, err
		}
		if n != pc.buffer.Len() {
			return index, ErrInvalidConn
		}

		mc.compressionSequence++
		index += bytesWritten
	}
	return len(packet), nil
}

func (pc *packetCompressor) writeToBuffer(data []byte, sequence uint8, compress bool) (int, error) {
	var err error
	var header compressedPacketHeader
	var compressedLength int

	// Initialize result buffer
	if pc.buffer.Cap() == 0 {
		pc.buffer.Grow(defaultCompressBufferSize)
	}

	// Reuse result buffer
	pc.buffer.Reset()

	// If do not need compress, copy data directly
	if !compress {
		var size = len(data)
		if size >= maxPacketSize {
			size = maxPacketSize
		}
		header.setPayload(size)
		header.setSequence(sequence)
		// If not compressed, set uncompressed payload to 0
		header.setUncompressedLength(0)
		_, err = pc.buffer.Write(header[:])
		if err != nil {
			return 0, err
		}
		_, err = pc.buffer.Write(data[:size])
		if err != nil {
			return 0, err
		}
		return size, nil
	}

	// Initialize compressor
	if pc.compressor == nil {
		pc.compressor = &sysZLibCompressor{}
		/*
		if UseZLibCgo {
			pc.compressor = &cgoZLibCompressor{}
		} else {
			pc.compressor = &sysZLibCompressor{}
		}
		 */
	}

	// Keep memory for compressed header
	pc.buffer.Write(header[:])

	// Compress data
	compressedLength, err = pc.compressor.compress(data, &pc.buffer)

	// If compression is not helpful, do not compress
	if compressedLength > maxPacketSize || compressedLength > len(data) {
		return pc.writeToBuffer(data, sequence, false)
	}

	// Write header
	header.setPayload(compressedLength)
	header.setSequence(sequence)
	header.setUncompressedLength(len(data))
	copy(pc.buffer.Bytes(), header[:])

	return len(data), nil
}

// Read packet to buffer 'data'
func (pb *packetTransceiver) readPacket(mc *mysqlConn, compress bool) ([]byte, error) {
	var prevData []byte
	for {
		var err error
		var data []byte
		// read packet header
		if compress {
			data, err = pb.decompressor.readNext(mc, 4)
		} else {
			data, err = mc.buf.readNext(4)
		}

		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		// packet length [24 bit]
		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

		// check packet sync [8 bit]
		if data[3] != mc.sequence {
			if data[3] > mc.sequence {
				return nil, ErrPktSyncMul
			}
			return nil, ErrPktSync
		}
		mc.sequence++

		// packets with length 0 terminate a previous packet which is a
		// multiple of (2^24)-1 bytes long
		if pktLen == 0 {
			// there was no previous packet
			if prevData == nil {
				errLog.Print(ErrMalformPkt)
				mc.Close()
				return nil, ErrInvalidConn
			}

			return prevData, nil
		}

		// read packet body [pktLen bytes]
		if compress {
			data, err = pb.decompressor.readNext(mc, pktLen)
		} else {
			data, err = mc.buf.readNext(pktLen)
		}

		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		// return data if this was the last packet
		if pktLen < maxPacketSize {
			// zero allocations for non-split packets
			if prevData == nil {
				return data, nil
			}

			return append(prevData, data...), nil
		}

		prevData = append(prevData, data...)
	}
}

// Write packet buffer 'data'
func (pb *packetTransceiver) writePacket(mc *mysqlConn, data []byte, compress bool) error {
	pktLen := len(data) - 4

	if pktLen > mc.maxAllowedPacket {
		return ErrPktTooLarge
	}

	// Perform a stale connection check. We only perform this check for
	// the first query on a connection that has been checked out of the
	// connection pool: a fresh connection from the pool is more likely
	// to be stale, and it has not performed any previous writes that
	// could cause data corruption, so it's safe to return ErrBadConn
	// if the check fails.
	if mc.reset {
		mc.reset = false
		conn := mc.netConn
		if mc.rawConn != nil {
			conn = mc.rawConn
		}
		var err error
		// If this connection has a ReadTimeout which we've been setting on
		// reads, reset it to its default value before we attempt a non-blocking
		// read, otherwise the scheduler will just time us out before we can read
		if mc.cfg.ReadTimeout != 0 {
			err = conn.SetReadDeadline(time.Time{})
		}
		if err == nil && mc.cfg.CheckConnLiveness {
			err = connCheck(conn)
		}
		if err != nil {
			errLog.Print("closing bad idle connection: ", err)
			mc.Close()
			return driver.ErrBadConn
		}
	}

	for {
		var size int
		if pktLen >= maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = maxPacketSize
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		data[3] = mc.sequence

		var err error
		var n int
		if !compress {
			// Write packet
			if mc.writeTimeout > 0 {
				if err := mc.netConn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
					return err
				}
			}
			n, err = sendToNetwork(mc.netConn, data[:4+size])
		} else {
			n, err = pb.compressor.writePacket(mc, data[:4+size])
		}

		if err == nil && n == 4+size {
			mc.sequence++
			if size != maxPacketSize {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

		// Handle error
		if err == nil { // n != len(data)
			mc.cleanup()
			errLog.Print(ErrMalformPkt)
		} else {
			if cerr := mc.canceled.Value(); cerr != nil {
				return cerr
			}
			if n == 0 && pktLen == len(data)-4 {
				// only for the first loop iteration when nothing was written yet
				return errBadConnNoWrite
			}
			mc.cleanup()
			errLog.Print(err)
		}
		return ErrInvalidConn
	}
}
