package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

const HeaderSize = 16

const (
	FrameHandshake  uint16 = 1
	FrameClipText   uint16 = 2
	FrameClipImage  uint16 = 3
	FrameClipFiles  uint16 = 4
	FrameTaskReq    uint16 = 5
	FrameTaskResume uint16 = 6
	FrameTaskAck    uint16 = 7
	FrameTaskDone   uint16 = 8
	FrameTaskErr    uint16 = 9
	FrameDataChunk  uint16 = 100
)

const (
	FlagIsFolder   uint16 = 1 << 0
	FlagResume     uint16 = 1 << 1
	FlagCompressed uint16 = 1 << 2
)

type Frame struct {
	FrameType uint16
	BatchID   [4]byte
	BodyLen   uint64
	Flags     uint16
	Body      []byte
}

type TaskReqPayload struct {
	Name        string `json:"name"`
	Total       int64  `json:"total"`
	Sha256      string `json:"sha256"`
	IsFolder    bool   `json:"is_folder"`
	ClaimOffset int64  `json:"claim_offset"`
}

type TaskResumePayload struct {
	SafeOffset int64 `json:"safe_offset"`
}

func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	ft := binary.LittleEndian.Uint16(hdr[0:2])
	var bid [4]byte
	copy(bid[:], hdr[2:6])
	bl := binary.LittleEndian.Uint64(hdr[6:14])
	fl := binary.LittleEndian.Uint16(hdr[14:16])
	body := make([]byte, bl)
	if bl > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
	}
	return &Frame{
		FrameType: ft,
		BatchID:   bid,
		BodyLen:   bl,
		Flags:     fl,
		Body:      body,
	}, nil
}

func WriteFrame(w io.Writer, f *Frame) error {
	if uint64(len(f.Body)) != f.BodyLen {
		return errors.New("body length mismatch")
	}
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint16(hdr[0:2], f.FrameType)
	copy(hdr[2:6], f.BatchID[:])
	binary.LittleEndian.PutUint64(hdr[6:14], f.BodyLen)
	binary.LittleEndian.PutUint16(hdr[14:16], f.Flags)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if f.BodyLen > 0 {
		if _, err := w.Write(f.Body); err != nil {
			return err
		}
	}
	return nil
}
