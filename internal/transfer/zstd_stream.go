package transfer

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

func NewZstdWriter(w io.Writer) (*zstd.Encoder, error) {
	return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
}

func NewZstdReader(r io.Reader) (*zstd.Decoder, error) {
	return zstd.NewReader(r)
}
