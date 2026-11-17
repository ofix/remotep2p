package transfer

import (
	"fmt"
	"image"
	"image/jpeg"
	"os"
	"path/filepath"

	"golang.org/x/image/draw"
)

const ThumbSize = 256

func GenThumbnail(srcPath, dstPath string) error {
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		return err
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	tw, th := ThumbSize, ThumbSize
	if w > h {
		th = ThumbSize * h / w
	} else {
		tw = ThumbSize * w / h
	}
	dst := image.NewRGBA(image.Rect(0, 0, tw, th))
	draw.BiLinear.Scale(dst, dst.Bounds(), img, b, draw.Over, nil)
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()
	return jpeg.Encode(out, dst, &jpeg.Options{Quality: 70})
}

func ThumbPath(thumbDir, batchID string) string {
	return filepath.Join(thumbDir, fmt.Sprintf("%s.jpg", batchID))
}
