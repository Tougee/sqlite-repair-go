package repair

import (
	"encoding/binary"
	"errors"
	"math"
)

// readVarint reads a variable-length integer from buf.
func readVarint(buf []byte) (uint64, int) {
	var v uint64
	for i := 0; i < 9; i++ {
		if i >= len(buf) {
			return 0, 0
		}
		b := buf[i]
		v = (v << 7) | uint64(b&0x7f)
		if b < 0x80 {
			return v, i + 1
		}
	}
	if len(buf) >= 9 {
		return v, 9
	}
	return 0, 0
}

func parseRecord(payload []byte) ([]interface{}, error) {
	headerLen, n := readVarint(payload)
	if n == 0 || int(headerLen) > len(payload) {
		return nil, errors.New("invalid header length")
	}

	header := payload[n:headerLen]
	body := payload[headerLen:]

	var types []uint64
	idx := 0
	for idx < len(header) {
		t, n := readVarint(header[idx:])
		if n == 0 {
			break
		}
		types = append(types, t)
		idx += n
	}

	var values []interface{}
	bodyIdx := 0
	for _, t := range types {
		val, n, err := parseSerialType(t, body[bodyIdx:])
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		bodyIdx += n
	}

	return values, nil
}

func parseSerialType(t uint64, buf []byte) (interface{}, int, error) {
	switch t {
	case 0:
		return nil, 0, nil
	case 1: // 8-bit signed integer
		if len(buf) < 1 {
			return nil, 0, errors.New("buffer too short")
		}
		return int64(int8(buf[0])), 1, nil
	case 2: // 16-bit signed integer
		if len(buf) < 2 {
			return nil, 0, errors.New("buffer too short")
		}
		return int64(int16(binary.BigEndian.Uint16(buf))), 2, nil
	case 3: // 24-bit signed integer
		if len(buf) < 3 {
			return nil, 0, errors.New("buffer too short")
		}
		// Read 3 bytes as big-endian
		val := int64(int8(buf[0]))<<16 | int64(buf[1])<<8 | int64(buf[2])
		return val, 3, nil
	case 4: // 32-bit signed integer
		if len(buf) < 4 {
			return nil, 0, errors.New("buffer too short")
		}
		return int64(int32(binary.BigEndian.Uint32(buf))), 4, nil
	case 5: // 48-bit signed integer
		if len(buf) < 6 {
			return nil, 0, errors.New("buffer too short")
		}
		val := int64(int8(buf[0]))<<40 | int64(buf[1])<<32 | int64(binary.BigEndian.Uint32(buf[2:]))
		return val, 6, nil
	case 6: // 64-bit signed integer
		if len(buf) < 8 {
			return nil, 0, errors.New("buffer too short")
		}
		return int64(binary.BigEndian.Uint64(buf)), 8, nil
	case 7: // 64-bit IEEE floating point number
		if len(buf) < 8 {
			return nil, 0, errors.New("buffer too short")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(buf)), 8, nil
	case 8:
		return int64(0), 0, nil
	case 9:
		return int64(1), 0, nil
	}

	if t >= 12 && t%2 == 0 { // BLOB
		n := int((t - 12) / 2)
		if len(buf) < n {
			return nil, 0, errors.New("buffer too short for blob")
		}
		// Copy blob
		b := make([]byte, n)
		copy(b, buf[:n])
		return b, n, nil
	}

	if t >= 13 && t%2 == 1 { // String
		n := int((t - 13) / 2)
		if len(buf) < n {
			return nil, 0, errors.New("buffer too short for string")
		}
		return string(buf[:n]), n, nil
	}

	return nil, 0, errors.New("unknown serial type")
}
