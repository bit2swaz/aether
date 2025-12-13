package store

import (
	"fmt"
	"time"
)

const (
	OIDBytea     uint32 = 17
	OIDInt8      uint32 = 20
	OIDText      uint32 = 25
	OIDFloat8    uint32 = 701
	OIDTimestamp uint32 = 1114
)

func encodeValue(val any) (oid uint32, encoded []byte, err error) {
	if val == nil {
		return ^uint32(0), nil, nil
	}

	switch v := val.(type) {
	case int:
		return OIDInt8, []byte(fmt.Sprintf("%d", v)), nil

	case int32:
		return OIDInt8, []byte(fmt.Sprintf("%d", v)), nil

	case int64:
		return OIDInt8, []byte(fmt.Sprintf("%d", v)), nil

	case float64:
		return OIDFloat8, []byte(fmt.Sprintf("%f", v)), nil

	case string:
		return OIDText, []byte(v), nil

	case []byte:
		return OIDBytea, v, nil

	case time.Time:
		return OIDTimestamp, []byte(v.Format(time.RFC3339)), nil

	default:
		return OIDText, []byte(fmt.Sprint(v)), nil
	}
}
