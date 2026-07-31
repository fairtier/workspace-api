package duckflight

import (
	"encoding/base64"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	dateFormat = "2006-01-02"
	timeFormat = "15:04:05.999999999"
)

// rowValue converts one Arrow cell into a JSON-marshalable Go value for the
// Console grid. The contract (mirrored in query.proto):
//   - int64/uint64 and decimals are JSON *strings* — a JS client parses row
//     JSON with number = float64, which silently corrupts values past 2^53
//   - timestamps are RFC 3339, dates "2006-01-02", times "15:04:05[.ns]"
//   - binary is base64
//   - everything else defers to Arrow's own GetOneForMarshal: bools, small
//     ints and floats stay JSON numbers/bools, strings stay strings, decimals
//     come back scale-formatted, and list/struct/map nest naturally. (Known
//     caveat: int64 *inside* a nested value stays a JSON number.)
func rowValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch arr := col.(type) {
	case *array.Int64:
		return strconv.FormatInt(arr.Value(i), 10)
	case *array.Uint64:
		return strconv.FormatUint(arr.Value(i), 10)
	}
	if v, ok := temporalValue(col, i); ok {
		return v
	}
	if v, ok := binaryValue(col, i); ok {
		return v
	}
	return col.GetOneForMarshal(i)
}

// temporalValue formats timestamp/date/time cells; ok is false for other types.
func temporalValue(col arrow.Array, i int) (any, bool) {
	switch arr := col.(type) {
	case *array.Timestamp:
		if toTime, err := arr.DataType().(*arrow.TimestampType).GetToTimeFunc(); err == nil {
			return toTime(arr.Value(i)).Format(time.RFC3339Nano), true
		}
		return arr.ValueStr(i), true
	case *array.Date32:
		return arr.Value(i).ToTime().Format(dateFormat), true
	case *array.Date64:
		return arr.Value(i).ToTime().Format(dateFormat), true
	case *array.Time32:
		return arr.Value(i).ToTime(arr.DataType().(*arrow.Time32Type).Unit).Format(timeFormat), true
	case *array.Time64:
		return arr.Value(i).ToTime(arr.DataType().(*arrow.Time64Type).Unit).Format(timeFormat), true
	default:
		return nil, false
	}
}

// binaryValue base64-encodes binary cells; ok is false for other types.
func binaryValue(col arrow.Array, i int) (any, bool) {
	switch arr := col.(type) {
	case *array.Binary:
		return base64.StdEncoding.EncodeToString(arr.Value(i)), true
	case *array.LargeBinary:
		return base64.StdEncoding.EncodeToString(arr.Value(i)), true
	case *array.FixedSizeBinary:
		return base64.StdEncoding.EncodeToString(arr.Value(i)), true
	default:
		return nil, false
	}
}
