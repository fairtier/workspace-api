package duckflight

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildArray constructs a one-or-two element array of the given type via a
// RecordBuilder so the test exercises the same array kinds a real FlightSQL
// stream yields.
func buildArray(t *testing.T, dt arrow.DataType, appendVal func(b array.Builder)) arrow.Array {
	t.Helper()
	pool := memory.NewGoAllocator()
	b := array.NewBuilder(pool, dt)
	defer b.Release()
	appendVal(b)
	arr := b.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

// asJSON runs rowValue and marshals it, which is exactly what the handler
// does per row.
func asJSON(t *testing.T, arr arrow.Array, i int) string {
	t.Helper()
	out, err := json.Marshal(rowValue(arr, i))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(out)
}

func TestRowValue(t *testing.T) {
	tests := []struct {
		name   string
		dt     arrow.DataType
		append func(b array.Builder)
		want   string
	}{
		{
			name:   "int32 stays number",
			dt:     arrow.PrimitiveTypes.Int32,
			append: func(b array.Builder) { b.(*array.Int32Builder).Append(-42) },
			want:   `-42`,
		},
		{
			name:   "int64 becomes string",
			dt:     arrow.PrimitiveTypes.Int64,
			append: func(b array.Builder) { b.(*array.Int64Builder).Append(9007199254740993) }, // 2^53+1
			want:   `"9007199254740993"`,
		},
		{
			name:   "uint64 becomes string",
			dt:     arrow.PrimitiveTypes.Uint64,
			append: func(b array.Builder) { b.(*array.Uint64Builder).Append(18446744073709551615) },
			want:   `"18446744073709551615"`,
		},
		{
			name:   "float64 stays number",
			dt:     arrow.PrimitiveTypes.Float64,
			append: func(b array.Builder) { b.(*array.Float64Builder).Append(1.5) },
			want:   `1.5`,
		},
		{
			name:   "bool stays bool",
			dt:     arrow.FixedWidthTypes.Boolean,
			append: func(b array.Builder) { b.(*array.BooleanBuilder).Append(true) },
			want:   `true`,
		},
		{
			name:   "string stays string",
			dt:     arrow.BinaryTypes.String,
			append: func(b array.Builder) { b.(*array.StringBuilder).Append("alice") },
			want:   `"alice"`,
		},
		{
			name:   "binary becomes base64",
			dt:     arrow.BinaryTypes.Binary,
			append: func(b array.Builder) { b.(*array.BinaryBuilder).Append([]byte{0xde, 0xad}) },
			want:   `"3q0="`,
		},
		{
			name: "decimal128 keeps scale as string",
			dt:   &arrow.Decimal128Type{Precision: 18, Scale: 2},
			append: func(b array.Builder) {
				b.(*array.Decimal128Builder).Append(decimal128.FromI64(12345)) // 123.45
			},
			want: `"123.45"`,
		},
		{
			name: "timestamp is RFC3339",
			dt:   &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			append: func(b array.Builder) {
				b.(*array.TimestampBuilder).Append(arrow.Timestamp(1767225600000000)) // 2026-01-01T00:00:00Z
			},
			want: `"2026-01-01T00:00:00Z"`,
		},
		{
			name:   "date32 is yyyy-mm-dd",
			dt:     arrow.FixedWidthTypes.Date32,
			append: func(b array.Builder) { b.(*array.Date32Builder).Append(arrow.Date32(20454)) }, // 2026-01-01
			want:   `"2026-01-01"`,
		},
		{
			name: "time64 is clock time",
			dt:   arrow.FixedWidthTypes.Time64us,
			append: func(b array.Builder) {
				b.(*array.Time64Builder).Append(arrow.Time64(12*3600e6 + 30*60e6)) // 12:30:00
			},
			want: `"12:30:00"`,
		},
		{
			name: "list nests as JSON array",
			dt:   arrow.ListOf(arrow.PrimitiveTypes.Int32),
			append: func(b array.Builder) {
				lb := b.(*array.ListBuilder)
				lb.Append(true)
				vb := lb.ValueBuilder().(*array.Int32Builder)
				vb.Append(1)
				vb.Append(2)
			},
			want: `[1,2]`,
		},
		{
			name: "struct nests as JSON object",
			dt:   arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32}),
			append: func(b array.Builder) {
				sb := b.(*array.StructBuilder)
				sb.Append(true)
				sb.FieldBuilder(0).(*array.Int32Builder).Append(7)
			},
			want: `{"a":7}`,
		},
		{
			name:   "null is JSON null",
			dt:     arrow.PrimitiveTypes.Int64,
			append: func(b array.Builder) { b.(*array.Int64Builder).AppendNull() },
			want:   `null`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := buildArray(t, tt.dt, tt.append)
			if got := asJSON(t, arr, 0); got != tt.want {
				t.Errorf("rowValue JSON = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNormalizeAddr(t *testing.T) {
	tests := []struct {
		in, want      string
		wantPlaintext bool
	}{
		{in: "grpc://duckflight.customer-a.fairtier.com:443", want: "duckflight.customer-a.fairtier.com:443"},
		{in: "https://duckflight.customer-a.fairtier.com", want: "duckflight.customer-a.fairtier.com:443"},
		{in: "https://duckflight.customer-a.fairtier.com/", want: "duckflight.customer-a.fairtier.com:443"},
		{in: "duckflight.customer-a.fairtier.com", want: "duckflight.customer-a.fairtier.com:443"},
		{in: "localhost:31337", want: "localhost:31337"},
		// The dial-override shape: the box's in-cluster Service, which is the
		// h2c backend with no TLS-terminating edge in front of it.
		{in: "http://duckflight:31337", want: "duckflight:31337", wantPlaintext: true},
		{in: "http://duckflight:31337/", want: "duckflight:31337", wantPlaintext: true},
		// Plaintext with no port is :80, not the TLS default.
		{in: "http://duckflight", want: "duckflight:80", wantPlaintext: true},
		// grpc:// stays TLS: it is what the shared substrate already stores.
		{in: "grpc+tls://duckflight.customer-a.fairtier.com:443", want: "duckflight.customer-a.fairtier.com:443"},
	}
	for _, tt := range tests {
		got, plaintext := normalizeAddr(tt.in)
		if got != tt.want || plaintext != tt.wantPlaintext {
			t.Errorf("normalizeAddr(%q) = (%q, %v), want (%q, %v)", tt.in, got, plaintext, tt.want, tt.wantPlaintext)
		}
	}
}

// TestBearerCredsFollowTransport pins the reason bearerCreds carries a flag at
// all: gRPC refuses per-RPC credentials demanding transport security over an
// insecure connection, so a plaintext dial that kept requireTLS would fail
// before the token ever left the process.
func TestBearerCredsFollowTransport(t *testing.T) {
	tls := bearerCreds{token: "t", requireTLS: true}
	if !tls.RequireTransportSecurity() {
		t.Error("a TLS dial must still demand transport security")
	}
	plain := bearerCreds{token: "t", requireTLS: false}
	if plain.RequireTransportSecurity() {
		t.Error("a plaintext dial must not demand transport security")
	}
}
