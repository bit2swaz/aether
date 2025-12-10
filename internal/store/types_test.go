package store

import (
	"testing"
	"time"
)

func TestEncodeValue(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expectedOID uint32
		expectedVal string
	}{
		{
			name:        "nil value",
			input:       nil,
			expectedOID: ^uint32(0),
			expectedVal: "",
		},
		{
			name:        "int value",
			input:       42,
			expectedOID: OIDInt8,
			expectedVal: "42",
		},
		{
			name:        "int32 value",
			input:       int32(123),
			expectedOID: OIDInt8,
			expectedVal: "123",
		},
		{
			name:        "int64 value",
			input:       int64(9223372036854775807),
			expectedOID: OIDInt8,
			expectedVal: "9223372036854775807",
		},
		{
			name:        "negative int64",
			input:       int64(-12345),
			expectedOID: OIDInt8,
			expectedVal: "-12345",
		},
		{
			name:        "float64 value",
			input:       3.14159,
			expectedOID: OIDFloat8,
			expectedVal: "3.141590",
		},
		{
			name:        "string value",
			input:       "Hello, Aether!",
			expectedOID: OIDText,
			expectedVal: "Hello, Aether!",
		},
		{
			name:        "empty string",
			input:       "",
			expectedOID: OIDText,
			expectedVal: "",
		},
		{
			name:        "byte slice",
			input:       []byte{0x01, 0x02, 0x03, 0xFF},
			expectedOID: OIDBytea,
			expectedVal: "\x01\x02\x03\xff",
		},
		{
			name:        "empty byte slice",
			input:       []byte{},
			expectedOID: OIDBytea,
			expectedVal: "",
		},
		{
			name:        "time.Time value",
			input:       time.Date(2025, 12, 10, 15, 30, 0, 0, time.UTC),
			expectedOID: OIDTimestamp,
			expectedVal: "2025-12-10T15:30:00Z",
		},
		{
			name:        "bool (fallback to text)",
			input:       true,
			expectedOID: OIDText,
			expectedVal: "true",
		},
		{
			name:        "struct (fallback to text)",
			input:       struct{ Name string }{Name: "test"},
			expectedOID: OIDText,
			expectedVal: "{test}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oid, encoded, err := encodeValue(tt.input)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if oid != tt.expectedOID {
				t.Errorf("Expected OID %d, got %d", tt.expectedOID, oid)
			}

			if string(encoded) != tt.expectedVal {
				t.Errorf("Expected encoded value %q, got %q", tt.expectedVal, string(encoded))
			}

			t.Logf("%s: OID=%d, value=%q", tt.name, oid, string(encoded))
		})
	}
}

func TestEncodeValueOIDConstants(t *testing.T) {
	if OIDBytea != 17 {
		t.Errorf("OIDBytea should be 17, got %d", OIDBytea)
	}
	if OIDInt8 != 20 {
		t.Errorf("OIDInt8 should be 20, got %d", OIDInt8)
	}
	if OIDText != 25 {
		t.Errorf("OIDText should be 25, got %d", OIDText)
	}
	if OIDFloat8 != 701 {
		t.Errorf("OIDFloat8 should be 701, got %d", OIDFloat8)
	}
	if OIDTimestamp != 1114 {
		t.Errorf("OIDTimestamp should be 1114, got %d", OIDTimestamp)
	}

	t.Log("✓ All OID constants are correct")
}
