package main

import (
	"github.com/jackc/pgx/v5/pgtype"
	"testing"
)

// Run this file explicitly: full web package init connects to live providers.
func TestRawSharePrecisionNumericCopyEncoding(t *testing.T) {
	m := pgtype.NewMap()
	for _, v := range []float64{0, 925050, 925050.5} {
		encoded, err := m.Encode(pgtype.NumericOID, pgtype.BinaryFormatCode, v, nil)
		if err != nil {
			t.Fatal(err)
		}
		var back float64
		if err = m.Scan(pgtype.NumericOID, pgtype.BinaryFormatCode, encoded, &back); err != nil || back != v {
			t.Fatalf("numeric COPY precision: %v %v", back, err)
		}
	}
}
