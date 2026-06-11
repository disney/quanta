package server

import "testing"

func TestIsBSIBitmapPathUsesPathShape(t *testing.T) {
	if !isBSIBitmapPath([]string{"customers_qa", "cust_id", "bsi", "default", "1"}) {
		t.Fatal("expected bsi path to be classified as BSI")
	}

	if isBSIBitmapPath([]string{"customers_qa", "cust_id", "0", "1970-01-01T00"}) {
		t.Fatal("expected row-id bitmap path under a BSI field to be classified as standard")
	}
}
