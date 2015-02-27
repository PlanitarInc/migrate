package driver

import (
	"testing"
)

func TestNew(t *testing.T) {
	if _, err := New(nil, "unknown://url"); err == nil {
		t.Error("no error although driver unknown")
	}
}
