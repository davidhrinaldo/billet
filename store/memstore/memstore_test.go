package memstore

import (
	"testing"

	"github.com/davidhrinaldo/billet/store"
	"github.com/davidhrinaldo/billet/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Suite(t, func(t *testing.T) (store.Store, func()) {
		return New(), func() {}
	})
}
