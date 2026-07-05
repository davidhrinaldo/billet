package memhistory

import (
	"testing"

	"github.com/davidhrinaldo/billet/history"
	"github.com/davidhrinaldo/billet/history/historytest"
)

func TestConformance(t *testing.T) {
	historytest.Suite(t, func(t *testing.T) (history.History, func()) {
		return New(), func() {}
	})
}
