package shadow

import (
	"testing"

	"github.com/davidhrinaldo/billet/hlc"
)

func TestComputeDelta(t *testing.T) {
	tests := []struct {
		name     string
		reported Section
		desired  Section
		want     Delta
	}{
		{
			name:     "empty sections produce empty delta",
			reported: Section{Values: map[string]Value{}},
			desired:  Section{Values: map[string]Value{}},
			want:     Delta{Diffs: map[string]Diff{}},
		},
		{
			name: "desired matches reported produces empty delta",
			reported: Section{Values: map[string]Value{
				"temp": {Data: []byte("22"), Timestamp: hlc.Timestamp{Physical: 100}},
			}},
			desired: Section{Values: map[string]Value{
				"temp": {Data: []byte("22"), Timestamp: hlc.Timestamp{Physical: 200}},
			}},
			want: Delta{Diffs: map[string]Diff{}},
		},
		{
			name:     "desired key not in reported appears in delta",
			reported: Section{Values: map[string]Value{}},
			desired: Section{Values: map[string]Value{
				"mode": {Data: []byte("auto"), Timestamp: hlc.Timestamp{Physical: 100}},
			}},
			want: Delta{Diffs: map[string]Diff{
				"mode": {Key: "mode", Desired: []byte("auto"), Reported: nil},
			}},
		},
		{
			name: "desired differs from reported appears in delta",
			reported: Section{Values: map[string]Value{
				"mode": {Data: []byte("manual"), Timestamp: hlc.Timestamp{Physical: 50}},
			}},
			desired: Section{Values: map[string]Value{
				"mode": {Data: []byte("auto"), Timestamp: hlc.Timestamp{Physical: 100}},
			}},
			want: Delta{Diffs: map[string]Diff{
				"mode": {Key: "mode", Desired: []byte("auto"), Reported: []byte("manual")},
			}},
		},
		{
			name: "reported key not in desired does not appear in delta",
			reported: Section{Values: map[string]Value{
				"temp": {Data: []byte("22"), Timestamp: hlc.Timestamp{Physical: 100}},
			}},
			desired: Section{Values: map[string]Value{}},
			want:    Delta{Diffs: map[string]Diff{}},
		},
		{
			name: "multiple keys mixed",
			reported: Section{Values: map[string]Value{
				"a": {Data: []byte("1"), Timestamp: hlc.Timestamp{Physical: 10}},
				"b": {Data: []byte("2"), Timestamp: hlc.Timestamp{Physical: 10}},
				"c": {Data: []byte("3"), Timestamp: hlc.Timestamp{Physical: 10}},
			}},
			desired: Section{Values: map[string]Value{
				"a": {Data: []byte("1"), Timestamp: hlc.Timestamp{Physical: 20}},  // same value
				"b": {Data: []byte("99"), Timestamp: hlc.Timestamp{Physical: 20}}, // different
				"d": {Data: []byte("4"), Timestamp: hlc.Timestamp{Physical: 20}},  // new
			}},
			want: Delta{Diffs: map[string]Diff{
				"b": {Key: "b", Desired: []byte("99"), Reported: []byte("2")},
				"d": {Key: "d", Desired: []byte("4"), Reported: nil},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeDelta(tt.reported, tt.desired)

			if len(got.Diffs) != len(tt.want.Diffs) {
				t.Fatalf("got %d diffs, want %d\ngot:  %v\nwant: %v", len(got.Diffs), len(tt.want.Diffs), got.Diffs, tt.want.Diffs)
			}

			for key, wantDiff := range tt.want.Diffs {
				gotDiff, ok := got.Diffs[key]
				if !ok {
					t.Errorf("missing diff for key %q", key)
					continue
				}
				if gotDiff.Key != wantDiff.Key {
					t.Errorf("diff[%q].Key = %q, want %q", key, gotDiff.Key, wantDiff.Key)
				}
				if string(gotDiff.Desired) != string(wantDiff.Desired) {
					t.Errorf("diff[%q].Desired = %q, want %q", key, gotDiff.Desired, wantDiff.Desired)
				}
				if string(gotDiff.Reported) != string(wantDiff.Reported) {
					t.Errorf("diff[%q].Reported = %q, want %q", key, gotDiff.Reported, wantDiff.Reported)
				}
			}
		})
	}
}

func TestDocumentDelta(t *testing.T) {
	doc := &Document{
		DeviceID: "dev-1",
		Reported: Section{Values: map[string]Value{
			"temp": {Data: []byte("22")},
		}},
		Desired: Section{Values: map[string]Value{
			"temp": {Data: []byte("25")},
		}},
	}

	delta := doc.Delta()

	if len(delta.Diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(delta.Diffs))
	}

	d, ok := delta.Diffs["temp"]
	if !ok {
		t.Fatal("missing diff for key 'temp'")
	}
	if string(d.Desired) != "25" {
		t.Errorf("desired = %q, want %q", d.Desired, "25")
	}
	if string(d.Reported) != "22" {
		t.Errorf("reported = %q, want %q", d.Reported, "22")
	}
}
