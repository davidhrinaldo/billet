package resolver

import (
	"testing"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
)

func emptyDoc(deviceID string) *shadow.Document {
	return &shadow.Document{
		DeviceID: deviceID,
		Reported: shadow.Section{Values: map[string]shadow.Value{}},
		Desired:  shadow.Section{Values: map[string]shadow.Value{}},
	}
}

func TestSectionAuthority_Resolve(t *testing.T) {
	tests := []struct {
		name       string
		doc        *shadow.Document
		op         shadow.Op
		wantErr    error
		wantKey    string
		wantData   string
		wantSection shadow.SectionType
	}{
		{
			name: "device writes reported accepted",
			doc:  emptyDoc("dev-1"),
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 1},
				DeviceID:  "dev-1",
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("22"),
				Timestamp: hlc.Timestamp{Physical: 100, NodeID: 1},
			},
			wantErr:     nil,
			wantKey:     "temp",
			wantData:    "22",
			wantSection: shadow.SectionReported,
		},
		{
			name: "controller writes desired accepted",
			doc:  emptyDoc("dev-1"),
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 100, Seq: 1},
				DeviceID:  "dev-1",
				Section:   shadow.SectionDesired,
				Key:       "mode",
				Data:      []byte("auto"),
				Timestamp: hlc.Timestamp{Physical: 200, NodeID: 100},
			},
			wantErr:     nil,
			wantKey:     "mode",
			wantData:    "auto",
			wantSection: shadow.SectionDesired,
		},
		{
			name: "newer timestamp overwrites older",
			doc: &shadow.Document{
				DeviceID: "dev-1",
				Reported: shadow.Section{Values: map[string]shadow.Value{
					"temp": {Data: []byte("20"), Timestamp: hlc.Timestamp{Physical: 50, NodeID: 1}},
				}},
				Desired: shadow.Section{Values: map[string]shadow.Value{}},
			},
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 2},
				DeviceID:  "dev-1",
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("25"),
				Timestamp: hlc.Timestamp{Physical: 100, NodeID: 1},
			},
			wantErr:     nil,
			wantKey:     "temp",
			wantData:    "25",
			wantSection: shadow.SectionReported,
		},
		{
			name: "older timestamp does not overwrite newer",
			doc: &shadow.Document{
				DeviceID: "dev-1",
				Reported: shadow.Section{Values: map[string]shadow.Value{
					"temp": {Data: []byte("25"), Timestamp: hlc.Timestamp{Physical: 100, NodeID: 1}},
				}},
				Desired: shadow.Section{Values: map[string]shadow.Value{}},
			},
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 1},
				DeviceID:  "dev-1",
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("20"),
				Timestamp: hlc.Timestamp{Physical: 50, NodeID: 1},
			},
			wantErr:     nil,
			wantKey:     "temp",
			wantData:    "25",
			wantSection: shadow.SectionReported,
		},
		{
			name: "op for wrong device rejected",
			doc:  emptyDoc("dev-1"),
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 1},
				DeviceID:  "dev-2",
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("22"),
				Timestamp: hlc.Timestamp{Physical: 100, NodeID: 1},
			},
			wantErr: ErrDeviceMismatch,
		},
		{
			name: "invalid section type rejected",
			doc:  emptyDoc("dev-1"),
			op: shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: 1},
				DeviceID:  "dev-1",
				Section:   shadow.SectionType(0),
				Key:       "temp",
				Data:      []byte("22"),
				Timestamp: hlc.Timestamp{Physical: 100, NodeID: 1},
			},
			wantErr: ErrInvalidSection,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &SectionAuthority{}
			result, err := r.Resolve(tt.doc, tt.op)

			if err != tt.wantErr {
				t.Fatalf("Resolve error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantErr != nil {
				return
			}

			var section shadow.Section
			switch tt.wantSection {
			case shadow.SectionReported:
				section = result.Reported
			case shadow.SectionDesired:
				section = result.Desired
			}

			v, ok := section.Values[tt.wantKey]
			if !ok {
				t.Fatalf("key %q not found in section", tt.wantKey)
			}
			if string(v.Data) != tt.wantData {
				t.Errorf("value = %q, want %q", v.Data, tt.wantData)
			}
		})
	}
}
