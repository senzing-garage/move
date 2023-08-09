package move

import (
	"testing"
)

func Test_szRecord_GetMessageId(t *testing.T) {
	type fields struct {
		body   string
		id     int
		source string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{name: "test read JSONL file", fields: fields{body: "", id: 0, source: "file.jsonl"}, want: "file.jsonl-0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &szRecord{
				body:   tt.fields.body,
				id:     tt.fields.id,
				source: tt.fields.source,
			}
			if got := r.GetMessageId(); got != tt.want {
				t.Errorf("szRecord.GetMessageId() = %v, want %v", got, tt.want)
			}
		})
	}
}
