package move_test

import (
	"testing"

	"github.com/senzing-garage/move/move"
)

func Test_szRecord_GetMessageId(test *testing.T) {
	type fields struct {
		body   string
		id     int
		source string
	}

	testCases := []struct {
		name     string
		fields   fields
		expected string
	}{
		{
			name:     "test read JSONL file",
			fields:   fields{body: "", id: 0, source: "file.jsonl"},
			expected: "file.jsonl-0",
		},
	}
	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			record := &move.SzRecord{
				Body:   testCase.fields.body,
				ID:     testCase.fields.id,
				Source: testCase.fields.source,
			}
			if actual := record.GetMessageID(); actual != testCase.expected {
				test.Errorf("szRecord.GetMessageID() = %v, want %v", actual, testCase.expected)
			}
		})
	}
}
