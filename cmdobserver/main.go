package cmdobserver

type ObserverMessage struct {
	MessageID   string `json:"messageId"`
	MessageTime string `json:"messageTime"`
	SubjectID   string `json:"subjectId"`
}

type ObserverMessage62028000 struct {
	MessageID   string `json:"messageId"`
	MessageTime string `json:"messageTime"`
	ObserverID  string `json:"observerId"`
	SubjectID   string `json:"subjectId"`
}

type ObserverMessage62028001 struct {
	DataSourceCode string `json:"dataSourceCode"`
	MessageID      string `json:"messageId"`
	MessageTime    string `json:"messageTime"`
	RecordID       string `json:"recordId"`
	SubjectID      string `json:"subjectId"`
}

type ObserverMessage62028002 struct {
	DataSourceCode string `json:"dataSourceCode"`
	MessageID      string `json:"messageId"`
	MessageTime    string `json:"messageTime"`
	RecordID       string `json:"recordId"`
	SubjectID      string `json:"subjectId"`
}

type ObserverMessage62028003 struct {
	DataSourceCode string `json:"dataSourceCode"`
	LineNumber     string `json:"lineNumber"`
	MessageID      string `json:"messageId"`
	MessageTime    string `json:"messageTime"`
	RecordID       string `json:"recordId"`
	SubjectID      string `json:"subjectId"`
}
