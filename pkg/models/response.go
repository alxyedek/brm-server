package models

// SimpleResponse represents the response structure for simple endpoints
type SimpleResponse struct {
	HostString    string `json:"hostString"`
	PathString    string `json:"pathString"`
	TimeString    string `json:"timeString"`
	RandomInteger int    `json:"randomInteger"`
	GoroutineInfo string `json:"goroutineInfo"`
}
