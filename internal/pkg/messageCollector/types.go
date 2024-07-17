package messagecollector

import (
	"strconv"
	"time"
)

type QueryResult struct {
	Data       any
	Count      uint64
	Offset     uint64
	Limit      uint64
	TotalCount uint64
	Error      error
}

type QueryParams map[string][]string

func ParseQuery(q map[string][]string) QueryParams {
	return q
}

func (q QueryParams) GetString(key string) (string, bool) {
	s, ok := q[key]
	if !ok {
		return "", false
	}
	return s[0], true
}

func (q QueryParams) GetUint64(key string) (uint64, bool) {
	s, ok := q[key]
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseUint(s[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return i, true
}

func (q QueryParams) GetUint64OrDefault(key string, i uint64) uint64 {
	v, ok := q.GetUint64(key)
	if !ok {
		return i
	}
	return v
}

func NewMeasurement(ts time.Time, id, deviceID, name, urn, tenant string) Measurement {
	return Measurement{
		DeviceID:  deviceID,
		ID:        id,
		Name:      name,
		Tenant:    tenant,
		Timestamp: ts,
		Urn:       urn,
	}
}

type Measurement struct {
	DeviceID    string    `json:"deviceID"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Tenant      string    `json:"tenant"`
	Timestamp   time.Time `json:"timestamp"`
	Urn         string    `json:"urn"`
	BoolValue   *bool     `json:"vb,omitempty"`
	Lat         float64   `json:"lat"`
	Lon         float64   `json:"lon"`
	StringValue string    `json:"vs,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Value       *float64  `json:"v,omitempty"`
}

type MeasurementResult struct {
	DeviceID string  `json:"deviceID"`
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Tenant   string  `json:"tenant"`
	Urn      string  `json:"urn"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	Values   []Value `json:"values"`
}

type Value struct {
	Timestamp   time.Time `json:"timestamp"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue string    `json:"vs,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Value       *float64  `json:"v,omitempty"`
}

type DeviceResult struct {
	DeviceID     string            `json:"deviceID"`
	LastObserved time.Time         `json:"lastObserved"`
	TotalCount   uint64            `json:"totalCount"`
	Measurements []MeasurementType `json:"measurements"`
}

type MeasurementType struct {
	ID           string    `json:"id"`
	Urn          string    `json:"urn"`
	Count        uint64    `json:"count"`
	LastObserved time.Time `json:"lastObserved"`
	Link         string    `json:"link"`
}