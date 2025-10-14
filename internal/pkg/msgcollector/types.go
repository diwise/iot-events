package messagecollector

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
)

type QueryResult struct {
	Data       any
	Count      uint64
	Offset     uint64
	Limit      uint64
	TotalCount uint64
	Error      error
}

var ErrNotFound error = fmt.Errorf("not found")

type QueryParams map[string][]string

func ParseQuery(q map[string][]string) QueryParams {
	m := map[string][]string{}

	for k, v := range q {
		if len(v) == 0 {
			continue
		}

		vv := []string{}

		for _, s := range v {
			if s == "" {
				continue
			}
			vv = append(vv, s)
		}

		if len(vv) == 0 {
			continue
		}

		key := strings.ToLower(k)
		m[key] = vv
	}

	return m
}

type QueryArgs struct {
	Where       string
	Args        pgx.NamedArgs
	Offset      uint64
	Limit       uint64
	OffsetLimit string
}

func (q QueryParams) NamedArgs() (QueryArgs, error) {
	qa := QueryArgs{}

	n := pgx.NamedArgs{}
	s := []string{}

	for k, v := range q {
		if len(v) == 0 {
			continue
		}

		switch k {
		case "timerel":
			if !slices.Contains([]string{"after", "before", "between"}, v[0]) {
				return qa, fmt.Errorf("invalid timerel value: %s", v[0])
			}

			var timeAt, endTimeAt time.Time
			var ok bool

			timeAt, ok = q.GetTime("timeAt")
			if !ok {
				return qa, fmt.Errorf("invalid timeAt value: %s", v[0])
			}
			if v[0] == "between" {
				endTimeAt, ok = q.GetTime("endTimeAt")
				if !ok {
					return qa, fmt.Errorf("parameter endTimeAt is invalid")
				}
			} else {
				endTimeAt = time.Now().UTC()
			}

			switch v[0] {
			case "after":
				s = append(s, "time >= @timeAt")
				n["timeAt"] = timeAt
			case "before":
				s = append(s, "time <= @timeAt")
				n["timeAt"] = timeAt
			case "between":
				s = append(s, "time BETWEEN @timeAt AND @endTimeAt")
				n["timeAt"] = timeAt
				n["endTimeAt"] = endTimeAt
			default:
				return QueryArgs{}, fmt.Errorf("invalid timerel value: %s", v[0])
			}
		case "timeat", "endtimeat", "latest":
		case "limit":
			i, err := strconv.Atoi(v[0])
			if err != nil {
				return qa, fmt.Errorf("invalid limit value: %s", v[0])
			}
			qa.Limit = uint64(i)
			n["limit"] = qa.Limit
		case "offset":
			i, err := strconv.Atoi(v[0])
			if err != nil {
				return qa, fmt.Errorf("invalid offset value: %s", v[0])
			}
			qa.Offset = uint64(i)
			n["offset"] = qa.Offset
		case "name":
			s = append(s, "e.n=@name")
			n["name"] = v[0]
		default:
			if strings.HasPrefix(k, "metadata[") && strings.HasSuffix(k, "]") {
				re := regexp.MustCompile(`^metadata\[(.+)\]$`)
				matches := re.FindStringSubmatch(k)
				if len(matches) != 2 {
					return QueryArgs{}, fmt.Errorf("invalid metadata query parameter: %s", k)
				}
				metaKey := matches[1]
				w := fmt.Sprintf(`EXISTS (SELECT 1 FROM events_measurements_metadata mm WHERE mm.id = e.id AND mm.key=@meta_key_%s `, metaKey)
				if len(v) > 0 && strings.TrimSpace(v[0]) != "" {
					w += fmt.Sprintf("AND mm.value=@meta_value_%s ", metaKey)
					n["meta_value_"+metaKey] = strings.TrimSpace(v[0])
				}
				w += ")"
				s = append(s, w)

				n["meta_key_"+metaKey] = metaKey

				continue
			}

			if len(v) > 1 {
				s = append(s, fmt.Sprintf("e.%s=ANY(@%s)", k, k))
				n["@"+k] = v
				continue
			}

			s = append(s, fmt.Sprintf("e.%s=@%s", k, k))
			n[k] = v[0]
		}
	}

	qa.Where = fmt.Sprintf("AND %s", strings.Join(s, " AND "))
	qa.Args = n

	if qa.Limit > 0 {
		qa.OffsetLimit = fmt.Sprintf("OFFSET %d LIMIT %d", qa.Offset, qa.Limit)
	} else if qa.Offset > 0 {
		qa.OffsetLimit = fmt.Sprintf("OFFSET %d", qa.Offset)
	}

	return qa, nil
}

func (q QueryParams) ValidateParams(fn func(string) bool) error {
	for k := range q {
		if !fn(k) {
			return fmt.Errorf("invalid query parameter: %s", k)
		}
	}
	return nil
}

func (q QueryParams) GetString(key string) (string, bool) {
	key = strings.ToLower(key)
	s, ok := q[key]
	if !ok {
		return "", false
	}

	if s[0] == "" {
		return "", false
	}

	return s[0], true
}

func (q QueryParams) GetUint64(key string) (uint64, bool) {
	key = strings.ToLower(key)
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

func (q QueryParams) GetTime(key string) (time.Time, bool) {
	ts, ok := q.GetString(key)
	if !ok {
		return time.Time{}, false
	}

	if !strings.HasSuffix(ts, "Z") {
		ts += "Z"
	}

	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}, false
	}

	return t.UTC(), true
}

func (q QueryParams) GetBool(key string) (bool, bool) {
	b, ok := q.GetString(key)
	if !ok {
		return false, false
	}
	return strings.EqualFold(b, "true"), true
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
	DeviceID    string     `json:"deviceID"`
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Tenant      string     `json:"tenant"`
	Timestamp   time.Time  `json:"timestamp"`
	Urn         string     `json:"urn"`
	BoolValue   *bool      `json:"vb,omitempty"`
	Lat         float64    `json:"lat"`
	Lon         float64    `json:"lon"`
	StringValue string     `json:"vs,omitempty"`
	Unit        string     `json:"unit,omitempty"`
	Value       *float64   `json:"v,omitempty"`
	Metadata    []Metadata `json:"metadata,omitempty"`
}

type MeasurementResult struct {
	ID           string     `json:"id,omitempty"`
	DeviceID     string     `json:"deviceID,omitempty"`
	Name         string     `json:"name,omitempty"`
	Urn          string     `json:"urn,omitempty"`
	Lat          *float64   `json:"lat,omitempty"`
	Lon          *float64   `json:"lon,omitempty"`
	LastObserved *time.Time `json:"lastObserved,omitempty"`
	Values       []Value    `json:"values"`
	Tenant       string     `json:"tenant,omitempty"`
}

type Value struct {
	ID          *string   `json:"id,omitempty"`
	Name        *string   `json:"n,omitempty"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue string    `json:"vs,omitempty"`
	Value       *float64  `json:"v,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Link        *string   `json:"link,omitempty"`
	Sum         *float64  `json:"sum,omitempty"`
	Urn         *string   `json:"urn,omitempty"`
}

type AggrResult struct {
	Average *float64 `json:"avg,omitempty"`
	Total   *float64 `json:"sum,omitempty"`
	Minimum *float64 `json:"min,omitempty"`
	Maximum *float64 `json:"max,omitempty"`
	Count   *uint64  `json:"count,omitempty"`
}

type Metadata struct {
	ID       string `json:"id,omitempty"`
	DeviceID string `json:"deviceID,omitempty"`
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
}

func LoadMetadata(ctx context.Context, f io.Reader) ([]Metadata, error) {
	log := logging.GetFromContext(ctx)

	metadata := []Metadata{}
	r := csv.NewReader(f)
	r.Comma = ';'
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("could not read metadata csv: %w", err)
	}

	if len(records) < 1 {
		return nil, fmt.Errorf("metadata csv is empty")
	}

	header := records[0]
	colIndex := map[string]int{}

	for i, col := range header {
		colIndex[strings.ToLower(col)] = i
	}

	requiredCols := []string{"id", "key", "value"}

	for _, col := range requiredCols {
		if _, ok := colIndex[col]; !ok {
			return nil, fmt.Errorf("metadata csv is missing required column: %s", col)
		}
	}

	for i, record := range records[1:] {
		if len(record) != len(header) {
			log.Warn("skipping metadata record with wrong number of columns", "line", i+2)
			continue
		}

		m := Metadata{
			ID:    record[colIndex["id"]],
			Key:   record[colIndex["key"]],
			Value: record[colIndex["value"]],
		}

		if m.ID == "" || m.Key == "" || m.Value == "" {
			log.Warn("skipping metadata record with empty required field", "line", i+2)
			continue
		}

		parts := strings.Split(m.ID, "/")

		if len(parts) > 1 {
			m.DeviceID = parts[0]
		}

		metadata = append(metadata, m)
	}

	return metadata, nil
}
