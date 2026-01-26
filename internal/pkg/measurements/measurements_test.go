package measurements

import (
	"strings"
	"testing"

	"github.com/matryer/is"
)

/*
	func TestParseQueryParams(t *testing.T) {
		is := is.New(t)
		q := ParseQuery(map[string][]string{
			"id":             {"id"},
			"device_id":      {"device_id"},
			"timeRel":        {"between"},
			"timeAt":         {"2024-01-01T00:00:00Z"},
			"endTimeAt":      {"2024-01-02T00:00:00Z"},
			"limit":          {"10"},
			"offset":         {"5"},
			"metadata[name]": {"name"},
		})
		is.Equal(len(q), 8)

		qa, err := q.Parse()
		is.NoErr(err)
		is.Equal(len(qa.Args), 8)
		is.Equal(qa.Offset, uint64(5))
		is.Equal(qa.Limit, uint64(10))
		is.True(strings.Contains(qa.Where, "e.id=@id"))
		is.True(strings.Contains(qa.Where, "e.device_id=@device_id"))
		is.True(strings.Contains(qa.Where, "time BETWEEN @timeAt AND @endTimeAt"))
		is.True(strings.Contains(qa.Where, "EXISTS (SELECT 1 FROM events_measurements_metadata mm WHERE mm.id = e.id AND mm.key = @meta_key_name AND mm.value LIKE @meta_value_name)"))
	}

	func TestParseQueryMetadata(t *testing.T) {
		is := is.New(t)
		q := ParseQuery(map[string][]string{
			"metadata[key1]": {"value1"},
			"metadata[key2]": {"value2"},
		})

		qa, err := q.Parse()
		is.NoErr(err)
		is.Equal(len(qa.Args), 4)
		is.True(strings.Contains(qa.Where, "AND EXISTS (SELECT 1 FROM events_measurements_metadata mm WHERE mm.id = e.id AND mm.key = @meta_key_key1 AND mm.value LIKE @meta_value_key1)"))
		is.True(strings.Contains(qa.Where, "AND EXISTS (SELECT 1 FROM events_measurements_metadata mm WHERE mm.id = e.id AND mm.key = @meta_key_key2 AND mm.value LIKE @meta_value_key2"))
	}
*/
func TestParseQueryParamsNamedArgs(t *testing.T) {
	is := is.New(t)
	q := ParseQuery(map[string][]string{
		"key1": {"value1"},
		"key2": {"value2a", "value2b"},
		"key3": {""},
		"KEY4": {"value1"},
	})

	qa, err := q.Parse()
	is.NoErr(err)
	is.Equal(len(qa.Args), 3)
	is.True(strings.Contains(qa.Where, "key1=@key1"))
	is.True(strings.Contains(qa.Where, "key2=ANY(@key2)"))
	is.True(strings.Contains(qa.Where, "key4=@key4"))
}

func TestParseQueryParamsNameMultiValue(t *testing.T) {
	is := is.New(t)
	q := ParseQuery(map[string][]string{
		"name": {"alpha", "beta"},
	})

	qa, err := q.Parse()
	is.NoErr(err)
	is.True(strings.Contains(qa.Where, "e.n=ANY(@name)"))

	values, ok := qa.Args["name"].([]string)
	is.True(ok)
	is.Equal(values, []string{"alpha", "beta"})
}

func TestParseQueryParamsNameSingleValue(t *testing.T) {
	is := is.New(t)
	q := ParseQuery(map[string][]string{
		"name": {"alpha"},
	})

	qa, err := q.Parse()
	is.NoErr(err)
	is.True(strings.Contains(qa.Where, "e.n=@name"))
	is.Equal(qa.Args["name"], "alpha")
}

func TestParseQueryParamsTimeRel(t *testing.T) {
	is := is.New(t)
	q := ParseQuery(map[string][]string{
		"timeRel":   {"between"},
		"timeAt":    {"2024-01-01T00:00:00Z"},
		"endTimeAt": {"2024-01-02T00:00:00Z"},
	})

	qa, err := q.Parse()
	is.NoErr(err)
	is.Equal(len(qa.Args), 2)
	is.Equal(qa.Where, "AND time BETWEEN @timeAt AND @endTimeAt")

}

func TestParseQueryOffsetLimit(t *testing.T) {
	is := is.New(t)
	q := ParseQuery(map[string][]string{
		"offset": {"10"},
		"limit":  {"25"},
	})

	qa, err := q.Parse()
	is.NoErr(err)
	is.Equal(qa.Offset, uint64(10))
	is.Equal(qa.Limit, uint64(25))
	is.Equal(qa.OffsetLimit, "OFFSET 10 LIMIT 25")
}
