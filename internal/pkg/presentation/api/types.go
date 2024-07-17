package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
)

type meta struct {
	TotalRecords uint64  `json:"totalRecords"`
	Offset       *uint64 `json:"offset,omitempty"`
	Limit        *uint64 `json:"limit,omitempty"`
	Count        uint64  `json:"count"`
}

type links struct {
	Self  *string `json:"self,omitempty"`
	First *string `json:"first,omitempty"`
	Prev  *string `json:"prev,omitempty"`
	Next  *string `json:"next,omitempty"`
	Last  *string `json:"last,omitempty"`
}

type ApiResponse struct {
	Meta  *meta  `json:"meta,omitempty"`
	Data  any    `json:"data"`
	Links *links `json:"links,omitempty"`
}

func NewApiResponse(r *http.Request, data any, count, total, offset, limit uint64) ApiResponse {
	meta := &meta{
		TotalRecords: total,
		Offset: &offset,
		Limit: &limit,
		Count: count,
	}

	links := createLinks(r.URL, meta)

	return ApiResponse{
		Meta: meta,
		Data: data,
		Links: links,
	}
}

func (r ApiResponse) Byte() []byte {
	b, _ := json.Marshal(r)
	return b
}

func createLinks(u *url.URL, m *meta) *links {
	if m == nil || m.TotalRecords == 0 {
		return nil
	}

	query := u.Query()

	newUrl := func(offset uint64) *string {
		query.Set("offset", strconv.Itoa(int(offset)))
		u.RawQuery = query.Encode()
		u_ := u.String()
		return &u_
	}

	first := uint64(0)
	last := ((m.TotalRecords - 1) / *m.Limit) * *m.Limit
	next := *m.Offset + *m.Limit
	prev := int64(*m.Offset) - int64(*m.Limit)

	links := &links{
		Self:  newUrl(*m.Offset),
		First: newUrl(first),
		Last:  newUrl(last),
	}

	if next < m.TotalRecords {
		links.Next = newUrl(next)
	}

	if prev >= 0 {
		links.Prev = newUrl(uint64(prev))
	}

	return links
}

