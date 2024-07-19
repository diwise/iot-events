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
	Count        *uint64 `json:"count,omitempty"`
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
	}

	if offset > 0 {
		meta.Offset = &offset
	}

	if count != total {
		meta.Limit = &limit
		meta.Count = &count
	}

	links := createLinks(r.URL, meta)

	return ApiResponse{
		Meta:  meta,
		Data:  data,
		Links: links,
	}
}

func (r ApiResponse) Byte() []byte {
	b, _ := json.Marshal(r)
	return b
}

func createLinks(u *url.URL, m *meta) *links {
	if m == nil || m.TotalRecords == 0 || m.Count == nil || (*m.Count == m.TotalRecords) {
		return nil
	}

	query := u.Query()

	newUrl := func(offset int64) *string {
		query.Set("offset", strconv.Itoa(int(offset)))
		u.RawQuery = query.Encode()
		u_ := u.String()
		return &u_
	}

	var offset int64 = 0
	if m.Offset != nil {
		offset = int64(*m.Offset)
	}

	var limit int64 = 10
	if m.Limit != nil {
		limit = int64(*m.Limit)
	}

	first := int64(0)
	last := (int64(m.TotalRecords-1) / limit) * limit
	next := offset + limit
	prev := offset - limit

	links := &links{
		Self:  newUrl(offset),
		First: newUrl(first),
		Last:  newUrl(last),
	}

	if next < int64(m.TotalRecords) {
		links.Next = newUrl(next)
	}

	if prev >= 0 {
		links.Prev = newUrl(prev)
	}

	return links
}
