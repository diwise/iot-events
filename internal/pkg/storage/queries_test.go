package storage

import (
	"testing"

	"github.com/diwise/iot-events/internal/pkg/measurements"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/matryer/is"
)

func TestApplyLocationToResultPreservesNegative(t *testing.T) {
	is := is.New(t)

	loc := pgtype.Point{
		Valid: true,
		P: pgtype.Vec2{
			X: -10.5,
			Y: 52.3,
		},
	}

	result := measurements.MeasurementResult{}
	applyLocationToResult(loc, &result)

	is.Equal(*result.Lat, 52.3)
	is.Equal(*result.Lon, -10.5)
}

func TestApplyLocationToResultIgnoresInvalid(t *testing.T) {
	is := is.New(t)

	loc := pgtype.Point{Valid: false}

	result := measurements.MeasurementResult{}
	applyLocationToResult(loc, &result)

	is.True(result.Lat == nil)
	is.True(result.Lon == nil)
}
