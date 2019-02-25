package main

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestTickMapper(t *testing.T) {
	t.Run("valid tick", func(t *testing.T) {
		row := "1,2019-02-25 11:30:06.691,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87"
		columns := strings.Split(row, ",")

		mappedRow, err := tickMapper(columns)

		assert.Equal(t, "2019-02-25 11:30:06.691,23.8800,12,6714,23.8700,23.9700,6,O,25,3D87", mappedRow)
		assert.Nil(t, err)
	})

	t.Run("too few columns", func(t *testing.T) {
		row := "1,2019-02-25 11:30:06.691,23.8800,12,6714,23.8700,23.9700,6,O,25"
		columns := strings.Split(row, ",")

		mappedRow, err := tickMapper(columns)

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})

	t.Run("no columns", func(t *testing.T) {
		var columns []string

		mappedRow, err := tickMapper(columns)

		assert.Equal(t, "", mappedRow)
		assert.Errorf(t, err, "too few columns")
	})
}
