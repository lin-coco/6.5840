package test

import (
	"strings"
	"testing"
	"unicode"
)

func TestString(t *testing.T) {
	s := "The Project Gutenberg eBook, The Importance of Being Earnest, by Oscar\nWilde"
	fieldsFunc := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	t.Log(fieldsFunc)
}
