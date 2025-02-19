package schema

import (
	"testing"
)

func TestCorrectSchema(t *testing.T) {
	p, err := NewParserString(`
[
    x > str
]
	`)
	if err != nil {
		t.Error("unexpected new parser from string", err)
	}

	type testable struct {
		X string
	}
	schema, err := p.Parse()
	if err != nil {
		t.Error("unexpected parse schema", err)
	}

	err = Validate(schema, testable{X: ""})
	if err != nil {
		t.Error("unexpected validate", err)
	}
}

func TestIncorrectSchema(t *testing.T) {
	p, err := NewParserString(`
[
    x > str
]
	`)
	if err != nil {
		t.Error("unexpected new parser from string", err)
	}

	type testable struct {
		Y string
	}
	schema, err := p.Parse()
	if err != nil {
		t.Error("unexpected parse schema", err)
	}

	err = Validate(schema, testable{Y: ""})
	if err == nil {
		t.Error("unexpected nil error")
	}
}

func TestNestedCorrectSchema(t *testing.T) {
	p, err := NewParserString(`
[
    x > [
		y > int
	]
]
	`)
	if err != nil {
		t.Error("unexpected new parser from string", err)
	}

	type testable struct {
		X struct{ Y int }
	}
	schema, err := p.Parse()
	if err != nil {
		t.Error("unexpected parse schema", err)
	}

	err = Validate(schema, testable{X: struct{ Y int }{Y: 0}})
	if err != nil {
		t.Error("unexpected validate", err)
	}
}

func TestNestedIncorrectSchema(t *testing.T) {
	p, err := NewParserString(`
[
    x > [
		y > int
	]
]
	`)
	if err != nil {
		t.Error("unexpected new parser from string", err)
	}

	type testable struct {
		X struct{ Y string }
	}
	schema, err := p.Parse()
	if err != nil {
		t.Error("unexpected parse schema", err)
	}

	err = Validate(schema, testable{X: struct{ Y string }{Y: ""}})
	if err == nil {
		t.Error("unexpected nil error")
	}
}
