package schema

import (
	"fmt"
	"reflect"
	"strings"
)

func Validate(schema NodeSchema, data any) error {
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return fmt.Errorf("field is nil, expected struct")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %T", val.Kind())
	}

	t := val.Type()

	for _, assign := range schema.body {
		var found bool
		var field reflect.StructField

		for i := 0; i < t.NumField(); i++ {
			fieldName := t.Field(i).Name
			if strings.EqualFold(assign.ident.String(), fieldName) {
				found = true
				field = t.Field(i)
				break
			}
		}

		if !found {
			return fmt.Errorf("missing field '%s' in struct", assign.ident.String())
		}

		fieldVal := val.FieldByIndex(field.Index)
		if !fieldVal.CanInterface() {
			return fmt.Errorf("field '%s' is unexported", assign.ident.String())
		}

		err := validateNode(assign.val, fieldVal)
		if err != nil {
			return fmt.Errorf("validate field field %s: %w", assign.ident.String(), err)
		}
	}

	return nil
}

func validateNode(node Node, val reflect.Value) error {
	switch n := node.(type) {
	case NodeLiteral:
		switch n.name {
		case "int":
			if val.Kind() != reflect.Int {
				return fmt.Errorf("expected int, got %s", val.Kind())
			}
		case "str":
			if val.Kind() != reflect.String {
				return fmt.Errorf("expected str, got %s", val.Kind())
			}
		default:
			return fmt.Errorf("unsupported type: %s", val.Kind())
		}
	case NodeSchema:
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				return fmt.Errorf("field is nil, expected struct")
			}

			val = val.Elem()
		}

		if val.Kind() != reflect.Struct {
			return fmt.Errorf("expected struct, got %v", val.Kind())
		}

		return Validate(n, val.Interface())
	default:
		return fmt.Errorf("unknown node type: %T", node)
	}

	return nil
}

func ValidateMap(schema NodeSchema, data map[string]any) error {
	for _, assign := range schema.body {
		key := assign.ident.String()
		var foundKey string

		for k := range data {
			if strings.EqualFold(k, key) {
				foundKey = k
				break
			}
		}

		if foundKey == "" {
			return fmt.Errorf("missing field %q in payload", key)
		}

		value := data[foundKey]
		if err := validateNodeMap(assign.val, value); err != nil {
			return fmt.Errorf("validate field %q: %w", key, err)
		}
	}
	return nil
}

func validateNodeMap(node Node, value interface{}) error {
	switch n := node.(type) {
	case NodeLiteral:
		switch n.name {
		case "int":
			_, ok := value.(int)
			if !ok {
				return fmt.Errorf("expected number, got %T", value)
			}
		case "str":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("expected string, got %T", value)
			}
		default:
			return fmt.Errorf("unsupported literal type: %s", n.name)
		}
	case NodeSchema:
		m, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected object for nested schema, got %T", value)
		}
		return ValidateMap(n, m)
	default:
		return fmt.Errorf("unknown node type: %T", node)
	}
	return nil
}
