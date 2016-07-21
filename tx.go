package datum

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type (
	Tx struct {
		*Context
		*sql.Tx
	}
)

func (tx *Tx) Save(m interface{}) (err error) {
	val := reflect.ValueOf(m).Elem()
	name := strings.ToLower(val.Type().Name())
	fields := []string{}
	placeholders := []string{}
	values := []interface{}{}

	for i := 0; i < val.NumField(); i++ {
		// fmt.Printf("Field Name: %s,\t Field Value: %v,\t Tag Value: %v\n", typeField.Name, valueField.Interface(), tag.Get("field") == "")
		valueField := val.Field(i)

		typeField := val.Type().Field(i)
		tag := typeField.Tag

		field := tag.Get("field")
		if field == "" {
			field = strings.ToLower(typeField.Name)
		}

		if field == "id" {
			continue
		}

		fields = append(fields, field)
		placeholders = append(placeholders, "?")
		values = append(values, valueField.Interface())
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", name, strings.Join(fields, ", "), strings.Join(placeholders, ", "))

	_, err = tx.Exec(sql, values...)

	return err
}
