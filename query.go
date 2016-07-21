package datum

import (
	"errors"
	"log"
	"reflect"

	"github.com/reekoheek/go-show/datum/core"
)

func (c *Context) Query(recv interface{}, sqlStr string, args ...interface{}) error {
	argErr := errors.New("Receiver must be *slice of *models")

	sliceValue := reflect.Indirect(reflect.ValueOf(recv))
	if sliceValue.Kind() != reflect.Slice &&
		sliceValue.Type().Elem().Kind() != reflect.Ptr &&
		sliceValue.Type().Elem().Elem().Kind() != reflect.Struct {
		return argErr
	}

	log.Printf("[SQL] %s", sqlStr)

	rows, err := c.db.Query(sqlStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnCount := len(columns)
	modelType := sliceValue.Type().Elem().Elem()

	model := c.ModelByType(modelType)

	for rows.Next() {
		scanResults := make([]interface{}, columnCount)

		for i := 0; i < columnCount; i++ {
			var cell interface{}
			scanResults[i] = &cell
		}

		if err = rows.Scan(scanResults...); err != nil {
			break
		}
		modelValue := reflect.New(modelType)

		for i, v := range scanResults {
			val := reflect.Indirect(reflect.ValueOf(v))
			resolvedVal := reflect.ValueOf(val.Interface())

			if val.Interface() == nil {
				continue
			}

			field := model.fieldIndexMap[columns[i]]
			if field == nil {
				continue
			}

			fieldVal := modelValue.Elem().Field(field.structIndex)
			switch fieldVal.Kind() {
			case reflect.String:
				switch resolvedVal.Kind() {
				case reflect.String:
					fieldVal.SetString(resolvedVal.String())
				case reflect.Slice:
					fieldVal.SetString(string(resolvedVal.Bytes()))
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				switch resolvedVal.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					fieldVal.SetInt(resolvedVal.Int())
				}
			case reflect.Bool:
				switch resolvedVal.Kind() {
				case reflect.Bool:
					fieldVal.SetBool(resolvedVal.Bool())
				}
			case reflect.Struct:
				if fieldVal.Type().ConvertibleTo(core.TimeType) {
					if resolvedVal.Type() == core.TimeType {
						fieldVal.Set(resolvedVal)
					}
				}
			}

		}

		//log.Println(modelValue.Interface())
		sliceValue.Set(reflect.Append(sliceValue, modelValue))
	}

	return err
}
