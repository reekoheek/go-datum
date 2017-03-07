package datum

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/reekoheek/go-datum/core"
)

type (
	query struct {
		c              *Context
		sql            string
		args           []interface{}
		rows           *sql.Rows
		columns        []string
		columnCount    int
		modelType      reflect.Type
		modelSignature *ModelSignature
	}
)

var (
	QueryArgError = errors.New("Receiver must be *[]*model or *model")
)

func (q *query) As(recv interface{}) error {
	var err error

	value := reflect.ValueOf(recv)
	if value.Kind() != reflect.Ptr {
		return QueryArgError
	}

	q.rows, err = q.c.db.Query(q.sql, q.args...)
	if err != nil {
		return err
	}
	defer q.rows.Close()

	if q.columns, err = q.rows.Columns(); err != nil {
		return err
	}
	q.columnCount = len(q.columns)

	recvValue := reflect.Indirect(value)

	switch kind := recvValue.Kind(); kind {
	case reflect.Slice:
		return q.fetchSliceValue(recvValue)
	default:
		if q.rows.Next() {
			q.detectModelType(recvValue.Type())
			return q.fetchModelValue(value)
		} else {
			return NotFoundErr
		}
	}
	return nil
}

func (q *query) detectModelType(t reflect.Type) {
	switch kind := t.Kind(); kind {
	case reflect.Ptr:
		switch kind := t.Elem().Kind(); kind {
		case reflect.Struct:
			q.modelType = t.Elem()
			q.modelSignature = q.c.ModelByType(q.modelType)
		default:
			q.modelType = t.Elem()
		}
	case reflect.Struct:
		q.modelType = t
		q.modelSignature = q.c.ModelByType(q.modelType)
	case reflect.Map, reflect.Slice:
		q.modelType = t
	default:
		panic("Unimplemented")
	}
}

func (q *query) fetchSliceValue(recvValue reflect.Value) error {
	directType := recvValue.Type().Elem()
	q.detectModelType(directType)

	for q.rows.Next() {
		var modelValue reflect.Value
		switch kind := q.modelType.Kind(); kind {
		case reflect.Struct:
			modelValue = reflect.New(q.modelType)
		case reflect.Map:
			modelValue = reflect.New(q.modelType)
			modelValue.Elem().Set(reflect.MakeMap(q.modelType))
		case reflect.Slice:
			modelValue = reflect.New(q.modelType)
			modelValue.Elem().Set(reflect.MakeSlice(q.modelType, 0, 1))
		default:
			panic(fmt.Sprintf("Unimplemented %s", kind))
		}
		if err := q.fetchModelValue(modelValue); err != nil {
			return err
		}
		if directType.Kind() == reflect.Ptr {
			recvValue.Set(reflect.Append(recvValue, modelValue))
		} else {
			recvValue.Set(reflect.Append(recvValue, modelValue.Elem()))
		}
	}

	return nil
}

func (q *query) fetchModelValue(modelValue reflect.Value) error {
	scanResults := make([]interface{}, q.columnCount)

	for i := 0; i < q.columnCount; i++ {
		var cell interface{}
		scanResults[i] = &cell
	}

	if err := q.rows.Scan(scanResults...); err != nil {
		return err
	}

	for i, v := range scanResults {
		val := reflect.Indirect(reflect.ValueOf(v))
		resolvedVal := reflect.ValueOf(val.Interface())

		if val.Interface() == nil {
			continue
		}

		switch kind := q.modelType.Kind(); kind {
		case reflect.Struct:
			field := q.modelSignature.fieldIndexMap[q.columns[i]]
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
		case reflect.Map:
			modelValue.Elem().SetMapIndex(reflect.ValueOf(q.columns[i]), val)
		case reflect.Slice:
			modelValue.Elem().Set(reflect.Append(modelValue.Elem(), val))
		default:
			panic(fmt.Sprintf("Unimplemented %s", kind))
		}
	}
	return nil
}

//func (c *Context) Query(recv interface{}, sqlStr string, args ...interface{}) error {
//
//	sliceValue := reflect.Indirect(reflect.ValueOf(recv))
//	if sliceValue.Kind() != reflect.Slice &&
//		sliceValue.Type().Elem().Kind() != reflect.Ptr &&
//		sliceValue.Type().Elem().Elem().Kind() != reflect.Struct {
//		return argErr
//	}
//
//	log.Printf("[SQL] %s", sqlStr)
//
//	rows, err := c.db.Query(sqlStr, args...)
//	if err != nil {
//		return err
//	}
//	defer rows.Close()
//
//	columns, _ := rows.Columns()
//	columnCount := len(columns)
//	modelType := sliceValue.Type().Elem().Elem()
//
//	model := c.ModelByType(modelType)
//
//	for rows.Next() {
//		scanResults := make([]interface{}, columnCount)
//
//		for i := 0; i < columnCount; i++ {
//			var cell interface{}
//			scanResults[i] = &cell
//		}
//
//		if err = rows.Scan(scanResults...); err != nil {
//			break
//		}
//		modelValue := reflect.New(modelType)
//
//		for i, v := range scanResults {
//			val := reflect.Indirect(reflect.ValueOf(v))
//			resolvedVal := reflect.ValueOf(val.Interface())
//
//			if val.Interface() == nil {
//				continue
//			}
//
//			field := model.fieldIndexMap[columns[i]]
//			if field == nil {
//				continue
//			}
//
//			fieldVal := modelValue.Elem().Field(field.structIndex)
//			switch fieldVal.Kind() {
//			case reflect.String:
//				switch resolvedVal.Kind() {
//				case reflect.String:
//					fieldVal.SetString(resolvedVal.String())
//				case reflect.Slice:
//					fieldVal.SetString(string(resolvedVal.Bytes()))
//				}
//			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//				switch resolvedVal.Kind() {
//				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//					fieldVal.SetInt(resolvedVal.Int())
//				}
//			case reflect.Bool:
//				switch resolvedVal.Kind() {
//				case reflect.Bool:
//					fieldVal.SetBool(resolvedVal.Bool())
//				}
//			case reflect.Struct:
//				if fieldVal.Type().ConvertibleTo(core.TimeType) {
//					if resolvedVal.Type() == core.TimeType {
//						fieldVal.Set(resolvedVal)
//					}
//				}
//			}
//
//		}
//
//		//log.Println(modelValue.Interface())
//		sliceValue.Set(reflect.Append(sliceValue, modelValue))
//	}
//
//	return err
//}
