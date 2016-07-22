package datum

import (
	"database/sql"
	"errors"
	"log"
	"reflect"
	"strings"

	"github.com/reekoheek/go-datum/core"
)

type (
	Scope func(*Tx) error

	ModelSignature struct {
		fieldIndexMap map[string]*FieldSignature
	}

	FieldSignature struct {
		structIndex int
		structName  string
	}

	Context struct {
		db       *sql.DB
		modelMap map[string]*ModelSignature
	}
)

func (c *Context) Model(name string) *ModelSignature {
	return c.modelMap[name]
}

func (c *Context) ModelByType(modelType reflect.Type) *ModelSignature {
	name := modelType.Name()
	if c.modelMap[name] == nil {

		fieldIndexMap := make(map[string]*FieldSignature)

		for i := 0; i < modelType.NumField(); i++ {
			field := modelType.Field(i)

			name := field.Tag.Get("field")
			if name == "" {
				name = strings.ToLower(field.Name)
			}

			fieldIndexMap[name] = &FieldSignature{
				structName:  field.Name,
				structIndex: i,
			}
		}

		modelSignature := &ModelSignature{
			fieldIndexMap: fieldIndexMap,
		}

		if c.modelMap == nil {
			c.modelMap = make(map[string]*ModelSignature)
		}
		c.modelMap[name] = modelSignature
	}
	return c.modelMap[name]
}

func (c *Context) DB() *sql.DB {
	if c.db == nil {
		var err error
		if c.db, err = sql.Open("sqlite3", "data.db"); err == nil {
			return c.db
		}
		panic(err)
	} else {
		return c.db
	}
}

func (c *Context) Scope(scope Scope) error {
	rtx, err := c.db.Begin()
	if err == nil {
		tx := &Tx{
			Context: c,
			Tx:      rtx,
		}
		if err = scope(tx); err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}
	return err
}

func (c *Context) Close() {
	c.db.Close()
}

func Open(driverName string, dataSourceName string) *Context {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		panic(err.Error())
	}

	context := &Context{
		db: db,
	}
	return context
}

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
