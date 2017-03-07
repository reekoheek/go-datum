package datum

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
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

var (
	NotFoundErr = errors.New("Not found")
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

func (c *Context) Query(sqlStr string, args ...interface{}) *query {
	q := &query{
		c:    c,
		sql:  sqlStr,
		args: args,
	}

	return q
}
