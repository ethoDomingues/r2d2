package r2d2

import (
	"bytes"
	"database/sql"
	"reflect"
	"time"

	"github.com/ethoDomingues/c3po"
	"github.com/lib/pq"
)

var _db *sql.DB

func GetDBSession() (*DB, error) {
	if _db == nil {
		var err error
		dsn := "host=localhost user=gorm password=gorm dbname=gorm sslmode=disable"
		_db, err = sql.Open("postgres", dsn)
		if err != nil {
			return nil, err
		}
		if err := _db.Ping(); err != nil {
			return nil, err
		}
		_db.SetConnMaxLifetime(0)
		_db.SetMaxIdleConns(50)
		_db.SetMaxOpenConns(50)
	}

	return &DB{
		SQL:  bytes.NewBufferString(""),
		pool: _db,
		Args: []any{},
	}, nil
}

type DB struct {
	pool *sql.DB
	SQL  *bytes.Buffer
	Args []any
}

func (db *DB) Raw(f string, args ...any) *DB {
	db.SQL.Reset()
	db.SQL.WriteString(f)
	return db
}

func (db *DB) Find(dst any) {
	vFielder := c3po.ParseSchemaWithTag("r2d2", dst)
	rows, err := db.pool.Query(db.SQL.String())
	checkErr(err)
	defer rows.Close()

	var vals = c3po.GetReflectElem(vFielder.New())
	var dstRV = c3po.GetReflectElem(reflect.ValueOf(dst))
	var filderT *c3po.Fielder

	if vFielder.IsSlice {
		filderT = vFielder.SliceType
	} else {
		filderT = vFielder
	}

	for rows.Next() {
		cols, err := rows.Columns()
		checkErr(err)
		val := []any{}
		fields := map[string]reflect.Value{}
		for _, c := range cols {
			if v, ok := filderT.Children[c]; ok {
				rv := v.New()
				switch v.Schema.(type) {
				case time.Time:
					v.SkipOnErr = true
					t := &time.Time{}
					rv = reflect.ValueOf(&t)
				}
				fields[v.Name] = rv
				if rv.Kind() == reflect.Pointer {
					if r := rv.Elem(); r.Kind() == reflect.Slice {
						sl := pq.Array(r.Interface())
						if _, ok := sl.(pq.GenericArray); !ok {
							rv = reflect.ValueOf(sl)
						}
					}
				}
				val = append(val, rv.Interface())
			}
		}
		checkErr(rows.Scan(val...))
		data, err := c3po.Encode(fields)
		checkErr(err)
		value, err := filderT.Decode(data)
		checkErr(err)

		v := reflect.ValueOf(value)
		switch {
		case vFielder.IsSlice:
			vals = reflect.Append(vals, v)
		default:
			vals = v
		}

	}
	if vFielder.IsSlice {
		dstRV.Grow(vals.Cap())
		dstRV.SetLen(vals.Len())
		reflect.Copy(dstRV, vals)
		return
	}
	if vals.Kind() == reflect.Ptr {
		vals = vals.Elem()
	}
	dstRV.Set(vals)

	db.SQL.Reset()
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
