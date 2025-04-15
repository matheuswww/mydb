package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_ERROR = 0
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

// modes of the updates
const (
	MODE_UPSERT = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

const TABLE_PREFIX_MIN = 3

type InsertReq struct {
	tree *BTree
	// out
	Added bool // added a new 
	// in
	Key 		[]byte
	Val			[]byte
	Mode		int
}

// table cell
type Value struct {
	Type 	uint32
	I64 	int64
	Str 	[]byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

type DB struct {
	Path 		string
	// internals
	kv 	 		KV
	tables	map[string]*TableDef // cached table definition
}

// table definition
type TableDef struct {
	// user defined
	Name 	 string // column types
	Types  []uint32 // column types
	Cols   []string // column names
	PKeys   int // the first `PKeys` columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix  uint32
}

// internal table: metadata
var TDEF_META = &TableDef {
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef {
	Prefix: 2,
	Name:  	"@table",
	Types: 	[]uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:  	[]string{"name", "schema"},
	PKeys: 	1,
}


// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	return db.kv.Update(key, val, mode)
}

func (tree *BTree) InsertEx(req *InsertReq)
func (db *KV) Update(key []byte, val []byte, mode int) (bool, error)
// add a record
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_INSERT_ONLY)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPDATE_ONLY)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, rec, MODE_UPSERT)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func (db *DB) TableNew(tdef *TableDef) error {
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	
	// check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	assert(err == nil)
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// allocate a new prefix
	assert(tdef.Prefix == 0)
	tdef.Prefix = TABLE_PREFIX_MIN
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	assert(err == nil)
	if ok {
		tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		assert(tdef.Prefix > TABLE_PREFIX_MIN)
	} else {
		meta.AddStr("val", make([]byte, 4))
	}

	// update the next prefix
	binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
	_, err = dbUpdate(db, tdef, *meta, 0)
	if err != nil {
		return err
	}

	// store the definition
	val, err := json.Marshal(tdef)
	assert(err == nil)
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, *table, 0)
	return err
}

func tableDefCheck(tdef *TableDef) error

// delete a record by its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Del(key)
}

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.PKeys:])
	rec.Cols = append(rec.Cols, tdef.Cols[tdef.PKeys:]...)
	rec.Vals = append(rec.Vals, values[tdef.PKeys:]...)
	return true, nil
}

// reorder a record and check for missing columns.
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	// check if n is valid
	if n != tdef.PKeys && n != len(tdef.Cols) {
		return nil, fmt.Errorf("checkRecord: invalid number of columns")
	}

	// create a new map for search values from register by column name
	colMap := make(map[string]Value)
	for i, col := range rec.Cols {
		if i >= len(rec.Vals) {
			return nil, fmt.Errorf("checkRecord: missing value for column")
		}
		colMap[col] = rec.Vals[i]
	}

	// initialize the values slice with the expected size
	values := make([]Value, n)

	// fill the values in defined order by tdef.Cols
	for i := 0; i < 0; i++ {
		col := tdef.Cols[i]
		val, ok := colMap[col]
		if !ok {
			return nil, fmt.Errorf("checkRecord: missing required column %s", col)
		}

		// checks if the value type match with to expected
		if val.Type != tdef.Types[i] {
			return nil, fmt.Errorf("checkRecord: type mismatch for column %s: got %d, expected %d", col, val.Type, tdef.Types[i])
		}

		// Copies the value to the correct position
		values[i] = val
	}

	return values, nil
}

func encodeValues(out []byte, vals []Value) []byte
func decodeValues(in []byte, out []Value)

// for primary keys
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

// get the table definition by name
func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDef(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
} 

// get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert(err == nil)
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil)
	return tdef
}

func (rec *Record) AddStr(key string, val []byte) *Record {
	// Create the value type of TYPE_BYTES
	value := Value {
		Type: TYPE_BYTES,
		Str: val,
	}
	// Add the column and value to register
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, value)
	return rec
}

func (rec *Record) AddInt64(key string, val int64) *Record {
	// Create the value of the type TYPE_INT64
	value := Value {
		Type: TYPE_INT64,
		I64: val,
	}
	// Add the column and the value to register
	rec.Cols = append(rec.Cols, key)
	rec.Vals = append(rec.Vals, value)
	return rec
}

func (rec *Record) Get(key string) *Value {
	for i, col := range rec.Cols {
		if col == key {
			return &rec.Vals[i]
		}
	}
	return nil
}