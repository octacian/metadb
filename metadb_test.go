package metadb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const TestDBPath = "./test.sqlite"

// panicked takes a simple function to execute and returns an error containing
// the data passed to panic from within the function, and nil if no panic
// occurred.
func panicked(fn func()) error {
	ch := make(chan error)
	go func() {
		defer func() {
			// if function didn't panic, return nil
			if r := recover(); r == nil {
				ch <- nil
			} else { // else, return error
				switch r.(type) {
				case error:
					ch <- r.(error)
				case string:
					ch <- errors.New(r.(string))
				default:
					ch <- nil
				}
			}
		}()

		fn()
	}()

	return <-ch
}

// openDB connects to an SQLite3 database at the path specified by TestDBPath.
func openDB() *sql.DB {
	db, err := sql.Open("sqlite3", TestDBPath)
	if err != nil {
		panic(err)
	}

	return db
}

// closeDB disconnects from the database, if one is connected, and removes the
// residual SQLite file.
func closeDB() {
	if database != nil {
		err := database.Close()
		if err != nil {
			panic(err)
		}
	}

	database = nil

	if err := os.Remove(TestDBPath); err != nil {
		panic(err)
	}
}

// entryFixture contains the basic data required for a metadata entry.
type entryFixture struct {
	Name      string
	Value     interface{}
	ValueType uint
}

// insertFixtures takes a list of entryFixtures and inserts them into the
// provided database.
func insertFixtures(db *sql.DB, fixtures []entryFixture) {
	for _, fixture := range fixtures {
		_, err := db.Exec(`
			INSERT INTO metadata (Name, Value, ValueType) Values (?, ?, ?)
		`, fixture.Name, fixture.Value, fixture.ValueType)

		if err != nil {
			panic(fmt.Sprint("tests: failed to insert fixtures:\n", err))
		}
	}
}

// getFixtures returns an array of entryFixtures read from all the metadata
// entries in the provided database, most commonly used after insertFixtures.
func getFixtures(db *sql.DB) map[string]*entryFixture {
	rows, err := db.Query("SELECT Name, Value, ValueType FROM metadata;")
	if err != nil {
		panic(fmt.Sprint("tests: failed to retrieve fixtures:\n", err))
	}

	fixtures := make(map[string]*entryFixture)
	for rows.Next() {
		var value string
		fixture := entryFixture{}
		if err := rows.Scan(&fixture.Name, &value, &fixture.ValueType); err != nil {
			panic(fmt.Sprint("tests: failed to scan row while retrieving fixtures:\n", err))
		}
		fixture.Value = value
		fixtures[fixture.Name] = &fixture
	}

	return fixtures
}

// TestPrepare ensures that Prepare does not panic.
func TestPrepare(t *testing.T) {
	db := openDB()
	defer closeDB()

	if err := panicked(func() { Prepare(db) }); err != nil {
		t.Fatal("Prepare: got panic:\n", err)
	}
}

// TestPrepareShouldPanic ensures that Prepare panics when provided an invalid
// database connection.
func TestPrepareShouldPanic(t *testing.T) {
	if err := panicked(func() { Prepare(&sql.DB{}) }); err == nil {
		t.Fatal("Prepare: expected panic with invalid database connection")
	}
}

// TestExists ensures that Exists returns accurate data.
func TestExists(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	insertFixtures(db, []entryFixture{
		{Name: "foo", Value: "bar", ValueType: 3},
	})

	if Exists("bar") {
		t.Error("Exists: got 'true' expected 'false'")
	}

	if !Exists("foo") {
		t.Error("Exists: got 'false' expected 'true'")
	}
}

// TestToValueType ensures that the correct type index is returned for each of
// the allowed types.
func TestToValueType(t *testing.T) {
	testValid := func(value interface{}, expected uint) {
		if res, err := toValueType(value); err != nil {
			t.Error("toValueType: got error:\n", err)
		} else if res != expected {
			t.Errorf("toValueType: got '%d' expected '%d'", res, expected)
		}
	}

	testValid(true, 0)
	testValid(281, 1)
	testValid(43.183, 2)
	testValid("hello world!", 3)

	if _, err := toValueType([]string{"disallowed", "type"}); err == nil {
		t.Error("toValueType: expected error with disallowed type")
	}
}

// TestFromBlobString ensures that the correct data is returned for a number
// of combinations of blob strings and value types.
func TestFromBlobString(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	insertFixtures(db, []entryFixture{
		{Name: "bool", Value: true, ValueType: 0},
		{Name: "invalidBool", Value: "maybe", ValueType: 0},
		{Name: "int", Value: 239, ValueType: 1},
		{Name: "invalidInt", Value: "not a number", ValueType: 1},
		{Name: "float", Value: 21.42, ValueType: 2},
		{Name: "invalidFloat", Value: "21.48aje21", ValueType: 2},
		{Name: "string", Value: "hello world!", ValueType: 3},
		{Name: "unknown", Value: "nothing", ValueType: 100},
	})

	fixtures := getFixtures(db)

	testFixture := func(name string, expected interface{}) {
		fixture := fixtures[name]
		res, err := fromBlobString(fixture.Value.(string), fixture.ValueType)
		if err != nil {
			t.Error("fromBlobString: got errror:\n", err)
		} else if res != expected {
			t.Errorf("fromBlobString: got '%v' expected '%v'", res, expected)
		}
	}

	expectError := func(name string, msg string) {
		fixture := fixtures[name]
		if _, err := fromBlobString(fixture.Value.(string), fixture.ValueType); err == nil {
			t.Errorf("fromBlobString: expected error with %s", msg)
		}
	}

	testFixture("bool", true)
	testFixture("int", 239)
	testFixture("float", 21.42)
	testFixture("string", "hello world!")

	expectError("invalidBool", "invalid boolean blob string")
	expectError("invalidInt", "invalid integer blob string")
	expectError("invalidFloat", "invalid float blob string")
	expectError("unknown", "invalid value type")
}

// TestGetValueType ensures that getValueType returns accurate data.
func TestGetValueType(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	insertFixtures(db, []entryFixture{
		{Name: "foo", Value: "1", ValueType: 0},
		{Name: "bar", Value: "1011", ValueType: 1},
	})

	testValueType := func(name string, expected uint) {
		if res, err := getValueType(name); err != nil {
			t.Error("getValueType: got error:\n", err)
		} else if res != expected {
			t.Errorf("getValueType: got '%d' expected '%d'", res, expected)
		}
	}

	testValueType("foo", 0)
	testValueType("bar", 1)

	_, err := getValueType("unknown")
	if err == nil {
		t.Error("getValueType: expected error with missing entry")
	} else if _, ok := err.(*ErrNoEntry); !ok {
		t.Error("getValueType: expected error of type *ErrNoEntry")
	}
}

// TestGetAndSet ensures that Get and Set respond as expected to different
// combinations of data and that data can be accurately read and updated
// once set.
func TestGetAndSet(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	checkResultWithBool := func(name string, fetched interface{}, expected bool) {
		if res, ok := fetched.(bool); ok {
			if res != expected {
				t.Errorf("%s: got '%t' expected '%t'", name, res, expected)
			}
		} else {
			t.Error("Get: got result of an unknown type, expected 'bool'")
		}
	}

	if err := Set("foo", true); err != nil {
		t.Fatal("Set: got error:\n", err)
	}

	if foo, err := Get("foo"); err != nil {
		t.Error("Get: got error:\n", err)
	} else {
		checkResultWithBool("Get", foo, true)
	}

	if _, err := Get("bar"); err == nil {
		t.Error("Get: expected error with non-existent entry")
	}

	if err := Set("foo", false); err != nil {
		t.Fatal("Set: got error:\n", err)
	}

	foo := MustGet("foo")
	checkResultWithBool("MustGet", foo, false)

	if err := panicked(func() { MustGet("bar") }); err == nil {
		t.Error("MustGet: expected panic with non-existent entry")
	} else if _, ok := err.(*ErrNoEntry); !ok {
		t.Error("MustGet: expected error of type *ErrNoEntry")
	}

	if err := Set("foo", []string{"disallowed", "type"}); err == nil {
		t.Error("Set: expected error with new value of disallowed type")
	}

	if err := Set("foo", 1784); err == nil {
		t.Error("Set: expected error with new value of different type than existing")
	}

	if err := panicked(func() { MustSet("foo", true) }); err != nil {
		t.Error("MustSet: got panic:\n", err)
	}

	if err := panicked(func() { MustSet("foo", 1834) }); err == nil {
		t.Error("MustSet: expected panic with new value of different type than existing")
	}

	if err := ForceSet("foo", 1873); err != nil {
		t.Error("ForceSet: got error:\n", err)
	}

	if err := panicked(func() { MustForceSet("foo", 1891) }); err != nil {
		t.Error("MustForceSet: got panic:\n", err)
	}

	if err := panicked(func() { MustForceSet("foo", []string{"disallowed", "type"}) }); err == nil {
		t.Error("MustForceSet: expected panic with new value of disallowed type")
	}
}
