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

// TestPrepare ensures that Prepare does not panic.
func TestPrepare(t *testing.T) {
	db := openDB()
	defer closeDB()

	if err := panicked(func() { Prepare(db) }); err != nil {
		t.Error("Prepare: got panic:\n", err)
	}
}

// TestPrepareShouldPanic ensures that Prepare panics when provided an invalid
// database connection.
func TestPrepareShouldPanic(t *testing.T) {
	if err := panicked(func() { Prepare(&sql.DB{}) }); err == nil {
		t.Error("Prepare: expected panic with invalid database connection")
	}
}

// TestExists ensures that Exists returns accurate data.
func TestExists(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	insertFixtures(db, []entryFixture{
		{
			Name:      "foo",
			Value:     "bar",
			ValueType: 3,
		},
	})

	if Exists("bar") {
		t.Errorf("Exists: got 'true' expected 'false'")
	}

	if !Exists("foo") {
		t.Errorf("Exists: got 'false' expected 'true'")
	}
}

// TestBlobStringConversion ensures that data is accurately converted to and
// from blob strings.
func TestBlobStringConversion(t *testing.T) {
	bool1, vtbool1, _ := toBlobString(true)
	if res, err := fromBlobString(bool1, vtbool1); err != nil {
		t.Errorf("fromBlobString: got error:\n%s", err)
	} else {
		if res != true {
			t.Errorf("fromBlobString: got '%b' expected 'true'", res)
		}
	}

	bool2, vtbool2, _ := toBlobString(false)
	if res, err := fromBlobString(bool2, vtbool2); err != nil {
		t.Errorf("fromBlobString: got error:\n%s", err)
	} else {
		if res != false {
			t.Errorf("fromBlobString: got '%b' expected 'false'", res)
		}
	}

	int1, vtint1, _ := toBlobString(583)
	if res, err := fromBlobString(int1, vtint1); err != nil {
		t.Errorf("fromBlobString: got error:\n%s", err)
	} else {
		if res != 583 {
			t.Errorf("fromBlobString: got '%d' expected '583'", res)
		}
	}

	float, vtfloat, _ := toBlobString(43.6812)
	if res, err := fromBlobString(float, vtfloat); err != nil {
		t.Errorf("fromBlobString: got error:\n%s", err)
	} else {
		if res != 43.6812 {
			t.Errorf("fromBlobString: got '%d' expected '43.6812'", res)
		}
	}

	string1, vtstring1, _ := toBlobString("hello world!")
	if res, err := fromBlobString(string1, vtstring1); err != nil {
		t.Errorf("fromBlobString: got error:\n%s", err)
	} else {
		if res != "hello world!" {
			t.Errorf("fromBlobString: got '%d' expected 'hello world!'", res)
		}
	}

	if _, _, err := toBlobString([]string{"disallowed", "type"}); err == nil {
		t.Errorf("fromBlobString: expected error with disallowed type")
	}

	if _, err := fromBlobString("invalid", 0); err == nil {
		t.Errorf("fromBlobString: expected error with invalid value for parsing")
	} else {
		if _, ok := err.(*ErrFailedToParse); !ok {
			t.Errorf("fromBlobString: expected error of type *ErrFailedToParse, got %s", err)
		}
	}

	if _, err := fromBlobString("invalid", 1); err == nil {
		t.Errorf("fromBlobString: expected error with invalid value for parsing")
	} else {
		if _, ok := err.(*ErrFailedToParse); !ok {
			t.Errorf("fromBlobString: expected error of type *ErrFailedToParse, got %s", err)
		}
	}

	if _, err := fromBlobString("invalid", 2); err == nil {
		t.Errorf("fromBlobString: expected error with invalid value for parsing")
	} else {
		if _, ok := err.(*ErrFailedToParse); !ok {
			t.Errorf("fromBlobString: expected error of type *ErrFailedToParse, got %s", err)
		}
	}

	if _, err := fromBlobString("12.8", 100); err == nil {
		t.Errorf("fromBlobString: expected error with invalid value type for parsing")
	}
}

// TestGetValueType ensures that getValueType returns accurate data.
func TestGetValueType(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	insertFixtures(db, []entryFixture{
		{
			Name:      "foo",
			Value:     "1",
			ValueType: 0,
		},
		{
			Name:      "bar",
			Value:     "1011",
			ValueType: 1,
		},
	})

	valueTypeFoo, err := getValueType("foo")
	if err != nil {
		t.Errorf("getValueType: got error:\n%s", err)
	}
	if valueTypeFoo != 0 {
		t.Errorf("getValueType: got '%d' expected '0'", valueTypeFoo)
	}

	valueTypeBar, err := getValueType("bar")
	if err != nil {
		t.Errorf("getValueType: got error:\n%s", err)
	}
	if valueTypeBar != 1 {
		t.Errorf("getValueType: got '%d' expected 1", valueTypeBar)
	}

	_, err = getValueType("unknown")
	if err == nil {
		t.Errorf("getValueType: expected error with missing entry")
	} else {
		if _, ok := err.(*ErrNoEntry); !ok {
			t.Errorf("getValueType: expected error of type *ErrNoEntry")
		}
	}
}

// TestGetAndSet ensures that Get and Set respond as expected to different
// combinations of data and that data can be accurately read and updated
// once set.
func TestGetAndSet(t *testing.T) {
	db := openDB()
	defer closeDB()
	Prepare(db)

	if err := Set("foo", true); err != nil {
		t.Fatal("Set: got error:\n", err)
	}

	if foo, err := Get("foo"); err != nil {
		t.Error("Get: got error:\n", err)
	} else {
		if res, ok := foo.(bool); ok {
			if res != true {
				t.Errorf("Get: got '%t' expected 'true'", res)
			}
		} else {
			t.Error("Get: got result of an unknown type, expected 'bool'")
		}
	}

	if _, err := Get("bar"); err == nil {
		t.Error("Get: expected error with non-existent entry")
	}

	if err := Set("foo", false); err != nil {
		t.Fatal("Set: got error:\n", err)
	}

	foo := MustGet("foo")
	if res, ok := foo.(bool); ok {
		if res != false {
			t.Errorf("MustGet got '%t' expected 'false'", res)
		}
	} else {
		t.Error("MustGet: got result of an unknown type, expected 'bool'")
	}

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
		t.Error("MustSet: got error:\n", err)
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
