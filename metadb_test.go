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

// TODO: Should unit tests be refactored so that all tests of methods attached
// to Instance are coupled to the test for NewInstance itself? This could
// entirely eliminate the need to work with fixtures as all data would be
// directly manipulated by the very methods being tested. Not only that, but
// this might eliminate the need to separately test toValueType and
// fromBlobString.

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

// RunWithDB runs a closure passing it a database handle which is disposed of
// afterward.
func RunWithDB(fn func(*sql.DB)) {
	db, err := sql.Open("sqlite3", TestDBPath)
	if err != nil {
		panic(err)
	}

	fn(db)

	err = db.Close()
	if err != nil {
		panic(err)
	}

	if err := os.Remove(TestDBPath); err != nil {
		panic(err)
	}
}

// RunWithInstance runs a closure passing it an Instance.
func RunWithInstance(fn func(*Instance)) {
	RunWithDB(func(db *sql.DB) {
		if instance, err := NewInstance(db); err != nil {
			panic(err)
		} else {
			fn(instance)
		}
	})
}

// EntryFixture contains the basic data required for a metadata entry.
type EntryFixture struct {
	Name      string
	Value     interface{}
	ValueType uint
}

// InsertFixtures takes a list of EntryFixtures and inserts them into the
// database handle managed by the provided Instance.
func InsertFixtures(instance *Instance, fixtures []EntryFixture) {
	for _, fixture := range fixtures {
		_, err := instance.DB.Exec(`
			INSERT INTO metadata (Name, Value, ValueType) Values (?, ?, ?)
		`, fixture.Name, fixture.Value, fixture.ValueType)

		if err != nil {
			panic(fmt.Sprint("tests: failed to insert fixtures:\n", err))
		}
	}
}

// GetFixtures returns an array of EntryFixtures read from all the metadata
// entries in the database managed by the provided Instance.
func GetFixtures(instance *Instance) map[string]*EntryFixture {
	rows, err := instance.DB.Query("SELECT Name, Value, ValueType FROM metadata;")
	if err != nil {
		panic(fmt.Sprint("tests: failed to retrieve fixtures:\n", err))
	}

	fixtures := make(map[string]*EntryFixture)
	for rows.Next() {
		var value string
		fixture := EntryFixture{}
		if err := rows.Scan(&fixture.Name, &value, &fixture.ValueType); err != nil {
			panic(fmt.Errorf("tests: failed to scan row while retrieving fixtures:\n%s", err))
		}
		fixture.Value = value
		fixtures[fixture.Name] = &fixture
	}

	return fixtures
}

// TestNewInstance ensures that an Instance object is returned as expected with
// a valid database handle, and an error with an invalid handle.
func TestNewInstance(t *testing.T) {
	if _, err := NewInstance(nil); err == nil {
		t.Error("NewInstance: expected error with nil database handle")
	}

	RunWithDB(func(db *sql.DB) {
		if _, err := NewInstance(db); err != nil {
			t.Fatal("NewInstance: got error:\n", err)
		}
	})
}

// TestExists ensures that Instance.Exists is accurate.
func TestExists(t *testing.T) {
	RunWithInstance(func(instance *Instance) {
		InsertFixtures(instance, []EntryFixture{
			{Name: "foo", Value: "bar", ValueType: 3},
		})

		if instance.Exists("bar") {
			t.Error("Instance.Exists: got 'true' expected 'false'")
		}

		if !instance.Exists("foo") {
			t.Error("Instance.Exists: got 'false' expected 'true'")
		}
	})
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
	RunWithInstance(func(instance *Instance) {
		InsertFixtures(instance, []EntryFixture{
			{Name: "bool", Value: true, ValueType: 0},
			{Name: "invalidBool", Value: "maybe", ValueType: 0},
			{Name: "int", Value: 239, ValueType: 1},
			{Name: "invalidInt", Value: "not a number", ValueType: 1},
			{Name: "float", Value: 21.42, ValueType: 2},
			{Name: "invalidFloat", Value: "21.48aje21", ValueType: 2},
			{Name: "string", Value: "hello world!", ValueType: 3},
			{Name: "unknown", Value: "nothing", ValueType: 100},
		})

		fixtures := GetFixtures(instance)

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
	})
}

// TestGetValueType ensures that getValueType returns accurate data.
func TestGetValueType(t *testing.T) {
	RunWithInstance(func(instance *Instance) {
		InsertFixtures(instance, []EntryFixture{
			{Name: "foo", Value: "1", ValueType: 0},
			{Name: "bar", Value: "1011", ValueType: 1},
		})

		testValueType := func(name string, expected uint) {
			if res, err := instance.getValueType(name); err != nil {
				t.Error("Instance.getValueType: got error:\n", err)
			} else if res != expected {
				t.Errorf("Instance.getValueType: got '%d' expected '%d'", res, expected)
			}
		}

		testValueType("foo", 0)
		testValueType("bar", 1)

		_, err := instance.getValueType("unknown")
		if err == nil {
			t.Error("Instance.getValueType: expected error with missing entry")
		} else if _, ok := err.(*ErrNoEntry); !ok {
			t.Error("Instance.getValueType: expected error of type *ErrNoEntry")
		}
	})
}

// TestGetAndSet ensures that Get and Set respond as expected to different
// combinations of data and that data can be accurately read and updated
// once set.
func TestGetAndSet(t *testing.T) {
	RunWithInstance(func(instance *Instance) {
		checkResultWithBool := func(name string, fetched interface{}, expected bool) {
			if res, ok := fetched.(bool); ok {
				if res != expected {
					t.Errorf("Instance.%s: got '%t' expected '%t'", name, res, expected)
				}
			} else {
				t.Errorf("Instance.%s: got result of an unknown type, expected 'bool'", name)
			}
		}

		if err := instance.Set("foo", true); err != nil {
			t.Fatal("Instance.Set: got error:\n", err)
		}

		if foo, err := instance.Get("foo"); err != nil {
			t.Error("Instance.Get: got error:\n", err)
		} else {
			checkResultWithBool("Get", foo, true)
		}

		if _, err := instance.Get("bar"); err == nil {
			t.Error("Instance.Get: expected error with non-existent entry")
		}

		if err := instance.Set("foo", false); err != nil {
			t.Fatal("Instance.Set: got error:\n", err)
		}

		foo := instance.MustGet("foo")
		checkResultWithBool("MustGet", foo, false)

		if err := panicked(func() { instance.MustGet("bar") }); err == nil {
			t.Error("Instance.MustGet: expected panic with non-existent entry")
		} else if _, ok := err.(*ErrNoEntry); !ok {
			t.Error("Instance.MustGet: expected error of type *ErrNoEntry")
		}

		if err := instance.Set("foo", []string{"disallowed", "type"}); err == nil {
			t.Error("Instance.Set: expected error with new value of disallowed type")
		}

		if err := instance.Set("foo", 1784); err == nil {
			t.Error("Instance.Set: expected error with new value of different type than existing")
		}

		if err := panicked(func() { instance.MustSet("foo", true) }); err != nil {
			t.Error("Instance.MustSet: got panic:\n", err)
		}

		if err := panicked(func() { instance.MustSet("foo", 1834) }); err == nil {
			t.Error("Instance.MustSet: expected panic with new value of different type than existing")
		}

		if err := instance.ForceSet("foo", 1873); err != nil {
			t.Error("Instance.ForceSet: got error:\n", err)
		}

		if err := panicked(func() { instance.MustForceSet("foo", 1891) }); err != nil {
			t.Error("Instance.MustForceSet: got panic:\n", err)
		}

		if err := panicked(func() { instance.MustForceSet("foo", []string{"disallowed", "type"}) }); err == nil {
			t.Error("Instance.MustForceSet: expected panic with new value of disallowed type")
		}
	})
}

// TestDelete ensures that metadata entries inserted by means of a fixture are
// properly deleted and that attempting to delete a non-existent entry results
// in an ErrNoEntry.
func TestDelete(t *testing.T) {
	RunWithInstance(func(instance *Instance) {
		InsertFixtures(instance, []EntryFixture{
			{Name: "int", Value: "2891", ValueType: 1},
			{Name: "string", Value: "hello world!", ValueType: 3},
		})

		if err := instance.Delete("int"); err != nil {
			t.Error("Instance.Delete: got error:\n", err)
		}

		if err := panicked(func() { instance.MustDelete("string") }); err != nil {
			t.Error("Instance.MustDelete: got panic:\n", err)
		}

		if err := instance.Delete("foo"); err == nil {
			t.Error("Instance.Delete: expected error with non-existent entry")
		} else if _, ok := err.(*ErrNoEntry); !ok {
			t.Error("Instance.Delete: expected error of type *ErrNoEntry")
		}

		if err := panicked(func() { instance.MustDelete("foo") }); err == nil {
			t.Error("Instance.MustDelete: expected panic with non-existent entry")
		}
	})
}
