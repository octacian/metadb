package metadb

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

var database *sql.DB

// ErrNoEntry is returned by Get when a requested entry does not exist.
type ErrNoEntry struct {
	Name string
}

// Error implements the error interface for ErrNoEntry
func (err *ErrNoEntry) Error() string {
	return fmt.Sprintf("metadb: no entry for '%s'", err.Name)
}

// ErrFailedToParse is returned by fromBlobString when a blob string cannot be
// parsed.
type ErrFailedToParse struct {
	Err error
}

// Error implements the error interface for ErrFailedToParse
func (err *ErrFailedToParse) Error() string {
	return fmt.Sprintf("metadb: failed to parse value blob string:\n%s", err.Err)
}

// Prepare takes a database object and creates the metadata table if it doesn't exist
func Prepare(db *sql.DB) {
	//-- 0 = bool, 1 = int, 2 = float64, 3 = string
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS metadata(
			ID INT AUTO_INCREMENT PRIMARY KEY,
			Name VARCHAR(255) NOT NULL UNIQUE,
			Value BLOB NOT NULL,
			ValueType TINYINT NOT NULL
		);
	`)

	if err != nil {
		//fmt.Println(err)
		panic(fmt.Errorf("failed to create metadata table:\n%s", err))
	}

	database = db
}

// Exists returns a boolean indicating whether the named metadata entry exists
func Exists(name string) bool {
	row := database.QueryRow("SELECT Name FROM metadata WHERE name = ?;", name)
	var receivedName string
	err := row.Scan(&receivedName)

	if err != nil {
		// if no rows were selected, return false
		if err == sql.ErrNoRows {
			return false
		}

		panic(fmt.Errorf("failed to check if metadata entry for '%s' exists:\n%s", name, err))
	}

	return true
}

// toBlobString takes a value interface and checks its type, returning not only
// an unsigned integer representing this type, but also a string containing
// binary data representing it. If the type is not allowed, an error is
// returned.
func toBlobString(value interface{}) (string, uint, error) {
	switch value.(type) {
	case bool:
		return strconv.FormatBool(value.(bool)), 0, nil
	case int:
		return strconv.FormatInt(int64(value.(int)), 10), 1, nil
	case float64:
		return strconv.FormatFloat(value.(float64), 'E', -1, 64), 2, nil
	case string:
		return value.(string), 3, nil
	default:
		return "", 0, errors.New("metadb: attempt to set a value with a disallowed type")
	}
}

// fromBlobString takes a string and an unsigned integer, the former
// being the result of retreiving a blob string from the database and the
// latter denoting the type of data stored within the blob string. An interface
// containing the decoded value is returned, and an error if anything goes
// wrong.
func fromBlobString(value string, valueType uint) (interface{}, error) {
	switch valueType {
	case 0: // value is a boolean
		res, err := strconv.ParseBool(value)
		if err != nil {
			return nil, &ErrFailedToParse{err}
		}

		return res, nil
	case 1: // value is an int
		res, err := strconv.ParseInt(value, 10, 0)
		if err != nil {
			return nil, &ErrFailedToParse{err}
		}

		return int(res), nil
	case 2: // value is a float64
		res, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, &ErrFailedToParse{err}
		}

		return res, nil
	case 3: // value is a string
		return value, nil
	default:
		return nil, fmt.Errorf("metadb: value type unrecognizable")
	}
}

// getValueType returns an unsigned integer representing the value stored
// within the metadata entry requested, or an ErrNoEntry if no entry exists.
func getValueType(name string) (uint, error) {
	row := database.QueryRow("SELECT ValueType FROM metadata WHERE name = ?", name)
	var valueType uint
	err := row.Scan(&valueType)

	if err != nil {
		// if no rows were selected, return ErrNoEntry
		if err == sql.ErrNoRows {
			return 0, &ErrNoEntry{name}
		}

		return 0, err
	}

	return valueType, nil
}

// Get returns an interface containing the value of the metadata entry requested,
// if it exists, and an error if it does not exist or if the ValueType is invalid
func Get(name string) (interface{}, error) {
	row := database.QueryRow("SELECT Value, ValueType FROM metadata WHERE name = ?", name)
	var value string
	var valueType uint
	err := row.Scan(&value, &valueType)

	if err != nil {
		// if no rows were selected, return an error
		if err == sql.ErrNoRows {
			return nil, &ErrNoEntry{name}
		}

		return nil, err
	}

	return fromBlobString(value, valueType)
}

// MustGet returns an interface containing the value of the metadata entry requested,
// and panics if it does not exist or the ValueType is invalid
func MustGet(name string) interface{} {
	if res, err := Get(name); err != nil {
		panic(err)
	} else {
		return res
	}
}

// set implements the code shared between Set and ForceSet, including an
// additional argument to control whether updating is forced.
func set(name string, value interface{}, force bool) error {
	_, valueType, err := toBlobString(value)
	if err != nil {
		return err
	}

	currentType, err := getValueType(name)
	if err != nil {
		// if error indicates that there is no entry by this name, insert one
		if _, ok := err.(*ErrNoEntry); ok {
			_, err = database.Exec(`INSERT INTO metadata (Name, Value, ValueType) VALUES (?, ?, ?);`, name, value, valueType)
			if err != nil {
				return fmt.Errorf("metadb: failed to insert entry for '%s':\n%s", name, err)
			}
		}

		return err // Otherwise, return the error
	}

	// if force is not true and valueType does not match currentType, return an error
	if !force && valueType != currentType {
		return fmt.Errorf("metadb: cannot change value for '%s' to one of a different type", name)
	}

	// Update entry
	_, err = database.Exec(`UPDATE metadata SET Value = ? WHERE Name = ?;`, value, name)
	if err != nil {
		return fmt.Errorf("metadb: failed to update entry for '%s':\n%s", name, err)
	}

	return nil
}

// Set inserts or updates a metadata entry and returns an error if the data
// inputted is not of an allowed type or clashes with the type of the data
// currently stored within the entry.
//
// Allowed types are:
// * bool
// * int
// * float64
// * string
func Set(name string, value interface{}) error {
	return set(name, value, false)
}

// MustSet inserts or updates an entry and panics if the data inputted is not
// of an allowed type or clashes with the type of the data currently stored
// within the entry.
func MustSet(name string, value interface{}) {
	if err := Set(name, value); err != nil {
		panic(err)
	}
}

// ForceSet inerts or updates an entry and returns an error only if the data
// inputted is not of an allowed type. Attempting to change the value for an
// existing entry to something of a type different than the current will not
// raise an error, unlike Set and MustSet.
func ForceSet(name string, value interface{}) error {
	return set(name, value, true)
}

// MustForceSet does the same as ForceSet, but panics if an error is returned.
func MustForceSet(name string, value interface{}) {
	if err := ForceSet(name, value); err != nil {
		panic(err)
	}
}
