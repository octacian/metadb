/*
Package metadb provides key-value data storage APIs for interaction with a
metadata table within an SQL database.

Basics

To get started with metadb, open a database connection and prepare the metadata
table:

	database, _ := sql.Open(...) // Open a database connection
	defer database.Close()

	metadb.Prepare(database) // Prepare the metadata table

	metadb.Set("foo", "bar") // Set key "foo" to contain value "bar"
	fmt.Println(metadb.Get("foo")) // Retrieve and print key "foo"
*/
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

// Error implements the error interface for ErrNoEntry.
func (err *ErrNoEntry) Error() string {
	return fmt.Sprintf("metadb: no entry for '%s'", err.Name)
}

// ErrFailedToParse is returned indirectly by Get when a blob string cannot be
// parsed.
type ErrFailedToParse struct {
	Err error
}

// Error implements the error interface for ErrFailedToParse.
func (err *ErrFailedToParse) Error() string {
	return fmt.Sprintf("metadb: failed to parse value blob string:\n%s", err.Err)
}

// Prepare takes a database object and creates the metadata table if it doesn't exist.
func Prepare(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS metadata(
			ID INT AUTO_INCREMENT PRIMARY KEY,
			Name VARCHAR(255) NOT NULL UNIQUE,
			Value BLOB NOT NULL,
			ValueType TINYINT NOT NULL
			-- 0 = bool, 1 = int, 2 = float64, 3 = string
		);
	`)

	if err != nil {
		panic(fmt.Errorf("failed to create metadata table:\n%s", err))
	}

	database = db
}

// Exists returns true if the requested entry exists, and false if it does not.
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

// toBlobString takes a value interface and checks its type, returning an
// unsigned integer representing this type and a string containing binary
// data representing it. If the type is not allowed, an error is returned.
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

// fromBlobString takes a string and an unsigned integer. The string is
// retrieved directly from the database and contains some raw data, while the
// unsigned integer represents the type of data retrieved and therefore how it
// is to be processed. An interface containing the decoded value is returned,
// or an error if conversion fails or the data type is invalid.
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

// getValueType returns an unsigned integer representing the type of data
// stored in the requested metadata entry, or an ErrNoEntry if none exists.
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

// Get returns an interface containing the data within the requested entry. If
// the entry does not exist or if the stored data type identifier is invalid,
// an error is returned.
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

// MustGet does the same as Get, but panics if an error is returned.
func MustGet(name string) interface{} {
	if res, err := Get(name); err != nil {
		panic(err)
	} else {
		return res
	}
}

// set implements the code shared between Set and ForceSet, using an additional
// parameter to differentiate between the two.
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

// Set inserts or updates a metadata entry. If the type of the new value is not
// one of bool, int, float64, or string, an error is returned. Or, if the entry
// already exists and the data type of the new value is different than that of
// the current, an error is also returned.
func Set(name string, value interface{}) error {
	return set(name, value, false)
}

// MustSet does the same as Set, but panics if an error is returned.
func MustSet(name string, value interface{}) {
	if err := Set(name, value); err != nil {
		panic(err)
	}
}

// ForceSet does the same as Set, but does not return an error if the entry
// already exists and the data type of the new value is different than that of
// the current.
func ForceSet(name string, value interface{}) error {
	return set(name, value, true)
}

// MustForceSet does the same as ForceSet, but panics if an error is returned.
func MustForceSet(name string, value interface{}) {
	if err := ForceSet(name, value); err != nil {
		panic(err)
	}
}
