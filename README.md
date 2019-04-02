# metadb

This package provides a simple but powerful key-value data storage API. Information is saved in the `metadata` table of an SQL database, passed to the library as an `sql.DB` object. Its key feature is the ability to accept and return multiple different data types (booleans, integers, floats, and strings) through interfaces. When storing some data, its type is also included, allowing it to be parsed back into a native Go type before being returned to the caller via an interface.

API documentation can be found at [GoDoc](https://godoc.org/github.com/octacian/metadb).
