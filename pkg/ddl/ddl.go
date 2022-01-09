// Package ddl holds SQL statements that create or upgrade a store on SQL
// for Terminus.
package ddl

import _ "embed"

//go:embed "store.sql"
var DDL string
