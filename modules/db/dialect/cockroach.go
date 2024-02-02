package dialect

import "fmt"

type cockroach struct {
	commonDialect
}

func (cockroach) GetName() string {
	return "cockroach"
}

func (cockroach) ShowTables() string {
	return "SHOW TABLES;"
}

func (cockroach) ShowColumns(table string) string {
	return fmt.Sprintf("SHOW COLUMNS FROM %s;", table)
}
