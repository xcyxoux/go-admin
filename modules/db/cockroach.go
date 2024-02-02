// Copyright 2019 GoAdmin Core Team. All rights reserved.
// Use of this source code is governed by a Apache-2.0 style
// license that can be found in the LICENSE file.

package db

import (
	"database/sql"
	"fmt"
	"github.com/GoAdminGroup/go-admin/modules/config"
)

// Cockroach is a Connection of cockroach.
type Cockroach struct {
	Base
}

// GetCockroachDB return the global cockroach connection.
func GetCockroachDB() *Cockroach {
	return &Cockroach{
		Base: Base{
			DbList: make(map[string]*sql.DB),
		},
	}
}

// Name implements the method Connection.Name.
func (db *Cockroach) Name() string {
	return "cockroach"
}

// GetDelimiter implements the method Connection.GetDelimiter.
func (db *Cockroach) GetDelimiter() string {
	return `"`
}

// GetDelimiter2 implements the method Connection.GetDelimiter2.
func (db *Cockroach) GetDelimiter2() string {
	return `"`
}

// GetDelimiters implements the method Connection.GetDelimiters.
func (db *Cockroach) GetDelimiters() []string {
	return []string{`"`, `"`}
}

// QueryWithConnection implements the method Connection.QueryWithConnection.
func (db *Cockroach) QueryWithConnection(con string, query string, args ...interface{}) ([]map[string]interface{}, error) {
	return CommonQuery(db.DbList[con], filterQuery(query), args...)
}

// ExecWithConnection implements the method Connection.ExecWithConnection.
func (db *Cockroach) ExecWithConnection(con string, query string, args ...interface{}) (sql.Result, error) {
	return CommonExec(db.DbList[con], filterQuery(query), args...)
}

// Query implements the method Connection.Query.
func (db *Cockroach) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {
	return CommonQuery(db.DbList["default"], filterQuery(query), args...)
}

// Exec implements the method Connection.Exec.
func (db *Cockroach) Exec(query string, args ...interface{}) (sql.Result, error) {
	return CommonExec(db.DbList["default"], filterQuery(query), args...)
}

func (db *Cockroach) QueryWith(tx *sql.Tx, conn, query string, args ...interface{}) ([]map[string]interface{}, error) {
	if tx != nil {
		return db.QueryWithTx(tx, query, args...)
	}
	return db.QueryWithConnection(conn, query, args...)
}

func (db *Cockroach) ExecWith(tx *sql.Tx, conn, query string, args ...interface{}) (sql.Result, error) {
	if tx != nil {
		return db.ExecWithTx(tx, query, args...)
	}
	return db.ExecWithConnection(conn, query, args...)
}

//func filterQuery(query string) string {
//	queCount := strings.Count(query, "?")
//	for i := 1; i < queCount+1; i++ {
//		query = strings.Replace(query, "?", "$"+strconv.Itoa(i), 1)
//	}
//	query = strings.ReplaceAll(query, "`", "")
//	// TODO: add " to the keyword
//	return strings.ReplaceAll(query, "by order ", `by "order" `)
//}

// InitDB implements the method Connection.InitDB.
func (db *Cockroach) InitDB(cfgList map[string]config.Database) Connection {
	db.Configs = cfgList
	db.Once.Do(func() {
		for conn, cfg := range cfgList {

			fmt.Println("check db config", cfg.GetDSN())

			// works with postgres driver
			sqlDB, err := sql.Open("postgres", cfg.GetDSN())
			if err != nil {
				if sqlDB != nil {
					_ = sqlDB.Close()
				}
				panic(err)
			}

			sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
			sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
			sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
			sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

			db.DbList[conn] = sqlDB

			if err := sqlDB.Ping(); err != nil {
				panic(err)
			}
		}
	})
	return db
}

// BeginTxWithReadUncommitted starts a transaction with level LevelReadUncommitted.
func (db *Cockroach) BeginTxWithReadUncommitted() *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList["default"], sql.LevelReadUncommitted)
}

// BeginTxWithReadCommitted starts a transaction with level LevelReadCommitted.
func (db *Cockroach) BeginTxWithReadCommitted() *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList["default"], sql.LevelReadCommitted)
}

// BeginTxWithRepeatableRead starts a transaction with level LevelRepeatableRead.
func (db *Cockroach) BeginTxWithRepeatableRead() *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList["default"], sql.LevelRepeatableRead)
}

// BeginTx starts a transaction with level LevelDefault.
func (db *Cockroach) BeginTx() *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList["default"], sql.LevelDefault)
}

// BeginTxWithLevel starts a transaction with given transaction isolation level.
func (db *Cockroach) BeginTxWithLevel(level sql.IsolationLevel) *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList["default"], level)
}

// BeginTxWithReadUncommittedAndConnection starts a transaction with level LevelReadUncommitted and connection.
func (db *Cockroach) BeginTxWithReadUncommittedAndConnection(conn string) *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList[conn], sql.LevelReadUncommitted)
}

// BeginTxWithReadCommittedAndConnection starts a transaction with level LevelReadCommitted and connection.
func (db *Cockroach) BeginTxWithReadCommittedAndConnection(conn string) *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList[conn], sql.LevelReadCommitted)
}

// BeginTxWithRepeatableReadAndConnection starts a transaction with level LevelRepeatableRead and connection.
func (db *Cockroach) BeginTxWithRepeatableReadAndConnection(conn string) *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList[conn], sql.LevelRepeatableRead)
}

// BeginTxAndConnection starts a transaction with level LevelDefault and connection.
func (db *Cockroach) BeginTxAndConnection(conn string) *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList[conn], sql.LevelDefault)
}

// BeginTxWithLevelAndConnection starts a transaction with given transaction isolation level and connection.
func (db *Cockroach) BeginTxWithLevelAndConnection(conn string, level sql.IsolationLevel) *sql.Tx {
	return CommonBeginTxWithLevel(db.DbList[conn], level)
}

// QueryWithTx is query method within the transaction.
func (db *Cockroach) QueryWithTx(tx *sql.Tx, query string, args ...interface{}) ([]map[string]interface{}, error) {
	return CommonQueryWithTx(tx, filterQuery(query), args...)
}

// ExecWithTx is exec method within the transaction.
func (db *Cockroach) ExecWithTx(tx *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	return CommonExecWithTx(tx, filterQuery(query), args...)
}
