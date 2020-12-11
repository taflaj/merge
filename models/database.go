// models/database.go

package models

import (
	"database/sql"
	"log"
	"time"

	// to isolate database details from the remainder of the application
	_ "github.com/mattn/go-sqlite3"
)

// DataStore implements model methods
type DataStore interface {
	Add(*Record) (int64, bool, error)
	Close() error
}

// DB is used to encapsulate sql.DB
type DB struct {
	*sql.DB
}

// RecordReader provides an asynchronouw record reader
type RecordReader struct {
	buffer chan *Record
}

func execute(db *sql.DB, cmd string, params ...interface{}) (sql.Result, error) {
	stmt, err := db.Prepare(cmd)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	// log.Printf("stmt: %#v", stmt)
	return stmt.Exec(params...)
}

func initialize(db *sql.DB) error {
	// create database tables
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	commands := []string{
		"CREATE TABLE IF NOT EXISTS Messages(\n" +
			"  ID INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
			"  TimeStamp TEXT NOT NULL,\n" +
			"  Name TEXT(16) NOT NULL,\n" +
			"  Node TEXT(4) NOT NULL,\n" +
			"  Message TEXT)",
		"CREATE INDEX IF NOT EXISTS Combo ON Messages(TimeStamp, Name, Node)",
	}
	for i := 0; i < len(commands); i++ {
		if _, err = execute(db, commands[i]); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Open connects to the database and initializes it
func Open(dataSourceName string) (*DB, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	if err = initialize(db); err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// Close closes the database
func (db *DB) Close() error {
	return db.DB.Close()
}

// Add adds a new record to the database, making sure it's not a duplicate
func (db *DB) Add(record *Record) (int64, bool, error) {
	stamp := record.TimeStamp.Format(Layout)
	// log.Printf("Inserting %v %v %v", stamp, record.Name, record.Node)
	// see if it's a duplicate
	query := "SELECT ID, Message FROM Messages WHERE TimeStamp=? AND Name=? AND Node=?"
	rows, err := db.Query(query, stamp, record.Name, record.Node)
	if err != nil {
		return -1, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var message string
		if err = rows.Scan(&id, &message); err != nil {
			return -1, false, err
		}
		if message == record.Message {
			// this is a duplicate message
			return id, true, nil
		}
	}
	// if we got here, it's because it's a new unique message
	cmd := "INSERT INTO Messages(TimeStamp, Name, Node, Message) VALUES(?, ?, ?, ?)"
	result, err := execute(db.DB, cmd, stamp, record.Name, record.Node, record.Message)
	if err != nil {
		return -1, false, err
	}
	// get record ID
	id, err := result.LastInsertId()
	return id, false, nil
}

func read(db *DB, rr *RecordReader) {
	query := "SELECT TimeStamp, Name, Node, Message FROM Messages ORDER BY TimeStamp ASC"
	rows, err := db.Query(query)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()
	// read each row
	for rows.Next() {
		var stamp string
		r := Record{}
		if err = rows.Scan(&stamp, &r.Name, &r.Node, &r.Message); err != nil {
			log.Panic(err)
		}
		dt, err := time.Parse(Layout, stamp)
		if err != nil {
			log.Panic(err)
		}
		r.TimeStamp = &dt
		// add to the queue
		rr.buffer <- &r
	}
	close(rr.buffer)
}

// ReadRecord returns the next record from the database
func (rr *RecordReader) ReadRecord() (*Record, bool) {
	record, ok := <-rr.buffer
	return record, ok
}

// NewRecordReader creates a new RecordReader with default buffer size
func NewRecordReader(db *DB) *RecordReader {
	rr := RecordReader{}
	rr.buffer = make(chan *Record, 10)
	go read(db, &rr)
	return &rr
}
