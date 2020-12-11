// main.go

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/taflaj/util/reader"

	"github.com/taflaj/merge/models"
)

// Env contains the database access environment
type Env struct {
	db models.DataStore
}

// to store all raw (unformatted) records
var raw []string
var duplicates int

func init() {
	log.SetFlags(log.Flags() | log.Lmicroseconds | log.Lshortfile)
}

func check(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func read(file string, env *Env) {
	// read each input file
	log.Printf("Reading %v\n", file)
	in := reader.NewLineReader(file)
	for {
		_, line, ok := in.ReadLine()
		if !ok {
			break
		}
		input := strings.Trim(line, " \t") // remove superfluous whitespace
		if len(input) > 0 {                // ignore blank lines
			record, err := models.NewRecord(input)
			check(err)
			if record == nil {
				raw = append(raw, input)
			} else {
				_, dup, err := env.db.Add(record)
				check(err)
				if dup {
					duplicates++
					// log.Printf("Line #%v is a duplicate of record #%v", n, id)
					// } else {
					// 	log.Printf("Record #%v belongs to %v", id, record.Name)
				}
			}
		}
	}
	log.Printf("Read %v lines\n", in.GetLines())
}

func write(w *bufio.Writer, line string) error {
	_, err := w.WriteString(line + "\n\n")
	return err
}

func run(files []string) {
	// instantiate a temporary database
	db, err := models.Open("file::memory:?cache=shared")
	check(err)
	env := &Env{db}
	defer env.db.Close()
	// read all files
	for i := 0; i < len(files); i++ {
		read(files[i], env)
	}
	log.Printf("Skipped %v duplicate records", duplicates)
	// write consolidated file
	log.Printf("Writing %v", files[0])
	f, err := os.Create(files[0]) // overwrite first file
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	// save unformatted records first
	for i := 0; i < len(raw); i++ {
		err = write(w, raw[i])
		check(err)
	}
	// save database data
	in := models.NewRecordReader(db)
	for {
		record, ok := in.ReadRecord()
		if !ok {
			break
		}
		stamp := record.TimeStamp
		date := stamp.Format("20060102")
		day := stamp.Weekday().String()[:3]
		time := stamp.Format("150405")
		zone := stamp.Format("MST")
		err = write(w, fmt.Sprintf("%v-%v-%v-%v|%v_%v|%v", date, day, time, zone, record.Name, record.Node, record.Message))
		check(err)
	}
	w.Flush()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please include at least one pubsub log file.")
		fmt.Printf("Usage: %v file [file [...]]\n", os.Args[0])
		fmt.Println("The consolidated log will be saved on the first file.")
		fmt.Println("If the first file is not blank, it will be read before being overwritten.")
		fmt.Println("Note: all unformatted records will be saved at the top of the consolidated log file.")
	} else {
		run(os.Args[1:])
	}
}
