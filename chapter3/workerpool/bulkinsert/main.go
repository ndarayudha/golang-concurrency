package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dbConnString  = "root:password@/test_db"
	dbMaxIdleConn = 4
	dbMaxConns    = 100
	totalWorker   = 100
	csvFile       = "dataset.csv"
)

var dataHeaders = make([]string, 0)

func main() {
	starts := time.Now()

	db, err := openConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	duraton := time.Since(starts)
	fmt.Println("done in ", int(math.Ceil(duraton.Seconds())), " seconds")
}

func dispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func doTheJob(workerIndex, counter int, db *sql.DB, values []interface{}) {
	maxRetries := 5 // Maximum number of retries

    for retry := 0; retry < maxRetries; retry++ {
        conn, err := db.Conn(context.Background())
        if err != nil {
            log.Printf("Error creating database connection: %v", err)
            continue // Retry the operation
        }

        query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
            strings.Join(dataHeaders, ","),
            strings.Join(generateQuestionsMark(len(dataHeaders)), ","),
        )

        _, err = conn.ExecContext(context.Background(), query, values...)
        conn.Close()

        if err != nil {
            log.Printf("Error executing database query: %v", err)
            log.Printf("Retrying %d ...", retry)
            continue // Retry the operation
        } else {
            break // Operation succeeded, exit the retry loop
        }
    }

    if counter%100 == 0 {
        log.Println("=> worker", workerIndex, "inserted", counter, "data")
    }
}

func insertData(db *sql.DB, values []interface{}) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
		strings.Join(dataHeaders, ","),
		strings.Join(generateQuestionsMark(len(dataHeaders)), ","),
	)

	_, err = conn.ExecContext(context.Background(), query, values...)
	return err
}

func generateQuestionsMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}
	return s
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("==> open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

func openConnection() (*sql.DB, error) {
	log.Println("==> open db connection")

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxConns)

	return db, nil
}
