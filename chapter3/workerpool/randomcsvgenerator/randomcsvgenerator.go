package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	numEntries = 1000000
	numWorkers = 100
)


type CSVData struct {
	ID             int
	Name           string
	Country        string
	Address        string
	PhoneNumber    string
	CurrentCompany string
	Salary         float64
}

func main() {
	startTime := time.Now()

	// Create a CSV file
	file, err := os.Create("random_data.csv")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the CSV header
	header := []string{"ID", "Name", "Country", "Address", "PhoneNumber", "CurrentCompany", "Salary"}
	writer.Write(header)
    
    jobs := make(chan CSVData, 0)

    // Start worker goroutines
    wg := new(sync.WaitGroup)
    wLock := new(sync.Mutex)
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go worker(i, jobs, wg, writer, wLock)
    }

    // Generate random data entries
    for i := 1; i <= numEntries; i++ {
        data := generateRandomData(i)
        jobs <- data
    }

    close(jobs)
    wg.Wait()

    elapsed := time.Since(startTime)
    fmt.Printf("CSV file generation completed in %.2f s\n", elapsed.Seconds())
}

// Generate and write data sequentially
func withOutGoroutine(writer *csv.Writer) {
    for i := 1; i <= numEntries; i++ {
        data := generateRandomData(i)
        writeToCSV(data, writer)
        fmt.Printf("Generated entry %d\n", data.ID)
    }
}

func worker(workerID int, dataChannel <-chan CSVData, wg *sync.WaitGroup, writer *csv.Writer, writeLock *sync.Mutex) {
	defer wg.Done()

	for data := range dataChannel {
		writeLock.Lock()
		writeToCSV(data, writer)
		writeLock.Unlock()
		fmt.Printf("Worker %d generated entry %d\n", workerID, data.ID)
	}
}

func writeToCSV(data CSVData, writer *csv.Writer) {
	row := []string{
		strconv.Itoa(data.ID),
		data.Name,
		data.Country,
		data.Address,
		data.PhoneNumber,
		data.CurrentCompany,
		strconv.FormatFloat(data.Salary, 'f', 2, 64),
	}
	writer.Write(row)
}

// generate random data
func generateRandomData(id int) CSVData {
	data := CSVData{
		ID:             id,
		Name:           randomName(),
		Country:        randomCountry(),
		Address:        randomAddress(),
		PhoneNumber:    randomPhoneNumber(),
		CurrentCompany: randomCompany(),
		Salary:         randomSalary(),
	}

	return data
}

// generate random name
func randomName() string {
	names := []string{
		"Yofan", "Niki", "Andara", "Yudha", "Ndara", "Jojo", "Jopan",
	}
	return names[rand.Intn(len(names))]
}

// generate random country
func randomCountry() string {
	countries := []string{
		"Indonesia", "Japan", "Singapore", "United States", "Germany",
	}
	return countries[rand.Intn(len(countries))]
}

// generate random address
func randomAddress() string {
	addresses := []string{
		"JL. Bagus Wakanda", "JL. Jelek Wakanda", "JL. Lumayan Wakanda",
	}
	return addresses[rand.Intn(len(addresses))]
}

// generate random phone number
func randomPhoneNumber() string {
	return fmt.Sprintf("+628%v", rand.Intn(100000000))
}

// generate random company
func randomCompany() string {
	companies := []string{
		"A", "B", "C", "D", "E", "F", "G", "H",
	}
	return fmt.Sprintf("Company %v", companies[rand.Intn(len(companies))])
}

// generate random salary
func randomSalary() float64 {
	return rand.Float64() * 1000000
}
