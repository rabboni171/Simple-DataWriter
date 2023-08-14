package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	done := make(chan bool)

	go func() {
		WriteDataToFile(done)
	}()

	success := <-done
	if !success {
		fmt.Println("Failed to write data to file.")
		return
	}

	InsertToDb()

	deleteSuccess := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		deleteSuccess = DeleteFile()
		wg.Done()
	}()

	wg.Wait()

	if deleteSuccess {
		fmt.Println("File deleted successfully!")
	} else {
		fmt.Println("Failed to delete file.")
	}

}

func InsertToDb() {
	db, err := sql.Open("postgres", "user=postgres password=admin dbname=article sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Printf("failed to close db : %v", err)
		}
	}(db)

	// Execute the COPY command
	_, err = db.Exec("COPY numbers (values) FROM 'D:\\data.csv' CSV")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Records inserted successfully!")
}

func generateRandomNumber(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func WriteDataToFile(done chan bool) {
	file, err := os.Create("D:\\data.csv")
	if err != nil {
		log.Printf("Failed to creat a file :%s", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
		}
	}(file)
	for i := 1; i <= 2000000; i++ {
		number := generateRandomNumber(100, 999)
		numberStr := strconv.Itoa(number)
		_, err := file.WriteString(numberStr + "\n")
		if err != nil {
			log.Printf("Error writing to file:%s", err)
			return
		}
	}
	fmt.Println("Writing to file is completed")

	done <- true
}

func DeleteFile() bool {
	time.Sleep(15 * time.Second) // Wait for 15 seconds before deleting the file

	err := os.Remove("D:\\data.csv")
	if err != nil {
		log.Printf("Failed to delete file: %s", err)
		return false
	}

	return true
}
