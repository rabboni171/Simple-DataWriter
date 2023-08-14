package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"

	_ "github.com/lib/pq"
)

func main() {
	done := make(chan bool)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- false // Send failure response to the channel
			}
		}()

		WriteDataToFile(done)
	}()

	success := <-done
	if success {
		InsertToDb()
	} else {
		fmt.Println("Failed to write data to file.")
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
