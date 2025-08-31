package main 

import (
	"fmt"
	"log"
)

func main(){
	fmt.Print("starting watermelonDB")
	db, err := Open("watermelondata")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	

	err = db.Put("name", "watermelonDB")
	if err != nil {
		log.Fatalf("Failed to put key: %v", err)
	}
	fileInfo, err := db.file.Stat()
	if err != nil {
		panic(err)
	}
	fmt.Printf("File size according to Go: %d bytes\n", fileInfo.Size())//gives me 4096 even though in my pc prop it shows 8kb

	err = db.Put("hello", "namaste")
	if err != nil {
		log.Fatalf("Failed to put key: %v", err)
	}
	value, err := db.Get("name")
	if err != nil {
		log.Fatalf("Failed to get key: %v", err)
	}
	fmt.Println(value)
	defer db.Close()

}