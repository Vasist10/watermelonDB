package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"io"
	"errors"
)

type DB struct {
	file *os.File
	index map[string]int64
}
func Open(path string)(*DB,error){
	f,err := os.OpenFile(path,os.O_RDWR|os.O_CREATE,0644)
	if err != nil{
		return nil, err
	}
	return &DB{file: f,
		index : make(map[string]int64)}, nil
}

func (db *DB) Close() error{
	return db.file.Close()
}

func (db *DB) Put(key string, value string) error{
	offset, err := db.file.Seek(0, io.SeekEnd)
	if err != nil{
		return err
	}
	var b bytes.Buffer

	KeyLen := int32(len(key))
	ValLen := int32(len(value))
	//&b → the writer (bytes.Buffer in our case).
	//binary.LittleEndian → says we want to write the integer in little-endian byte order.
	//Little-endian means the least significant byte goes first.
	if err := binary.Write(&b,binary.LittleEndian,KeyLen); err != nil{
		return err
	}
	if err := binary.Write(&b,binary.LittleEndian,ValLen); err != nil{
		return err
	}
	
	b.Write([]byte(key))
	b.Write([]byte(value))

	if _,err := db.file.Write(b.Bytes()); err != nil{
		return err
	}

	db.index[key] = offset

	return nil

}

var ErrKeyNotFound = errors.New("key not found")

func (db *DB) Get(key string)(string,error){
	offset, ok := db.index[key]
	if !ok{
		return "", ErrKeyNotFound
	}
	if _, err := db.file.Seek(offset, io.SeekStart); err != nil{
		return "", err
	}
	var KeyLen int32
	var ValLen int32
	//4 bytes from the file, interprets them as an int32, and stores the result in our KeyLen variable.
	if err := binary.Read(db.file,binary.LittleEndian,&KeyLen); err !=nil{ 
		return "", err
	}
	//does same for valen 
	if err := binary.Read(db.file,binary.LittleEndian,&ValLen); err !=nil{
		return "", err
	}
	//skips the key bytes cz we already know the key
	if _, err := db.file.Seek(int64(KeyLen), io.SeekCurrent); err != nil {
        return "", err
    }
	//decalre a byte slice to store our val
	valBytes := make([]byte, ValLen)
	//reads the val bytes from the file 
	if _, err := io.ReadFull(db.file,valBytes); err != nil{
		return "", err
	}
	return string(valBytes), nil

}