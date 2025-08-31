package main

import (
	//"bytes"
	"encoding/binary"
	"os"
	"io"
	"errors"
)
const PageSize = 4096

const (
	numRecordOffset = 0
	freeSpaceOffset = 2
	pageHeaderSize = 4

)

type Page struct {
	id 	int64
	data [PageSize]byte
	dirty bool //if pg is modified dirty is set to true and when we wb to disk we only weite the pages where this flag is set to 1
}

type RecordLocation struct {
	PageID int64
	PageOffset uint16
}
//index map string to RecordLocation:
// Key                 Value (RecordLocation)
// 
// "name"       { PageID: 0, PageOffset: 4  }
// "hello"    { PageID: 0, PageOffset: 30 }
// "smthng"   { PageID: 1, PageOffset: 4  }

type DB struct {
	file *os.File
	index map[string]RecordLocation
	totalPages int64
	freelist []int64
 }

func Open(path string)(*DB,error){
	f,err := os.OpenFile(path,os.O_RDWR|os.O_CREATE,0644)
	if err != nil{
		return nil, err
	}
	fileInfo, err := f.Stat()
	if err != nil{
		f.Close()
		return nil, err
	}
	totalPages := fileInfo.Size() / PageSize
	
	return &DB{
		file: f,
		index : make(map[string]RecordLocation),
		totalPages: totalPages,
		freelist: make([]int64,0),
		}, nil
}

func (db *DB) Close() error{
	return db.file.Close()
}

func (p *Page) numRecords() uint16{
	return binary.LittleEndian.Uint16(p.data[numRecordOffset:])// littleendinan says to our comp that do not convert to text/eng keep it in binary itself i want to use in binary format only
}

func (p *Page) setNumRecords(n uint16){//write the records to the page header basically
	binary.LittleEndian.PutUint16(p.data[numRecordOffset:], n)
	p.dirty = true
}
func (p *Page) freeSpaceOffset() uint16{ // offset where the free space starts
	return binary.LittleEndian.Uint16(p.data[freeSpaceOffset:])
}
func (p *Page) setFreeSpaceOffset(n uint16){//updating the frree space offset in the pg header
	binary.LittleEndian.PutUint16(p.data[freeSpaceOffset:], n)
	p.dirty = true
}
func (p *Page) addRecordtoPage(key string, value string) (uint16, error){
	keyLen, valLen := uint32(len(key)), uint32(len(value))
	recordSize := uint16(8+len(key)+len(value))
	recordOffset := p.freeSpaceOffset()
	amountFreeSpace := PageSize - recordOffset
	currentRecords := p.numRecords()
	if amountFreeSpace < recordSize {
		return 0, errors.New("not enough space in page")
	}

	binary.LittleEndian.PutUint32(p.data[recordOffset:], keyLen)
	binary.LittleEndian.PutUint32(p.data[recordOffset+4:], valLen)

	
	keyStart := recordOffset + 8
	copy(p.data[keyStart:],key)
	valStart := keyStart + uint16(len(key))
	copy(p.data[valStart:], value)

	p.setNumRecords(currentRecords + 1)
	p.setFreeSpaceOffset(recordOffset + recordSize)

	return recordOffset,nil 
}
func readPage(file *os.File, pageID int64)(*Page,error){
	pg:= &Page{id:pageID}
	_,err := file.Seek(pageID*PageSize, io.SeekStart)
	if err != nil{
		return nil,err
	}
	_,err = io.ReadFull(file,pg.data[:]);
	if err !=nil{
		return nil, err
	}
	return pg,nil

}
func writePage(file *os.File, pg *Page) error{
	if !pg.dirty{
		return nil
	}
	offset := pg.id * PageSize
	
	_,err := file.Seek(offset,io.SeekStart)
	if err != nil{
		return err
	}
	_,err = file.Write(pg.data[:])
	if  err != nil{
		return err
	}
	pg.dirty = false
	return nil

}
func (db *DB) Put(key string, value string) error{
	recordSize:= 8 + len(key) + len(value)
	if recordSize > PageSize - pageHeaderSize{
		return errors.New("record size exceeds page size")
	}
	var targetPage *Page 
	pageID := int64(-1)
	
	if len(db.freelist) > 0{
		pageID = db.freelist[len(db.freelist)-1]
		db.freelist = db.freelist[:len(db.freelist)-1]
		pg,err := readPage(db.file,pageID)
		if err != nil{
			return err
		}
		if PageSize - int(pg.freeSpaceOffset())>=recordSize{
			targetPage = pg
		}
	}
	if targetPage == nil{
		pageID = db.totalPages
		db.totalPages++
		targetPage = &Page{id:pageID}
		targetPage.setFreeSpaceOffset(pageHeaderSize)
		targetPage.setNumRecords(0)
	}
	recordOffset, err := targetPage.addRecordtoPage(key,value)
	if err != nil{
		return err
	}
	db.index[key] = RecordLocation{
		PageID: pageID,
		PageOffset: recordOffset,
	}
	err = writePage(db.file,targetPage)
	if err != nil{
		return err
	}
	if PageSize - int(targetPage.freeSpaceOffset()) < 8 + len(key) + len(value){
		db.freelist = append(db.freelist, pageID)
	}
	return nil
}

var ErrKeyNotFound = errors.New("key not found")

func (db *DB) Get(key string)(string,error){
	loc, ok := db.index[key]
	if !ok{
		return "", ErrKeyNotFound
	}
	pg,err := readPage(db.file, loc.PageID)
	if err != nil{
		return "", err
	}
	recordOffset := loc.PageOffset
	keyLen := binary.LittleEndian.Uint32(pg.data[recordOffset:])
	valLen := binary.LittleEndian.Uint32(pg.data[recordOffset+4:])

	valueStart := recordOffset + 8 + uint16(keyLen)
	valueEnd := valueStart + uint16(valLen)
	value := string(pg.data[valueStart:valueEnd])
	return value, nil
	
}