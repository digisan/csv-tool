package csvtool

import (
	"io"
	"os"

	"github.com/digisan/go-generics/str"
)

// ColAttr :
type ColAttr struct {
	Idx       int
	Header    string
	IsEmpty   bool
	IsUnique  bool
	HasNull   bool
	HasEmpty  bool
	AllFilled bool // no item is "null/NULL/nil" AND no empty item
}

// Column : header, items, err
func Column(r io.Reader, idx int) (hdr string, items []string, err error) {
	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	headers, _, err := Info(r)
	if err != nil {
		return "", nil, err
	}

	if idx >= len(headers) {
		return "", nil, fEf("idx(%d) is out of index range", idx)
	}

	return CsvReader(r, func(i, n int, headers, items []string) (ok bool, hdrline, row string) {
		return true, headers[idx], items[idx]
	}, true, true, nil)
}

// FileColumn : header, items, err
func FileColumn(path string, idx int) (hdr string, items []string, err error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return "", nil, err
	}
	defer csvFile.Close()
	return Column(csvFile, idx)
}

// GetColAttr :
func GetColAttr(r io.Reader, idx int) (*ColAttr, error) {
	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	hdr, items, err := Column(r, idx)
	if err != nil {
		return nil, err
	}

	ca := &ColAttr{
		Idx:       idx,
		Header:    hdr,
		IsEmpty:   len(items) == 0,
		IsUnique:  len(items) == len(str.MkSet(items...)),
		HasNull:   false,
		HasEmpty:  false,
		AllFilled: true,
	}
	for _, item := range items {
		switch trimBlank(item) {
		case "null", "nil", "NULL":
			ca.HasNull = true
		case "":
			ca.HasEmpty = true
		}
		if ca.HasNull && ca.HasEmpty {
			break
		}
	}
	ca.AllFilled = !ca.HasNull && !ca.HasEmpty
	return ca, nil
}

// FileColAttr :
func FileColAttr(csvpath string, idx int) (*ColAttr, error) {
	csvFile, err := os.Open(csvpath)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return nil, err
	}
	defer csvFile.Close()
	return GetColAttr(csvFile, idx)
}
