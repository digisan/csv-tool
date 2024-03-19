package csvtool

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
	lk "github.com/digisan/logkit"
)

func CellEsc(cell string) string {
	if len(cell) > 1 {
		hasComma, hasDQ, hasLF := strings.Contains(cell, ","), strings.Contains(tryStripDQ(cell), "\""), strings.Contains(cell, "\n")
		if hasDQ {
			cell = strings.ReplaceAll(cell, "\"", "\"\"")
		}
		if hasComma || hasLF || hasDQ {
			cell = tryWrapWithDQ(cell)
		}
	}
	return cell
}

// Info : headers, nRow, error
func Info(r io.Reader) ([]string, int, error) {
	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, -1, err
	}

	if len(content) == 0 {
		return []string{}, 0, nil
	}
	return content[0], len(content) - 1, nil
}

// CsvInfo : headers, nRow, error
func FileInfo(path string) ([]string, int, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return nil, 0, err
	}
	defer csvFile.Close()
	return Info(csvFile)
}

func HeaderHasAll(r io.Reader, hdr ...string) (bool, error) {
	headers, _, err := Info(r)
	if err != nil {
		return false, err
	}
	for _, h := range hdr {
		if NotIn(h, headers...) {
			return false, nil
		}
	}
	return true, nil
}

func FileHeaderHasAll(path string, hdr ...string) (bool, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return false, err
	}
	defer csvFile.Close()
	return HeaderHasAll(csvFile, hdr...)
}

func HeaderHasAny(r io.Reader, hdrs ...string) (bool, error) {
	headers, _, err := Info(r)
	if err != nil {
		return false, err
	}
	for _, hdr := range hdrs {
		if In(hdr, headers...) {
			return true, nil
		}
	}
	return false, nil
}

func FileHeaderHasAny(path string, hdr ...string) (bool, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return false, err
	}
	defer csvFile.Close()
	return HeaderHasAny(csvFile, hdr...)
}

// CsvReader : if [f arg: i==-1], it is pure HeaderRow csv
func CsvReader(
	r io.Reader,
	f func(i, n int, headers, cells []string) (ok bool, hdr, row string),
	keepOriHdr bool,
	keepAnyRow bool,
	w io.Writer,
) (string, []string, error) {

	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return "", nil, err
	}

	if len(content) == 0 {
		return "", []string{}, fmt.Errorf("FILE_EMPTY")
	}

	hdrOnly := false
	if len(content) == 1 {
		hdrOnly = true
	}

	headers := make([]string, 0)
	for i, hdrCell := range content[0] {
		if hdrCell == "" {
			hdrCell = fmt.Sprintf("column_%d", i)
			lk.WarnOnErr("%v: - column[%d] is empty, mark [%s]", fmt.Errorf("CSV_COLUMN_HEADER_EMPTY"), i, hdrCell)
		}
		headers = append(headers, CellEsc(hdrCell)) // default is original headers
	}

	// Remove The Header Row //
	content = content[1:]

	N := len(content) // N is row's count
	hdrLine, allRows := "", []string{}

	// if f is NOT provided, we select all rows including headers //
	if f == nil {
		if len(content) > 0 || keepOriHdr {
			hdrLine = strings.Join(headers, ",")
		}
		for _, d := range content {
			allRows = append(allRows, strings.Join(d, ","))
		}
		goto SAVE
	}

	if hdrOnly {
		if keepOriHdr {
			hdrLine = strings.Join(headers, ",")
		}
		goto SAVE
	}

	// default hdrLine is original header-line
	if len(content) > 0 || keepOriHdr {
		hdrLine = strings.Join(headers, ",")
	}

	for i, d := range content {
		if ok, hdr, row := f(i, N, headers, d); ok {
			if hdr != "" {
				hdrLine = hdr
			}
			if keepAnyRow {
				allRows = append(allRows, row)
			} else {
				if !isBlank(row) { // we use f to return row content for deciding wether to add this row
					allRows = append(allRows, row)
				}
			}
		}
	}

SAVE:
	// save
	if !IsNil(w) {
		data := []byte(strings.TrimSuffix(hdrLine+"\n"+strings.Join(allRows, "\n"), "\n"))
		_, err = w.Write(data)
		if err != nil {
			return "", nil, err
		}
	}
	return hdrLine, allRows, nil
}

// Scan : if [f arg: i==-1], it is pure HeaderRow csv
func Scan(in []byte, f func(i, n int, headers, cells []string) (ok bool, hdr, row string), keepOriHdr bool, w io.Writer) (string, []string, error) {
	return CsvReader(bytes.NewReader(in), f, keepOriHdr, false, w)
}

// ScanFile :
func ScanFile(path string, f func(i, n int, headers, cells []string) (ok bool, hdr, row string), keepOriHdr bool, outPath string) (string, []string, error) {

	fr, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer fr.Close()

	var fw *os.File = nil

	if trimBlank(outPath) != "" {
		fd.MustCreateDir(filepath.Dir(outPath))
		fw, err = os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return "", nil, err
		}
		defer fw.Close()
	}

	hRow, rows, err := CsvReader(fr, f, keepOriHdr, false, fw)
	if rows == nil && err != nil { // go internal csv func error
		return "", nil, err
	}
	return hRow, rows, err
}
