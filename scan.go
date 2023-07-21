package csvtool

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"

	. "github.com/digisan/go-generics/v2"
)

func ItemEsc(item string) string {
	if len(item) > 1 {
		hasComma, hasDQ, hasLF := sContains(item, ","), sContains(tryStripDQ(item), "\""), sContains(item, "\n")
		if hasDQ {
			item = sReplaceAll(item, "\"", "\"\"")
		}
		if hasComma || hasLF || hasDQ {
			item = tryWrapWithDQ(item)
		}
	}
	return item
}

// Info : headers, nItem, error
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

// CsvInfo : headers, nItem, error
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

func HeaderHasAll(r io.Reader, hdrItems ...string) (bool, error) {
	headers, _, err := Info(r)
	if err != nil {
		return false, err
	}
	for _, item := range hdrItems {
		if NotIn(item, headers...) {
			return false, nil
		}
	}
	return true, nil
}

func FileHeaderHasAll(path string, hdrItems ...string) (bool, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return false, err
	}
	defer csvFile.Close()
	return HeaderHasAll(csvFile, hdrItems...)
}

func HeaderHasAny(r io.Reader, hdrItems ...string) (bool, error) {
	headers, _, err := Info(r)
	if err != nil {
		return false, err
	}
	for _, item := range hdrItems {
		if In(item, headers...) {
			return true, nil
		}
	}
	return false, nil
}

func FileHeaderHasAny(path string, hdrItems ...string) (bool, error) {
	csvFile, err := os.Open(path)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return false, err
	}
	defer csvFile.Close()
	return HeaderHasAny(csvFile, hdrItems...)
}

// CsvReader : if [f arg: i==-1], it is pure HeaderRow csv
func CsvReader(
	r io.Reader,
	f func(i, n int, headers, items []string) (ok bool, hdr, row string),
	keepOriHdr bool,
	keepAnyRow bool,
	w io.Writer,
) (string, []string, error) {

	rs, ok := r.(io.ReadSeeker)
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	content, err := csv.NewReader(r).ReadAll()
	// failOnErr("%v", err)
	if err != nil {
		return "", nil, err
	}

	if len(content) == 0 {
		return "", []string{}, fEf("FILE_EMPTY")
	}

	hdrOnly := false
	if len(content) == 1 {
		hdrOnly = true
	}

	headers := make([]string, 0)
	for i, hdrItem := range content[0] {
		if hdrItem == "" {
			hdrItem = fSf("column_%d", i)
			warnOnErr("%v: - column[%d] is empty, mark [%s]", fEf("CSV_COLUMN_HEADER_EMPTY"), i, hdrItem)
		}
		headers = append(headers, ItemEsc(hdrItem)) // default is original headers
	}

	// Remove The Header Row //
	content = content[1:]

	N := len(content) // N is row's count
	hdrLine, allRows := "", []string{}

	// if f is NOT provided, we select all rows including headers //
	if f == nil {
		if len(content) > 0 || keepOriHdr {
			hdrLine = sJoin(headers, ",")
		}
		for _, d := range content {
			allRows = append(allRows, sJoin(d, ","))
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
		hdrLine = sJoin(headers, ",")
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
		data := []byte(sTrimSuffix(hdrLine+"\n"+sJoin(allRows, "\n"), "\n"))
		_, err = w.Write(data)
		failP1OnErr("%v", err)
	}

	return hdrLine, allRows, nil
}

// Scan : if [f arg: i==-1], it is pure HeaderRow csv
func Scan(in []byte, f func(i, n int, headers, items []string) (ok bool, hdr, row string), keepOriHdr bool, w io.Writer) (string, []string, error) {
	return CsvReader(bytes.NewReader(in), f, keepOriHdr, false, w)
}

// ScanFile :
func ScanFile(path string, f func(i, n int, headers, items []string) (ok bool, hdr, row string), keepOriHdr bool, outPath string) (string, []string, error) {

	fr, err := os.Open(path)
	failP1OnErr("csvPath: File is not found || wrong root : %v", err)
	defer fr.Close()

	var fw *os.File = nil

	if trimBlank(outPath) != "" {
		mustCreateDir(filepath.Dir(outPath))
		fw, err = os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE, 0666)
		failP1OnErr("outPath: File is not found || wrong root : %v", err)
		defer fw.Close()
	}

	hRow, rows, err := CsvReader(fr, f, keepOriHdr, false, fw)
	failOnErrWhen(rows == nil, "%v @ %s", err, outPath) // go internal csv func error
	return hRow, rows, err
}
