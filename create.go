package csvtool

import (
	"fmt"
	"strings"

	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
)

// Create : create csv file with input headers
func Create(csvOut string, hdrNames ...string) (string, error) {
	if len(hdrNames) == 0 {
		return "", fmt.Errorf("no headers provided")
	}

	headers := Map(hdrNames, func(i int, e string) string { return ItemEsc(e) })
	hdrRow := strings.Join(headers, ",")
	if csvOut != "" {
		fd.MustWriteFile(csvOut, []byte(hdrRow))
	}
	return hdrRow, nil
}

// Append : extend rows, append rows content to csv file
func Append(path string, validate bool, rows ...string) {
	if len(rows) > 0 {
		fd.MustAppendFile(path, []byte(strings.Join(rows, "\n")), true)
	}
	if validate {
		ScanFile(path, nil, true, "")
	}
}

// Combine : extend columns, linkHeaders combination must be UNIQUE in csvA & csvB
func Combine(pathA, pathB string, linkHeaders []string, onlyLinkedRow bool, outPath string) error {

	headersA, _, err := FileInfo(pathA)
	if err != nil {
		return err
	}
	if !SupEq(headersA, linkHeaders) {
		return fmt.Errorf("headers of csv-A must have all link-headers")
	}

	headersB, _, err := FileInfo(pathB)
	if err != nil {
		return err
	}
	if !SupEq(headersB, linkHeaders) {
		return fmt.Errorf("headers of csv-B must have all link-headers")
	}

	Create(outPath, Settify(Union(headersA, headersB)...)...)

	var (
		lkIndicesA = Map(linkHeaders, func(i int, e string) int { return IdxOf(e, headersA...) })
		lkIndicesB = Map(linkHeaders, func(i int, e string) int { return IdxOf(e, headersB...) })
		emptyComma = strings.Repeat(",", len(headersB)-len(linkHeaders))
		lkItemsGrp = [][]string{}
		mAiBr      = make(map[int]string)
	)

	_, rowsA, _ := ScanFile(
		pathA,
		func(i, n int, headers, items []string) (bool, string, string) {

			lkrItems := Map(lkIndicesA, func(i, e int) string { return items[e] })
			lkItemsGrp = append(lkItemsGrp, lkrItems)

			items4w := Map(items, func(i int, e string) string { return ItemEsc(e) })
			return true, "", strings.Join(items4w, ",")
		},
		false,
		"",
	)

	ScanFile(
		pathB,
		func(i, n int, headers, items []string) (bool, string, string) {
			for iAtRowA, lkrItems := range lkItemsGrp {
				if IsSuper(items, lkrItems) {
					items4w := FilterMap(items,
						func(i int, e string) bool { return NotIn(i, lkIndicesB...) },
						func(i int, e string) string { return ItemEsc(e) },
					)
					mAiBr[iAtRowA] = strings.Join(items4w, ",")
					return false, "", ""
				}
			}
			return false, "", ""
		},
		false,
		"",
	)

	rowsC := []string{}
	if onlyLinkedRow {
		for i, rA := range rowsA {
			if rB, ok := mAiBr[i]; ok {
				rowsC = append(rowsC, rA+","+rB)
			}
		}
	} else {
		for i, rA := range rowsA {
			if rB, ok := mAiBr[i]; ok {
				rowsC = append(rowsC, rA+","+rB)
			} else {
				rowsC = append(rowsC, rA+emptyComma)
			}
		}
	}

	Append(outPath, true, rowsC...)
	return nil
}
