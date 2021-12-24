package csvtool

import (
	"github.com/digisan/go-generics/i64"
	"github.com/digisan/go-generics/i64s"
	"github.com/digisan/go-generics/si64"
	"github.com/digisan/go-generics/str"
)

// Create : create csv file with input headers
func Create(outcsv string, hdrNames ...string) (string, error) {
	if len(hdrNames) == 0 {
		return "", fEf("No Headers Provided")
	}

	headers := str.FM(hdrNames, nil, func(i int, e string) string { return ItemEsc(e) })
	hdrRow := sJoin(headers, ",")
	if outcsv != "" {
		mustWriteFile(outcsv, []byte(hdrRow))
	}
	return hdrRow, nil
}

// Append : extend rows, append rows content to csv file
func Append(path string, validate bool, rows ...string) {
	if len(rows) > 0 {
		mustAppendFile(path, []byte(sJoin(rows, "\n")), true)
	}
	if validate {
		ScanFile(path, nil, true, "")
	}
}

// Combine : extend columns, linkHeaders combination must be UNIQUE in csvfileA & csvfileB
func Combine(pathA, pathB string, linkHeaders []string, onlyLinkedRow bool, outpath string) {

	headersA, _, err := CsvInfo(pathA)
	failOnErr("%v", err)
	failOnErrWhen(!str.SuperEq(headersA, linkHeaders), "%v", fEf("headers of csv-A must have all link-headers"))

	headersB, _, err := CsvInfo(pathB)
	failOnErr("%v", err)
	failOnErrWhen(!str.SuperEq(headersB, linkHeaders), "%v", fEf("headers of csv-B must have all link-headers"))

	Create(outpath, str.MkSet(str.Union(headersA, headersB)...)...)

	var (
		lkIndicesA = si64.FM(linkHeaders, nil, func(i int, e string) int { return str.IdxOf(e, headersA...) })
		lkIndicesB = si64.FM(linkHeaders, nil, func(i int, e string) int { return str.IdxOf(e, headersB...) })
		emptyComma = sRepeat(",", len(headersB)-len(linkHeaders))
		lkItemsGrp = [][]string{}
		mAiBr      = make(map[int]string)
	)

	_, rowsA, _ := ScanFile(
		pathA,
		func(i, n int, headers, items []string) (bool, string, string) {

			lkrItems := i64s.FM(lkIndicesA, nil, func(i, e int) string { return items[e] })
			lkItemsGrp = append(lkItemsGrp, lkrItems)

			items4w := str.FM(items, nil, func(i int, e string) string { return ItemEsc(e) })
			return true, "", sJoin(items4w, ",")
		},
		false,
		"",
	)

	ScanFile(
		pathB,
		func(i, n int, headers, items []string) (bool, string, string) {
			for iAtRowA, lkrItems := range lkItemsGrp {
				if str.Superset(items, lkrItems) {
					items4w := str.FM(items,
						func(i int, e string) bool { return i64.NotIn(i, lkIndicesB...) },
						func(i int, e string) string { return ItemEsc(e) },
					)
					mAiBr[iAtRowA] = sJoin(items4w, ",")
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

	Append(outpath, true, rowsC...)
}
