package csvtool

import (
	. "github.com/digisan/go-generics/v2"
)

// Create : create csv file with input headers
func Create(outcsv string, hdrNames ...string) (string, error) {
	if len(hdrNames) == 0 {
		return "", fEf("No Headers Provided")
	}

	headers := Map(hdrNames, func(i int, e string) string { return ItemEsc(e) })
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

	headersA, _, err := FileInfo(pathA)
	failOnErr("%v", err)
	failOnErrWhen(!SupEq(headersA, linkHeaders), "%v", fEf("headers of csv-A must have all link-headers"))

	headersB, _, err := FileInfo(pathB)
	failOnErr("%v", err)
	failOnErrWhen(!SupEq(headersB, linkHeaders), "%v", fEf("headers of csv-B must have all link-headers"))

	Create(outpath, Settify(Union(headersA, headersB)...)...)

	var (
		lkIndicesA = Map(linkHeaders, func(i int, e string) int { return IdxOf(e, headersA...) })
		lkIndicesB = Map(linkHeaders, func(i int, e string) int { return IdxOf(e, headersB...) })
		emptyComma = sRepeat(",", len(headersB)-len(linkHeaders))
		lkItemsGrp = [][]string{}
		mAiBr      = make(map[int]string)
	)

	_, rowsA, _ := ScanFile(
		pathA,
		func(i, n int, headers, items []string) (bool, string, string) {

			lkrItems := Map(lkIndicesA, func(i, e int) string { return items[e] })
			lkItemsGrp = append(lkItemsGrp, lkrItems)

			items4w := Map(items, func(i int, e string) string { return ItemEsc(e) })
			return true, "", sJoin(items4w, ",")
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
