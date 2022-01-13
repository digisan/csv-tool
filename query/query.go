package query

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	ct "github.com/digisan/csv-tool"
	"github.com/digisan/go-generics/i32"
	"github.com/digisan/go-generics/i64"
	"github.com/digisan/go-generics/si64"
	"github.com/digisan/go-generics/str"
	gtk "github.com/digisan/gotk"
	fd "github.com/digisan/gotk/filedir"
	gio "github.com/digisan/gotk/io"
	lk "github.com/digisan/logkit"
)

// GetRepeated : remove repeated items
func GetRepeated(csv, out string, f func(rRepCnt int) bool) (string, []string, error) {
	_, _, mHashCnt, err := Unique(csv, "")
	if err != nil {
		return "", nil, err
	}
	return ct.ScanFile(csv,
		func(i, n int, headers, items []string) (ok bool, hdrline string, row string) {
			md5s := str.FM(items, nil, func(i int, e string) string {
				return fmt.Sprint(md5.Sum([]byte(e)))
			})
			rowhash := strings.Join(md5s, ",")
			headers4w := str.FM(headers, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			items4w := str.FM(items, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			return f(mHashCnt[rowhash]), strings.Join(headers4w, ","), strings.Join(items4w, ",")
		},
		true,
		out,
	)
}

// Unique : remove repeated items
func Unique(csv, out string) (string, []string, map[string]int, error) {
	// check out csv file is valid
	defer func() {
		if out != "" {
			ct.ScanFile(out, nil, true, "")
		}
	}()

	mHashCnt := make(map[string]int)
	h, rs, err := ct.ScanFile(
		csv,
		func(idx, cnt int, headers, items []string) (bool, string, string) {
			md5s := str.FM(items, nil, func(i int, e string) string {
				return fmt.Sprint(md5.Sum([]byte(e)))
			})
			rowhash := strings.Join(md5s, ",")
			_, ok := mHashCnt[rowhash]
			defer func() { mHashCnt[rowhash]++ }()

			if ok {
				return false, "", ""
			}

			headers4w := str.FM(headers, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			items4w := str.FM(items, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			return !ok, strings.Join(headers4w, ","), strings.Join(items4w, ",")
		},
		true,
		out,
	)
	return h, rs, mHashCnt, err
}

// Subset : content iRow start from 0. i.e. 1st content row index is 0
func Subset(in []byte, incCol bool, hdrNames []string, incRow bool, iRows []int, w io.Writer) (string, []string, error) {

	fnRow := i64.NotIn
	if incRow {
		fnRow = i64.In
	}

	cIndices, hdrRow := []int{}, ""
	fast, min, max := gtk.IsContInts(iRows)

	return ct.Scan(in, func(idx, cnt int, headers, items []string) (bool, string, string) {

		// get [hdrRow], [cIndices] once
		if hdrRow == "" {
			// select needed columns, cIndices is qualified header's original index in file headers
			var hdrRt []string
			if incCol {
				cIndices = si64.FM(hdrNames,
					func(i int, e string) bool { return str.In(e, headers...) },
					func(i int, e string) int { return str.IdxOf(e, headers...) },
				)
				hdrRt = str.Reorder(headers, cIndices) // Reorder has filter
				hdrRt = str.FM(hdrRt, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			} else {
				cIndices = si64.FM(headers,
					func(i int, e string) bool { return str.NotIn(e, hdrNames...) },
					func(i int, e string) int { return i },
				)
				hdrRt = str.FM(headers,
					func(i int, e string) bool { return i64.In(i, cIndices...) },
					func(i int, e string) string { return ct.ItemEsc(e) },
				)
			}
			hdrRow = strings.Join(hdrRt, ",")
		}

		ok := false
		if fast {
			if (incRow && idx >= min && idx <= max) || (!incRow && (idx < min || idx > max)) {
				ok = true
			}
		} else {
			if fnRow(idx, iRows...) {
				ok = true
			}
		}

		if ok {
			// filter column items
			var itemsRt []string
			if incCol {
				itemsRt = str.Reorder(items, cIndices)
				itemsRt = str.FM(itemsRt, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			} else {
				itemsRt = str.FM(items,
					func(i int, e string) bool { return i64.In(i, cIndices...) },
					func(i int, e string) string { return ct.ItemEsc(e) },
				)
			}

			return true, hdrRow, strings.Join(itemsRt, ",")
		}

		return true, hdrRow, "" // still "ok" as hdrRow is needed even if empty content

	}, !incCol, w)
}

// Cond :
type Cond struct {
	Hdr    string
	Val    interface{}
	ValTyp string
	Rel    string
}

// Select : R : [&, |]; condition relation : [=, !=, >, <, >=, <=]
// [=, !=] only apply to string comparasion, [>, <, >=, <=] apply to number comparasion
func Select(in []byte, R rune, CGrp []Cond, w io.Writer) (string, []string, error) {

	lk.FailP1OnErrWhen(i32.NotIn(R, '&', '|'), "%v", fmt.Errorf("[R] can only be [&, |]"))
	nCGrp := len(CGrp)

	return ct.Scan(in, func(idx, cnt int, headers, items []string) (bool, string, string) {

		hdrNames := str.FM(headers, nil, func(i int, e string) string { return ct.ItemEsc(e) })
		hdrRow := strings.Join(hdrNames, ",")

		if len(items) == 0 {
			return true, hdrRow, ""
		}

		CResults := []interface{}{}

	NEXTCONDITION:
		for _, C := range CGrp {

			if R == '|' && len(CResults) > 0 {
				break NEXTCONDITION
			}

			if I := str.IdxOf(C.Hdr, headers...); I != -1 {
				iVal := items[I]

				if C.Rel == "=" {
					if iVal == C.Val {
						CResults = append(CResults, struct{}{})
					}
					continue NEXTCONDITION
				}
				if C.Rel == "!=" {
					if iVal != C.Val {
						CResults = append(CResults, struct{}{})
					}
					continue NEXTCONDITION
				}

				switch C.ValTyp {
				case "int", "int8", "int16", "int32", "int64":
					var cValue int64
					if i64Val, ok := C.Val.(int64); ok {
						cValue = i64Val
					} else if intVal, ok := C.Val.(int); ok {
						cValue = int64(intVal)
					}

					iValue, err := strconv.ParseInt(iVal, 10, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "uint", "uint8", "uint16", "uint32", "uint64":
					var cValue uint64
					if i64Val, ok := C.Val.(int64); ok {
						cValue = uint64(i64Val)
					} else if intVal, ok := C.Val.(int); ok {
						cValue = uint64(intVal)
					}

					iValue, err := strconv.ParseUint(iVal, 10, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "float32", "float64", "float", "double":
					cValue := C.Val.(float64)
					iValue, err := strconv.ParseFloat(iVal, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				default:
					panic("comparable type [" + C.ValTyp + "] is not supported")
				}
			}
		}

		ok := false

		// Has conditions
		if len(CGrp) > 0 {
			if len(CResults) == 0 {
				return true, hdrRow, ""
			}
			if (R == '&' && len(CResults) == nCGrp) || (R == '|' && len(CResults) > 0) {
				ok = true
			}
		}

		// No conditions OR condition ok
		if ok || len(CGrp) == 0 {
			itemValues := str.FM(items, nil, func(i int, e string) string { return ct.ItemEsc(e) })
			return true, hdrRow, strings.Join(itemValues, ",")
		}

		return true, hdrRow, ""

	}, true, w)
}

// Query : combine Subset(incCol, all rows) & Select
func Query(in []byte, incCol bool, hdrNames []string, R rune, CGrp []Cond, w io.Writer) (string, []string, error) {

	b := &bytes.Buffer{}
	_, _, err := Select(in, R, CGrp, io.Writer(b))
	if err == nil {
		return Subset(b.Bytes(), incCol, hdrNames, false, []int{}, w)
	}
	return "", nil, err

}

func QueryFile(csv string, incCol bool, hdrNames []string, R rune, CGrp []Cond, out string) error {

	// fPf("---querying...<%s>\n", csv)

	if !fd.FileExists(csv) {
		return fmt.Errorf("[%s] does NOT exist, ignore", csv)
	}

	if csv == out {
		out += ".csv"
		defer func() {
			os.Remove(csv)
			os.Rename(out, csv)
		}()
	}

	in, err := os.ReadFile(csv)
	lk.FailP1OnErr("%v", err)

	gio.MustCreateDir(filepath.Dir(out))

	fw, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE, 0666)
	lk.FailP1OnErr("%v", err)
	defer fw.Close()

	_, _, err = Query(in, incCol, hdrNames, R, CGrp, fw)
	return err
}

// QueryAtConfig :
func QueryAtConfig(tomlPath string) (int, error) {

	config := &Config{}
	if _, err := toml.DecodeFile(tomlPath, config); err != nil {
		return 0, err
	}
	// failOnErr("%v", err)

	for _, qry := range config.Query {

		cond := []Cond{}

		for _, c := range qry.Cond {
			cond = append(cond, Cond{Hdr: c.Header, Val: c.Value, ValTyp: c.ValueType, Rel: c.RelaOfItemValue})
		}

		fmt.Println("Processing ... " + qry.Name)

		QueryFile(
			qry.CsvPath,
			qry.IncCol,
			qry.HdrNames,
			rune(qry.RelaOfCond[0]),
			cond,
			qry.OutCsv,
		)
	}

	return len(config.Query), nil
}
