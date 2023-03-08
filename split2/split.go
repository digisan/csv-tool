package split2

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ct "github.com/digisan/csv-tool"
	qry "github.com/digisan/csv-tool/query"
	. "github.com/digisan/go-generics/v2"
	fd "github.com/digisan/gotk/file-dir"
	lk "github.com/digisan/logkit"
)

var (
	rmSchemaCol      bool
	rmSchemaColInIgn bool
	ignoredOut       = "ignored"
	strictSchema     = false
)

// RmSchemaCol :
func RmSchemaCol(rmSchema bool) {
	rmSchemaCol = rmSchema
}

// RmSchemaColInIgn :
func RmSchemaColInIgn(rmSchema bool) {
	rmSchemaColInIgn = rmSchema
}

// StrictSchema :
func StrictSchema(strict bool, ignrOut string) {
	strictSchema = strict
	if ignrOut != "" {
		ignoredOut = ignrOut
	}
}

// Split : return (split-files, ignored-files, error)
func Split(csv, out string, categories ...string) ([]string, []string, error) {

	name, dir := filepath.Base(csv), filepath.Dir(csv)
	schema, nSchema := categories, len(categories)

	fmt.Sprintln("---", name, dir, schema, nSchema)

	outDir := ""
	if out == "" {
		outDir = "./" + strings.TrimSuffix(name, ".csv") + "/"
	} else {
		outDir = strings.TrimSuffix(out, "/") + "/"
	}

	if !fd.DirExists(outDir) {
		fd.MustCreateDir(outDir)
	}

	inData, err := os.ReadFile(csv)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csv)
	}

	// --------------- strict schema check --------------- //
	headers, nRow, err := ct.FileInfo(csv)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csv)
	}
	if strictSchema {
		if !IsSuper(headers, schema) || nRow == 0 {

			nsCsv := filepath.Clean(filepath.Join(out, ignoredOut, name))

			if rmSchemaColInIgn {

				fd.MustCreateDir(filepath.Dir(nsCsv))
				fw, err := os.OpenFile(nsCsv, os.O_WRONLY|os.O_CREATE, 0666)
				lk.FailOnErr("%v @ %s", err, nsCsv)
				defer fw.Close()

				_, _, err = qry.Subset(inData, false, schema, false, nil, fw)
				lk.FailOnErr("Subset %v @ %s", err, nsCsv)
				// fmt.Println(header, len(header))

			} else {
				fd.MustWriteFile(nsCsv, inData)
			}

			return []string{}, []string{nsCsv}, nil
		}
	}

	// --------------- split --------------- //

	in, err := os.Open(csv)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csv)
	}

	var lvlDir []string

	for _, hdr := range schema {
		idx := IdxOf(hdr, headers...)
		h, items, err := ct.Column(in, idx)
		lk.FailOnErr("%v", err)

		items = Settify(items...)
		fmt.Sprintln(" --", h, items)

		if len(lvlDir) > 0 {
			for _, dir := range lvlDir {
				for _, item := range items {
					lvlDir = append(lvlDir, filepath.Join(dir, item))
				}
			}
		} else {
			for _, item := range items {
				lvlDir = append(lvlDir, filepath.Join(outDir, item))
			}
		}
	}

	// fmt.Println(lvlDir, len(lvlDir))
	// fmt.Println(" ---------- ")

	// remove partial paths
	FilterFast(&lvlDir, func(i int, e string) bool { return len(fd.AncestorList(e)) == len(schema)+len(fd.AncestorList(outDir)) })

	// fmt.Println(lvlDir, len(lvlDir))
	fmt.Sprintln(" ---------- ")

	// create structure folders & header only empty file
	hdrByte := []byte(strings.Join(headers, ","))
	if rmSchemaCol {
		hdrByte = []byte(strings.Join(Minus(headers, schema), ","))
	}

	splitFiles := []string{}
	for _, dir := range lvlDir {
		toCsv := filepath.Join(dir, name)
		splitFiles = append(splitFiles, toCsv)
		fd.MustWriteFile(toCsv, hdrByte)
	}

	// fetch line by line
	iSchema := []int{}
	for _, s := range schema {
		iSchema = append(iSchema, IdxOf(s, headers...))
	}
	ct.Scan(
		inData,
		func(i, n int, headers, items []string) (ok bool, hdrline string, row string) {

			schemaVal := []string{}
			for _, iSch := range iSchema {
				schemaVal = append(schemaVal, items[iSch])
			}

			toCsv := outDir
			for _, sv := range schemaVal {
				toCsv = filepath.Join(toCsv, sv)
			}
			toCsv = filepath.Join(toCsv, name)

			// fmt.Println(toCsv)

			if rmSchemaCol {
				for _, iSch := range iSchema {
					DelEleOrderlyAt(&items, iSch)
				}
			}

			line := []byte(strings.Join(items, ","))
			fd.MustAppendFile(toCsv, line, true)

			return true, "", ""
		},
		true,
		nil,
	)

	splitFiles = Settify(splitFiles...)
	return splitFiles, nil, nil
}
