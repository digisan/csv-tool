package split2

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ct "github.com/digisan/csv-tool"
	qry "github.com/digisan/csv-tool/query"
	"github.com/digisan/go-generics/str"
	fd "github.com/digisan/gotk/filedir"
	gio "github.com/digisan/gotk/io"
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
func StrictSchema(strict bool, ignOut string) {
	strictSchema = strict
	if ignOut != "" {
		ignoredOut = ignOut
	}
}

// Split : return (split-files, ignored-files, error)
func Split(csv, out string, categories ...string) ([]string, []string, error) {

	name, dir := filepath.Base(csv), filepath.Dir(csv)
	schema, nSchema := categories, len(categories)

	fmt.Sprintln("---", name, dir, schema, nSchema)

	outdir := ""
	if out == "" {
		outdir = "./" + strings.TrimSuffix(name, ".csv") + "/"
	} else {
		outdir = strings.TrimSuffix(out, "/") + "/"
	}

	if !fd.DirExists(outdir) {
		gio.MustCreateDir(outdir)
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
		if !str.Superset(headers, schema) || nRow == 0 {

			ignOut := ignoredOut
			absIgnOut, err := fd.AbsPath(ignOut, false)
			lk.FailOnErr("%v", err)
			if absIgnOut != ignOut {
				ignOut = filepath.Join(outdir, ignOut) // if [ignOut] is rel-path, put it under 'out'
			}

			nsCsv, _ := fd.RelPath(csv, false)
			nsCsv = filepath.Join(ignOut, nsCsv)

			// relPath output likes '../***' is not working with filepath.Join
			// manually put nsCsv under ignOut.
			if !strings.Contains(nsCsv, ignOut+"/") {
				nsCsv = filepath.Join(ignOut, nsCsv)
			}

			if rmSchemaColInIgn {

				gio.MustCreateDir(filepath.Dir(nsCsv))
				fw, err := os.OpenFile(nsCsv, os.O_WRONLY|os.O_CREATE, 0666)
				lk.FailOnErr("%v @ %s", err, nsCsv)
				defer fw.Close()

				_, _, err = qry.Subset(inData, false, schema, false, nil, fw)
				lk.FailOnErr("Subset %v @ %s", err, nsCsv)
				// fmt.Println(header, len(header))

			} else {
				gio.MustWriteFile(nsCsv, inData)
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
		idx := str.IdxOf(hdr, headers...)
		h, items, err := ct.Column(in, idx)
		lk.FailOnErr("%v", err)

		items = str.MkSet(items...)
		fmt.Sprintln(" --", h, items)

		if len(lvlDir) > 0 {
			for _, dir := range lvlDir {
				for _, item := range items {
					lvlDir = append(lvlDir, filepath.Join(dir, item))
				}
			}
		} else {
			for _, item := range items {
				lvlDir = append(lvlDir, filepath.Join(outdir, item))
			}
		}
	}

	// fmt.Println(lvlDir, len(lvlDir))
	// fmt.Println(" ---------- ")

	// remove partial paths
	lvlDir = str.FM(lvlDir, func(i int, e string) bool {
		return len(fd.AncestorList(e)) == len(schema)+len(fd.AncestorList(outdir))
	}, nil)

	// fmt.Println(lvlDir, len(lvlDir))
	fmt.Sprintln(" ---------- ")

	// create structure folders & header only empty file
	hdrByte := []byte(strings.Join(headers, ","))
	if rmSchemaCol {
		hdrByte = []byte(strings.Join(str.Minus(headers, schema), ","))
	}

	splitfiles := []string{}
	for _, dir := range lvlDir {
		toCsv := filepath.Join(dir, name)
		splitfiles = append(splitfiles, toCsv)
		gio.MustWriteFile(toCsv, hdrByte)
	}

	// fetch line by line
	iSchema := []int{}
	for _, s := range schema {
		iSchema = append(iSchema, str.IdxOf(s, headers...))
	}
	ct.Scan(
		inData,
		func(i, n int, headers, items []string) (ok bool, hdrline string, row string) {

			schemaVal := []string{}
			for _, iSch := range iSchema {
				schemaVal = append(schemaVal, items[iSch])
			}

			toCsv := outdir
			for _, sv := range schemaVal {
				toCsv = filepath.Join(toCsv, sv)
			}
			toCsv = filepath.Join(toCsv, name)

			// fmt.Println(toCsv)

			if rmSchemaCol {
				for _, iSch := range iSchema {
					str.DelEleOrderlyAt(&items, iSch)
				}
			}

			line := []byte(strings.Join(items, ","))
			gio.MustAppendFile(toCsv, line, true)

			return true, "", ""
		},
		true,
		nil,
	)

	splitfiles = str.MkSet(splitfiles...)
	return splitfiles, nil, nil
}
