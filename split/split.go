package split

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	ct "github.com/digisan/csv-tool"
	qry "github.com/digisan/csv-tool/query"
	"github.com/digisan/go-generics/str"
	fd "github.com/digisan/gotk/filedir"
	gio "github.com/digisan/gotk/io"
	lk "github.com/digisan/logkit"
)

var (
	basename         string
	csvdir           string
	mtx              = &sync.Mutex{}
	schema           []string
	nSchema          int
	rmSchemaCol      bool
	rmSchemaColInIgn bool
	outdir           string
	splitfiles       []string
	ignoredfiles     []string
	parallel         = false
	sglProc          = false
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

// ForceSglProc :
func ForceSglProc(sp bool) {
	sglProc = sp
}

// Split : return (splitfiles, ignoredfiles, error)
func Split(csv, out string, categories ...string) ([]string, []string, error) {

	basename, csvdir = filepath.Base(csv), filepath.Dir(csv)
	schema, nSchema = categories, len(categories)

	if out == "" {
		outdir = "./" + strings.TrimSuffix(basename, ".csv") + "/"
	} else {
		outdir = strings.TrimSuffix(out, "/") + "/"
	}

	if !fd.DirExists(outdir) {
		gio.MustCreateDir(outdir)
	}

	in, err := os.ReadFile(csv)
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
				qry.Subset(in, false, schema, false, nil, fw)
				fw.Close()
			} else {
				gio.MustWriteFile(nsCsv, in)
			}

			return []string{}, []string{nsCsv}, nil
		}
	}

	// --------------- parallel set --------------- //
	parallel = false
	if !sglProc && len(in) < 1024*1024*10 {
		parallel = true
	}
	// fmt.Printf("%s running on parallel? %v\n", csvfile, parallel)

	splitfiles, ignoredfiles = []string{}, []string{}
	return splitfiles, ignoredfiles, split(0, in, outdir)
}

func split(rl int, in []byte, prevpath string, pCatItems ...string) error {

	if rl >= nSchema {
		return nil
	}

	cat := schema[rl]
	rl++

	rmHdrGrp := []string{cat}
	if !rmSchemaCol {
		rmHdrGrp = nil
	}

	_, rows, err := qry.Subset(in, true, []string{cat}, false, nil, nil)
	if err != nil {
		return err
	}

	// --------------- not splittable --------------- //
	// empty / empty content / missing needed categories
	if func() bool {
		mtx.Lock()
		defer mtx.Unlock()
		if len(rows) == 0 || (len(rows) > 0 && len(strings.Trim(rows[0], " \t")) == 0) {

			ignoredOutInfo := fmt.Sprintf("%s(missing %s)", filepath.Base(ignoredOut), cat)
			nsCsvDir, _ := fd.RelPath(csvdir, false)
			ignoredInfo := fmt.Sprintf("%s(%s).csv", strings.TrimSuffix(basename, ".csv"), nsCsvDir)
			ignoredInfo = strings.ReplaceAll(ignoredInfo, "/", "~")
			nsCsv := filepath.Join(prevpath, ignoredOutInfo, ignoredInfo)

			if rmSchemaColInIgn {
				gio.MustCreateDir(filepath.Dir(nsCsv))
				fw, err := os.OpenFile(nsCsv, os.O_WRONLY|os.O_CREATE, 0666)
				lk.FailOnErr("%v @ %s", err, nsCsv)
				qry.Subset(in, false, schema, false, nil, fw)
				fw.Close()
			} else {
				gio.MustWriteFile(nsCsv, in)
			}

			ignoredfiles = append(ignoredfiles, nsCsv)
			return true
		}
		return false
	}() {
		return nil
	}

	// --------------- end --------------- //

	unirows := str.MkSet(rows...)
	unirows = str.FM(unirows, func(i int, e string) bool { return len(strings.Trim(e, " \t")) > 0 }, nil)

	// Safe Mode, But Slow //
	if !parallel {

		for _, catItem := range unirows {

			outcsv := outdir
			for _, pcItem := range pCatItems {
				outcsv += pcItem + "/"
			}
			outcsv += catItem + "/" + basename

			wBuf := &bytes.Buffer{}

			qry.Query(
				in,
				false,
				rmHdrGrp,
				'&',
				[]qry.Cond{{Hdr: cat, Val: catItem, Rel: "="}},
				io.Writer(wBuf),
			)

			if rl == nSchema {
				gio.MustWriteFile(outcsv, wBuf.Bytes())
				splitfiles = append(splitfiles, outcsv)
			}

			split(rl, wBuf.Bytes(), filepath.Dir(outcsv), append(pCatItems, catItem)...)
		}
	}

	// Whole Linux Exhausted When Running On Big Data //
	if parallel {

		wg := &sync.WaitGroup{}
		wg.Add(len(unirows))

		for _, catItem := range unirows {

			go func(catItem string) {
				defer wg.Done()

				outcsv := outdir
				for _, pcItem := range pCatItems {
					outcsv += pcItem + "/"
				}
				outcsv += catItem + "/" + basename

				wBuf := &bytes.Buffer{}

				qry.Query(
					in,
					false,
					rmHdrGrp,
					'&',
					[]qry.Cond{{Hdr: cat, Val: catItem, Rel: "="}},
					io.Writer(wBuf),
				)

				if rl == nSchema {
					mtx.Lock()
					gio.MustWriteFile(outcsv, wBuf.Bytes())
					splitfiles = append(splitfiles, outcsv)
					mtx.Unlock()
				}

				split(rl, wBuf.Bytes(), filepath.Dir(outcsv), append(pCatItems, catItem)...)

			}(catItem)
		}

		wg.Wait()
	}

	return nil
}
