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
	basename       string
	csvdir         string
	mtx            = &sync.Mutex{}
	schema         []string
	nSchema        int
	keepCatHdr     bool
	keepIgnCatHdr  bool
	outdir         string
	splitfiles     []string
	ignoredfiles   []string
	parallel       = false
	mustSingleProc = false
	fileIgnoredOut = ""
	strictSchema   = false
)

// KeepCatHeaders :
func KeepCatHeaders(keep bool) {
	keepCatHdr = keep
}

// KeepIgnCatHeaders :
func KeepIgnCatHeaders(keep bool) {
	keepIgnCatHdr = keep
}

// Dir4NotSplittable : in LooseMode, only take the last path seg for dump folder
func Dir4NotSplittable(dir string) {
	fileIgnoredOut = dir
}

// StrictSchema :
func StrictSchema(strict bool) {
	strictSchema = strict
}

// ForceNoParallel :
func ForceSingleProc(sp bool) {
	mustSingleProc = sp
}

// Split : return (splitfiles, ignoredfiles, error)
func Split(csvfile, outputdir string, categories ...string) ([]string, []string, error) {

	basename = filepath.Base(csvfile)
	csvdir = filepath.Dir(csvfile)

	schema = categories
	nSchema = len(schema)

	if outputdir == "" {
		outdir = "./" + strings.TrimSuffix(basename, ".csv") + "/"
	} else {
		outdir = strings.TrimSuffix(outputdir, "/") + "/"
	}

	in, err := os.ReadFile(csvfile)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csvfile)
	}

	// --------------- strict schema check --------------- //
	headers, nRow, err := ct.FileInfo(csvfile)
	if err != nil {
		return nil, nil, fmt.Errorf("%v @ %s", err, csvfile)
	}
	if strictSchema && len(fileIgnoredOut) > 0 {
		if !str.Superset(headers, schema) || nRow == 0 {

			nsCsvFile, _ := fd.RelPath(csvfile, false)
			nsCsvFile = filepath.Join(fileIgnoredOut, nsCsvFile)

			// relPath output likes '../***' is not working with filepath.Join
			// manually put nsCsvFile under fileIgnoredOut.
			if !strings.Contains(nsCsvFile, fileIgnoredOut+"/") {
				nsCsvFile = filepath.Join(fileIgnoredOut, nsCsvFile)
			}

			if keepIgnCatHdr {
				gio.MustWriteFile(nsCsvFile, in)
			} else {
				gio.MustCreateDir(filepath.Dir(nsCsvFile))
				fw, err := os.OpenFile(nsCsvFile, os.O_WRONLY|os.O_CREATE, 0666)
				lk.FailOnErr("%v @ %s", err, nsCsvFile)
				qry.Subset(in, false, schema, false, nil, fw)
				fw.Close()
			}

			return []string{}, []string{nsCsvFile}, nil
		}
	}

	// --------------- parallel set --------------- //
	parallel = false
	if !mustSingleProc && len(in) < 1024*1024*10 {
		parallel = true
	}
	// fmt.Printf("%s running on parallel? %v\n", csvfile, parallel)

	splitfiles = []string{}
	ignoredfiles = []string{}
	return splitfiles, ignoredfiles, split(0, in, outdir)
}

func split(rl int, in []byte, prevpath string, pCatItems ...string) error {

	if rl >= nSchema {
		return nil
	}

	cat := schema[rl]
	rl++

	rmHdrGrp := []string{cat}
	if keepCatHdr {
		rmHdrGrp = nil
	}

	_, rows, err := qry.Subset(in, true, []string{cat}, false, nil, nil)
	if err != nil {
		return err
	}

	// --------------- not splittable --------------- //
	// empty / empty content / missing needed categories
	if len(fileIgnoredOut) > 0 {
		if func() bool {
			mtx.Lock()
			defer mtx.Unlock()
			if len(rows) == 0 || (len(rows) > 0 && len(strings.Trim(rows[0], " \t")) == 0) {

				fileIgnoredOutInfo := fmt.Sprintf("%s(missing %s)", filepath.Base(fileIgnoredOut), cat)
				nsCsvDir, _ := fd.RelPath(csvdir, false)
				fileIgnoredInfo := fmt.Sprintf("%s(%s).csv", strings.TrimSuffix(basename, ".csv"), nsCsvDir)
				fileIgnoredInfo = strings.ReplaceAll(fileIgnoredInfo, "/", "~")
				nsCsvFile := filepath.Join(prevpath, fileIgnoredOutInfo, fileIgnoredInfo)

				if keepIgnCatHdr {
					gio.MustWriteFile(nsCsvFile, in)
				} else {
					gio.MustCreateDir(filepath.Dir(nsCsvFile))
					fw, err := os.OpenFile(nsCsvFile, os.O_WRONLY|os.O_CREATE, 0666)
					lk.FailOnErr("%v @ %s", err, nsCsvFile)
					qry.Subset(in, false, schema, false, nil, fw)
					fw.Close()
				}

				ignoredfiles = append(ignoredfiles, nsCsvFile)
				return true
			}
			return false
		}() {
			return nil
		}
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
				[]qry.Cond{{Hdr: cat, Val: catItem, ValTyp: "string", Rel: "="}},
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
					[]qry.Cond{{Hdr: cat, Val: catItem, ValTyp: "string", Rel: "="}},
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
