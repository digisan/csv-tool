package query

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	csvtool "github.com/digisan/csv-tool"
	"github.com/digisan/gotk"
	fd "github.com/digisan/gotk/filedir"
	gio "github.com/digisan/gotk/io"
	"github.com/digisan/gotk/iter"
	lk "github.com/digisan/logkit"
)

func TestSubset(t *testing.T) {

	defer gotk.TrackTime(time.Now())
	lk.Log2F(true, "./TestSubset.log")

	dir := "../data/"
	files, err := os.ReadDir(dir)
	lk.FailOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
		if !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "itemResults1.csv" {
		// 	continue
		// }

		func() {

			fmt.Println(fName)
			_, n, _ := csvtool.FileInfo(fName)

			in, err := os.ReadFile(fName)
			lk.FailOnErr("%v", err)

			out := "subset-out/"
			gio.MustCreateDir(out)
			file4w, err := os.OpenFile(filepath.Join(out, file.Name()), os.O_WRONLY|os.O_CREATE, 0644)
			lk.FailOnErr("%v", err)
			defer file4w.Close()
			Subset(
				in,
				false,
				[]string{"Domain", "Item Response", "YrLevel", "School", "Age", "substrand_id"},
				true,
				iter.Iter2Slc(n-1, -1),
				file4w,
			)

			out1 := "subset-out1/"
			gio.MustCreateDir(out1)
			file4w1, err := os.OpenFile(filepath.Join(out1, file.Name()), os.O_WRONLY|os.O_CREATE, 0644)
			lk.FailOnErr("%v", err)
			defer file4w1.Close()
			Subset(
				in,
				true,
				[]string{"School", "Domain", "YrLevel", "XXX", "Test Name", "Test level", "Test Domain", "Test Item RefID", "Item Response"},
				true,
				iter.Iter2Slc(0, 20000),
				file4w1,
			)

		}()
	}
}

func TestSelect(t *testing.T) {

	defer gotk.TrackTime(time.Now())
	lk.Log2F(true, "./TestSelect.log")

	dir := "../data/"
	files, err := os.ReadDir(dir)
	lk.FailOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
		if !strings.HasSuffix(fName, ".csv") {
			continue
		}

		fmt.Println(fName)

		func() {

			in, err := os.ReadFile(fName)
			lk.FailOnErr("%v", err)

			gio.MustWriteFile("out/"+file.Name(), []byte{})
			file4w, err := os.OpenFile("out/"+file.Name(), os.O_WRONLY|os.O_CREATE, 0666)
			lk.FailOnErr("%v", err)
			defer file4w.Close()

			Select(in, '&', []Cond{
				{Hdr: "School", Val: "21221", ValTyp: "string", Rel: "="},
				{Hdr: "Domain", Val: "Spelling", ValTyp: "string", Rel: "="},
				{Hdr: "YrLevel", Val: 3, ValTyp: "int", Rel: "<="},
			}, file4w)

		}()
	}
}

func TestQuery(t *testing.T) {

	defer gotk.TrackTime(time.Now())
	lk.Log2F(true, "./TestQuery.log")

	dir := "../data"
	files, err := os.ReadDir(dir)
	lk.FailOnErr("%v", err)

	n := len(files)
	fmt.Println(n, "files")

	wg := &sync.WaitGroup{}
	wg.Add(n)

	for _, file := range files {

		go func(filename string) {
			defer wg.Done()

			if !strings.HasSuffix(filename, ".csv") {
				return
			}

			fName := filepath.Join(dir, filename)
			fmt.Println(fName)

			QueryFile(
				fName,
				true,
				[]string{
					"Domain",
					"School",
					"YrLevel",
					"Test Name",
					"Test level",
					"Test Domain",
					"Test Item RefID",
				},
				'&',
				[]Cond{
					{Hdr: "School", Val: "21221", ValTyp: "string", Rel: "="},
					{Hdr: "YrLevel", Val: 5, ValTyp: "uint", Rel: ">"},
					{Hdr: "Domain", Val: "Reading", ValTyp: "string", Rel: "!="},
				},
				"out/"+filename,
			)

		}(file.Name())
	}

	wg.Wait()

	fmt.Println(fd.WalkFileDir(dir, true))
	fmt.Println(fd.WalkFileDir("out/", true))
}

func TestQueryAtConfig(t *testing.T) {
	n, err := QueryAtConfig("./query.toml")
	lk.FailOnErr("%v", err)
	fmt.Println(n)
}

func TestUnique(t *testing.T) {
	type args struct {
		csvpath string
		outcsv  string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := Unique(tt.args.csvpath, tt.args.outcsv)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unique() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Unique() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Unique() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
