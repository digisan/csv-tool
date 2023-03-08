package split

import (
	"fmt"
	"testing"

	fd "github.com/digisan/gotk/file-dir"
)

func TestSplit(t *testing.T) {
	type args struct {
		csvfile    string
		outdir     string
		categories []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvfile:    "./data/qldStudent.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults0.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults111.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults110.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults101.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "/home/qingmiao/Desktop/nrt-issue/csv-tool/data/sub/itemResults100.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvfile:    "./data/data.csv",
		// 		outdir:     "outmedium",
		// 		categories: []string{"School", "Domain", "YrLevel"},
		// 	},
		// 	want:    []string{},
		// 	wantErr: false,
		// },
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvfile:    "./data/big/itemResults.csv",
		// 		outdir:     "outbig",
		// 		categories: []string{"School", "Domain", "YrLevel"},
		// 	},
		// 	want:    []string{},
		// 	wantErr: false,
		// },
	}

	ForceSglProc(true)
	StrictSchema(true, "")
	RmSchemaCol(true)
	RmSchemaColInIgn(true)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			splitfiles, ignoredfiles, _ := Split(tt.args.csvfile, tt.args.outdir, tt.args.categories...)
			fmt.Println(len(splitfiles))
			fmt.Println(len(ignoredfiles))
		})
	}

	fmt.Println(fd.WalkFileDir("out", true))
	fmt.Println(fd.WalkFileDir("outmedium", true))
	fmt.Println(fd.WalkFileDir("outbig", true))
}
