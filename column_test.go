package csvtool

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestFileColumn(t *testing.T) {
	type args struct {
		csvpath string
		idx     int
	}
	tests := []struct {
		name      string
		args      args
		wantHdr   string
		wantItems []string
		wantErr   bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvpath: "./data/data.csv",
				idx:     1,
			},
			wantHdr:   `Id,"Name,Name1",Age,"Na,me"`,
			wantItems: []string{`Ahmad,Ahmad`, "Hello", `Test1`, `Test2`, `[""abc]`},
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHdr, gotItems, err := FileColumn(tt.args.csvpath, tt.args.idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHdr != tt.wantHdr {
				t.Errorf("FileColumn() gotHdr = %v, want %v", gotHdr, tt.wantHdr)
			}
			if !reflect.DeepEqual(gotItems, tt.wantItems) {
				t.Errorf("FileColumn() gotItems = %v, want %v", gotItems, tt.wantItems)
			}
		})
	}
}

func TestFileColAttr(t *testing.T) {
	type args struct {
		csvpath string
		idx     int
	}
	tests := []struct {
		name    string
		args    args
		want    *ColAttr
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvpath: "./data/itemResults999.csv",
				idx:     10,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvpath: "./data/Substrands.csv",
				idx:     0,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spew.Dump(FileColAttr(tt.args.csvpath, tt.args.idx))
		})
	}
}
