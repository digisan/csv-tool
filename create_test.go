package csvtool

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func f2w(w io.Writer) {
	fmt.Println(w == nil)
	fmt.Println(isInterfaceNil(w))
}

func TestMisc(t *testing.T) {
	var p *os.File = nil
	f2w(p)
	fmt.Println("------------------")
	p, _ = os.Open("./data/data.csv")
	f2w(p)
}

func TestCombine(t *testing.T) {
	type args struct {
		csvfileA        string
		csvfileB        string
		linkHeaders     []string
		onlyKeepLinkRow bool
		outcsv          string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvfileA:        "./data/Modules.csv",
				csvfileB:        "./data/Questions.csv",
				linkHeaders:     []string{"module_version_id"},
				onlyKeepLinkRow: true,
				outcsv:          "./out/combine.csv",
			},
		},
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvfileA:        "./data/Modules.csv",
		// 		csvfileB:        "./data/Questions.csv",
		// 		linkHeaders:     []string{"module_version_id"},
		// 		onlyKeepLinkRow: false,
		// 		outcsv:          "./out/combine1.csv",
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Combine(tt.args.csvfileA, tt.args.csvfileB, tt.args.linkHeaders, tt.args.onlyKeepLinkRow, tt.args.outcsv)
		})
	}
}
