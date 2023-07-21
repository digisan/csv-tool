package csvtool

import (
	"testing"
)

func TestCombine(t *testing.T) {
	type args struct {
		csvA            string
		csvB            string
		linkHeaders     []string
		onlyKeepLinkRow bool
		csvOut          string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvA:            "./data/Modules.csv",
				csvB:            "./data/Questions.csv",
				linkHeaders:     []string{"module_version_id"},
				onlyKeepLinkRow: true,
				csvOut:          "./out/combine.csv",
			},
		},
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvA:        "./data/Modules.csv",
		// 		csvB:        "./data/Questions.csv",
		// 		linkHeaders:     []string{"module_version_id"},
		// 		onlyKeepLinkRow: false,
		// 		csvOut:          "./out/combine1.csv",
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Combine(tt.args.csvA, tt.args.csvB, tt.args.linkHeaders, tt.args.onlyKeepLinkRow, tt.args.csvOut)
		})
	}
}
