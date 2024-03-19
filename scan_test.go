package csvtool

import (
	"fmt"
	"testing"
)

func TestInfo(t *testing.T) {
	fmt.Println(FileInfo("./data/data.csv"))	
}
