package csvtool

import (
	"fmt"
	"testing"
)

func TestWrap(t *testing.T) {
	fmt.Println(tryWrapWithDQ("a"))
	fmt.Println(tryWrapWithDQ(`"a"`))
}

func TestStrip(t *testing.T) {
	fmt.Println(tryStripDQ(`"a"`))
	fmt.Println(tryStripDQ(`'"a"`))
}
