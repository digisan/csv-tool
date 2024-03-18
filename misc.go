package csvtool

import (
	"strings"
)

func isWrappedWith(s, prefix, suffix string) bool {
	return strings.HasPrefix(s, prefix) && strings.HasSuffix(s, suffix)
}

func isWrappedWithSQ(s string) bool {
	return isWrappedWith(s, "'", "'")
}

func isWrappedWithDQ(s string) bool {
	return isWrappedWith(s, "\"", "\"")
}

func tryWrap(s, prefix, suffix string) string {
	if !isWrappedWith(s, prefix, suffix) {
		return prefix + s + suffix
	}
	return s
}

func tryWrapWithSQ(s string) string {
	return tryWrap(s, "'", "'")
}

func tryWrapWithDQ(s string) string {
	return tryWrap(s, "\"", "\"")
}

////////////////////////////////////////////////

func tryStrip(s string, n int) string {
	if n < 0 {
		panic("[n] must >= 0")
	}
	if len(s) >= 2*n {
		return s[n : len(s)-n]
	}
	return s
}

func tryStripSQ(s string) string {
	if isWrappedWithSQ(s) {
		return tryStrip(s, 1)
	}
	return s
}

func tryStripDQ(s string) string {
	if isWrappedWithDQ(s) {
		return tryStrip(s, 1)
	}
	return s
}

////////////////////////////////////////////////

func isBlank(s string) bool {
	return len(strings.Trim(s, " \t\n")) == 0
}

func trimBlank(s string) string {
	return strings.Trim(s, " \t\n")
}
