package csvtool

import (
	"fmt"
	"strings"

	fd "github.com/digisan/gotk/file-dir"
	lk "github.com/digisan/logkit"
)

var (
	sRepeat        = strings.Repeat
	sContains      = strings.Contains
	sReplaceAll    = strings.ReplaceAll
	sHasPrefix     = strings.HasPrefix
	sHasSuffix     = strings.HasSuffix
	sTrimSuffix    = strings.TrimSuffix
	sTrim          = strings.Trim
	sJoin          = strings.Join
	fSf            = fmt.Sprintf
	fEf            = fmt.Errorf
	warnOnErr      = lk.WarnOnErr
	failOnErr      = lk.FailOnErr
	failP1OnErr    = lk.FailP1OnErr
	failOnErrWhen  = lk.FailOnErrWhen
	mustCreateDir  = fd.MustCreateDir
	mustWriteFile  = fd.MustWriteFile
	mustAppendFile = fd.MustAppendFile
)

func isWrappedWith(s, prefix, suffix string) bool {
	return sHasPrefix(s, prefix) && sHasSuffix(s, suffix)
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
	return len(sTrim(s, " \t\n")) == 0
}

func trimBlank(s string) string {
	return sTrim(s, " \t\n")
}
