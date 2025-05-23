package main

// "vsystems.ca/scomm"
// "vsystems.ca/scomm/scomm"
// "github.com/valrusu/scomm/scomm"
// "github.com/valrusu/scomm/scomm"

import (
	"flag"
	"fmt"
	// "log"
	"os"

	"github.com/valrusu/scomm"
)

func main() {

	var (
		verbose                                        bool
		headerLines, batchSize                         int
		keyParam, payloadParam, delimiter, outputDelim string
		noCommon, noOld, noNew                         bool
	)

	// get all parameters; parse them here and pass parsed slices to scomm?
	// flag.Usage = func() {
	// 	fmt.Fprintf(flag.CommandLine.Output(), "\ntvldiff excludes identical lines from 2 TVL files and produces 2 output files,\n"+
	// 		"one which contains the new and changed tags and one with the old tags that were deleted\n")
	// 	flag.PrintDefaults()
	// }

	flag.BoolVar(&verbose, "v", false, "bool; verbose mode")
	flag.IntVar(&headerLines, "H", 0, "int; number header lines, which will be skipped")
	flag.StringVar(&keyParam, "k", "", "key field definition; without -d use fixed length fields (like cut -c), with -d use a field,list (like cut -f)")
	// flag.StringVar(&tagParam, "t", "5-14", "tag field definition; without -d use a fixed length fields, with -d use a field,list")
	flag.StringVar(&payloadParam, "p", "", "payload parameter not used currently")
	flag.StringVar(&delimiter, "d", "", "use delimited mode for KEY and PAYLOAD values, without it use fixed length fields")
	flag.IntVar(&batchSize, "b", 0, "batch size for reading input files")
	flag.StringVar(&outputDelim, "D", "", "delimiter for output in case 2 or more outputs go the same file descriptor")
	flag.BoolVar(&noCommon, "c", false, "discard common lines, otherwise output them on file descriptor 7 if specified, or stdout if not specified")
	flag.BoolVar(&noOld, "o", false, "discard lines only in the old file, otherwise output them on file descriptor 6 if specified, or stdout if not specified")
	flag.BoolVar(&noNew, "n", false, "discard lines only in the new file, otherwise output them on file descriptor 5 if specified, or stdout if not specified")
	flag.Parse()

	// params:
	// verbose: extra log output on stderr
	// skipLines: number of lines to skip from each input
	// keyParam: string defining the key, can be a list
	// PayloadParam: string defining the key payloads, can be a list - not sure this is used and how NOT USED
	// delimiter: empty for position-based, char (or string?) for separated fields
	// batchSize: 0 forces full mode; why? scomm should run by default in batch mode with a default batch size
	// delimiterOut: line to separate output in case it goes to the same writer

	// IDEA: allow user to specify which output goes where? like -old fd6 -new fd5   or  -old stdout -new stdout
	// if 2+ get the same writer, then I need delimiterOut; if -old/-new/-common not specified, then discard

	// the tool is meant to DO something, I should not have to enable things, more to disable
	// so by default it should output as much as possible, and have options to disable stuff

	// INPUT logic:
	// FD3 good: use for OLD			FD3 bad: use stdin
	// FD4 good: use for NEW			FD4 bad: use stdin
	// both OLD and NEW from stdin: error

	// OUTPUT logic:
	// FD7 good: use for COMMON			FD7 bad: !-common: use stdout for COMMON		FD7 bad: -common: discard COMMON
	// FD5 good: use for NEW			FD5 bad: !-new: use stdout for NEW				FD5 bad: -new: discard NEW
	// FD6 good: use for OLD			FD6 bad: !-old: use stdout for OLD				FD6 bad: -old: discard OLD
	// at least 2 on stdout: -delimiterOut: ok			at least 2 on stdout: !-delimiterOut: error
	// -old = do not output OLD			-new = do not output NEW			-common = do not output COMMON

	// COMPARE logic:
	// read OLD in mapOld(full line)
	// read all NEW lines:
	//		matches OLD: 	yes: print or discard; delete from mapOld; add to mapNew
	//						no: KEY specified: save key value in mapNewKeys (I could exclude more later)
	// KEY specified: will further compare OLD and NEW based on KEY existence (lines were different due to other fields)
	//		go through all OLD, get KEY field(s)
	//		found in mapNewKeys? 	yes: delete from mapOld (same old key exists in NEW => data got updated)
	// NEW on stdout and COMMON was stdout: print delimiterOut
	// print mapNew
	// OLD on stdout and ( COMMON was stdout or NEW was stdout ): print delimiterOut
	// print MapOld

	// TODO I should have:
	// inputFileSep = separator for when both inputs are coming in on the same FD or stdin
	// inputFieldSep = fields separator
	// outputFileSep = separator for when 2+ outputs are going out on the same FD or stdout

	if err := scomm.Scomm(
		true,                // verbose bool,
		1,                   //skipLines int,
		"1,2",               // keyParam string,
		"",                  // payloadParam string, -- not used yet
		",",                 // delimiter string,
		0,                   // batchSize int,
		"xxxXXXxxx",         // delimiterOut string
		false, false, false, // discard old new common
	); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
