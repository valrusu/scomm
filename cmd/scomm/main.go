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
		verbose                           bool
		headerLines, batchSize            int
		keyParam, payloadParam, delimiter string
		noCommon, noOld, noNew            bool
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
	flag.BoolVar(&noCommon, "c", false, "discard common lines, otherwise output them on file descriptor 7 if specified, or stdout if not specified")
	flag.BoolVar(&noOld, "o", false, "discard lines only in the old file, otherwise output them on file descriptor 6 if specified, or stdout if not specified")
	flag.BoolVar(&noNew, "n", false, "discard lines only in the new file, otherwise output them on file descriptor 5 if specified, or stdout if not specified")
	flag.Parse()

	// params:
	// -v verbose: extra log output on stderr
	// -H skipLines: number of lines to skip from each input
	// -k keyParam: string defining the key, can be a list
	// -p PayloadParam: string defining the key payloads, can be a list - not sure this is used and how NOT USED
	// -d delimiter: empty for position-based, char (or string?) for separated fields
	// -b batchSize: 0 forces full mode; why? scomm should run by default in batch mode with a default batch size
	// -x with -k/-p display the FILE1 lines with same key in FILE2 but payload different on FD8; ignore without -k/-p
	// -5 discard output on corresponding FD
	// -6
	// -7
	// -8

	if err := scomm.Scomm(
		true,                              // verbose bool,
		1,                                 // skipLines int,
		"1,2",                             // keyParam string,
		"",                                // payloadParam string, -- not used yet
		",",                               // delimiter string,
		0,                                 // batchSize int,
		true,                              // extra output from FILE1 on FD8
		false, false, false, false, false, // discard old new common
	); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
