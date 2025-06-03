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
		verbose                               bool
		headerLines, batchSize                int
		keyParam, payloadParam, delimiter     string
		noCommon, noFile1, noFile2, fullLines bool
		outModeMerge                          bool
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
	flag.BoolVar(&outModeMerge, "m", true, "extra info from FILE1 for data matching FILE2")
	flag.BoolVar(&noFile1, "1", false, "discard lines only in FILE1 , otherwise output them on file descriptor 6")
	flag.BoolVar(&noFile2, "2", false, "discard lines only in FILE2 , otherwise output them on file descriptor 5")
	flag.BoolVar(&noCommon, "3", false, "discard common lines, otherwise output them on file descriptor 7 if specified")
	flag.BoolVar(&fullLines, "f", false, "If -k/-pa are used, then output full lines, otherwise just the KEY/PAYLOAD fields")
	flag.Parse()

	if err := scomm.Scomm(
		verbose,                    // verbose bool,
		headerLines,                // skipLines int,
		keyParam,                   // keyParam string,
		payloadParam,               // payloadParam string, -- not used yet
		delimiter,                  // delimiter string,
		batchSize,                  // batchSize int,
		outModeMerge,               // false: generate merge+delete; true: generate delete+insert
		fullLines,                  // full lines output
		noCommon, noFile1, noFile2, // discard 5 6 7
	); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
