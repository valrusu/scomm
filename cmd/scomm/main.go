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

	// INPUT:
	// order of the files matter in the output; file1 is considered the "old" one and file2 the "new" one
	// FILE1 from FD3
	// FILE2 from DF4

	// OUTPUT: without -k/-p                     with -k/-p
	// if an FD is not "good", do not output that data; can I check this correctly for output going to another process???
	// FD5: lines unique to FILE1                FILE1 lines for which key(file1) does not exist in FILE2
	// FD6: lines unique to FILE2
	// FD7: lines common                         lines common
	//                                           FILE1 lines for which key(file1) exists in FILE2 but payloads are different

	// without k/p:
	// load all file1 into lines1
	// read lines from file2 as line2:
	// 	line2 in lines1 : common, output line2 (or line1) on FD7
	// 	                  delete from lines1
	// 	else : save line2 in lines2
	// output all lines1 on FD5
	// output all lines2 on FD6

	// with k/p: treat the inputs as this is the "only" info I care about in a line; but I need original line output :(
	// load all file1 into lines1
	// read lines from file2 as line2:
	// 	line2 in lines1 : common, output line2 (or line1) on FD7
	// 	                  delete from lines1
	// 	else : save line2 in lines2
	// 	       save key2 in keys2
	// for all lines1 remaining:
	// 	key1 in keys2, payload2=payload1 : common2, output line2 on FD7 (or another one?)
	// 	                                   delete line1 from lines1
	// 	key1 in keys2, payload2<>payload1 : updated, output line2 on FD6
	// 	    								delete line1 from lines1
	// 	key1 not in keys2 : deleted, output line1 on FD5
	// anything left???

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
