package main

// "vsystems.ca/scomm"
// "vsystems.ca/scomm/scomm"
// "github.com/valrusu/scomm/scomm"
// "github.com/valrusu/scomm/scomm"

import (
	"log"

	"github.com/valrusu/scomm"
)

func main() {
	log.Println("cmd scomm main")

	// get all parameters; parse them here and pass parsed slices to scomm?
	// flag.Usage = func() {
	// 	fmt.Fprintf(flag.CommandLine.Output(), "\ntvldiff excludes identical lines from 2 TVL files and produces 2 output files,\n"+
	// 		"one which contains the new and changed tags and one with the old tags that were deleted\n")
	// 	flag.PrintDefaults()
	// }
	
	// flag.BoolVar(&verbose, "v", false, "bool; verbose mode")
	// flag.BoolVar(&headerline, "H", true, "bool; header line; set if the files have a header line, which will be skipped")
	// flag.StringVar(&agencyParam, "a", "1-4", "agency field definition; without -d use a fixed length fields, with -d use a field,list")
	// flag.StringVar(&tagParam, "t", "5-14", "tag field definition; without -d use a fixed length fields, with -d use a field,list")
	// flag.StringVar(&payloadParam, "p", "15-25", "tag's other fields than a and t; without -d use a fixed length fields, with -d use a field,list; optional")
	// flag.StringVar(&delimiter, "d", "", "use delimited mode for a, t and p values, otherwise use fixed length fields")
	// flag.IntVar(&batchSize, "b", 0, "batch size for reading input files; it affects the comparins algorithm TODO write doc")
	// flag.StringVar(&testParam, "test", "", "internal") // TODO write proper go test
	// flag.Parse()
	
	// set package values or pass as parameters
	if err:=scomm.Scomm("1"); err !=nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
