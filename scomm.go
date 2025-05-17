package scomm

import "log"

var (
	Headerline, Profile, Verbose                   bool
	BatchSize                                      int
	AgencyParam, TagParam, PayloadParam, Delimiter string
	AgencyPos                                      [2]int
	TagPos, KeyPos,PayloadPos                             [][2]int

	cntLinesOld, cntLinesNew, cntSameLines, cntNewLines, updatedTags int
	linesOld                                                         map[string]struct{}
	linesNew                                                         map[string]struct{}
	newTagsList                                                      map[string]struct{}
)

func dbg(params ...interface{}) {
	if verbose {
		log.Println(params...)
	}
}

func getSimpleField(line string, pos [2]int) string {
	if delimiter == "" {
		var x, y int
		if pos[0] == 0 {
			x = 0
		} else {
			x = pos[0] - 1
		}
		if pos[1] == 0 {
			y = len(line)
		} else {
			y = pos[1]
		}
		if y > len(line) {
			log.Println("invalid data: " + line)
			os.Exit(1)
		}
		return line[x:y]
	}
	ss := strings.Split(line, delimiter)
	if pos[0] > len(ss) {
		log.Println("invalid data: " + line)
		os.Exit(1)
		// return ""
	}
	return ss[pos[0]-1]
}

func getCompoundField(line string, pos [][2]int) string {
	var s string

	if delimiter == "" {
		for _, v := range pos {
			var x, y int
			if v[0] == 0 {
				x = 0
			} else {
				x = v[0] - 1
			}
			if v[1] == 0 {
				y = len(line)
			} else {
				y = v[1]
			}
			if y > len(line) {
				log.Println("invalid data: " + line)
				os.Exit(1)
			}
			s += line[x:y]
		}
		return s
	}

	ss := strings.Split(line, delimiter)
	for _, v := range pos {
		if v[0] == v[1] { // single field
			if v[0] > len(ss) {
				log.Println("invalid data: " + line)
				os.Exit(1)
			}
			s += ss[v[0]-1] + delimiter
		} else { // interval field like 3-7
			if v[0] == 0 {
				v[0] = 1
			}
			if v[1] == 0 {
				v[1] = len(ss)
			}
			for w := v[0]; w <= v[1]; w++ {
				if w > len(ss) {
					log.Println("invalid data: " + line)
					os.Exit(1)
				}
				s += ss[w-1] + delimiter
			}
		}
	}
	// take out the last delimiter
	return strings.TrimRight(s, delimiter)
}

// getTag returns the tag serial number part of the input line
func getAgency(line string) string {
	return getSimpleField(line, agencyPos)
}

func getTag(line string) string {
	// return getSimpleField(line, tagPos)
	return getCompoundField(line, tagPos)
}

// getKey returns the agency and tag serial number parts of the input line (the key in a key-value map)
func getKey(line string) string {
	if delimiter == "" {
		return getAgency(line) + getTag(line)
	}
	return strings.Trim(getAgency(line)+delimiter+getTag(line), delimiter)
}

// getPayload returns the fields other then the key fields in the input line, likely including the tag status
func getPayload(line string) string {
	return getCompoundField(line, payloadPos)
}

// getTagLine returns all the tag related fields from the input line (TVL files may have extra info we do not use)
func getTagLine(line string) string {
	if delimiter == "" {
		return getKey(line) + getPayload(line)
	}
	return strings.Trim(getKey(line)+delimiter+getPayload(line), delimiter)
}

// parseListItem parses an input int or int-int interval into an array [2]int
func parseListItem(s string) (error, [2]int) {
	var ret [2]int

	if s == "" {
		return errors.New("option requires range argument"), ret
	}

	if strings.Contains(s, "-") {

		ss := strings.Split(s, "-")

		if len(ss) > 2 {
			return errors.New("invalid range " + s), ret
		}

		if ss[0] == "" {
			ret[0] = 0 // interval like "-3" which means "1-3"
		} else {
			i, err := strconv.Atoi(ss[0])
			if err != nil {
				return err, ret // TODO use fmt.Errorf
			}
			ret[0] = i
		}

		if ss[1] == "" {
			ret[1] = 0 // interval like "4-" which means "4-end of string"
			return nil, ret
		}

		i, err := strconv.Atoi(ss[1])
		if err != nil {
			return err, ret
		}
		if ret[0] != 0 && ret[0] > i {
			return errors.New("reverted interval " + s), ret
		}
		ret[1] = i
		return nil, ret
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return err, ret // TODO use fmt.Errorf
	}

	if delimiter != "" && i <= 0 {
		return errors.New("field is invalid " + s), ret
	}

	ret[0], ret[1] = i, i
	return nil, ret
}

// parseTagParams parses the agency, tag and payload parameters, which define the TVL structure; some projects may have extra data in the TVL file
func parseTagParams() error {
	var err error

	if delimiter != "" && strings.Contains(agencyParam, "-") {
		return errors.New("cannot have list of fields: " + agencyParam)
	}

	err, agencyPos = parseListItem(agencyParam)
	if err != nil {
		return err
	}

	// if delimiter != "" && strings.Contains(tagParam, "-") {
	// return errors.New("cannot have list of fields: " + tagParam)
	// }

	// err, tagPos = parseListItem(tagParam)
	// if err != nil {
	// 	return err
	// }

	ss := strings.Split(tagParam, ",")
	for _, v := range ss {
		err, y := parseListItem(v)
		if err != nil {
			return err
		}
		tagPos = append(tagPos, y)
	}

	ss = strings.Split(payloadParam, ",")
	for _, v := range ss {
		err, y := parseListItem(v)
		if err != nil {
			return err
		}
		payloadPos = append(payloadPos, y)
	}

	if verbose {
		log.Println("headerline", headerline)
		log.Println("agency", agencyParam, agencyPos)
		log.Println("tag", tagParam, tagPos)
		log.Println("payload", payloadParam, payloadPos)
	}

	return nil
}

log.SetFlags(log.Ldate | log.Ltime)

// if !verbose {
// 	f, err := os.Open(os.DevNull)
// 	if err != nil {
// 		log.Println(err)
// 		os.Exit(1)
// 	}
// 	log.SetOutput(f)
// }

flag.Usage = func() {
	fmt.Fprintf(flag.CommandLine.Output(), "\ntvldiff excludes identical lines from 2 TVL files and produces 2 output files,\n"+
		"one which contains the new and changed tags and one with the old tags that were deleted\n")
	flag.PrintDefaults()
}

flag.BoolVar(&verbose, "v", false, "bool; verbose mode")
flag.BoolVar(&headerline, "H", true, "bool; header line; set if the files have a header line, which will be skipped")
flag.StringVar(&agencyParam, "a", "1-4", "agency field definition; without -d use a fixed length fields, with -d use a field,list")
flag.StringVar(&tagParam, "t", "5-14", "tag field definition; without -d use a fixed length fields, with -d use a field,list")
flag.StringVar(&payloadParam, "p", "15-25", "tag's other fields than a and t; without -d use a fixed length fields, with -d use a field,list; optional")
flag.StringVar(&delimiter, "d", "", "use delimited mode for a, t and p values, otherwise use fixed length fields")
flag.IntVar(&batchSize, "b", 0, "batch size for reading input files; it affects the comparins algorithm TODO write doc")
flag.StringVar(&testParam, "test", "", "internal") // TODO write proper go test
flag.Parse()

log.Println("start tvldiff")

if err := parseTagParams(); err != nil {
	log.Println(err)
	fmt.Println(err)
	os.Exit(1)
}

log.Println("test parameter: [", testParam, "]")
if testParam != "" {
	ss := strings.Split(testParam, ":")
	switch {
	case testParam == "test1:default":
		test1("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
		os.Exit(0)
	case strings.Contains(testParam, "test1:"):
		test1(ss[1])
		os.Exit(0)
	case testParam == "test2:default":
		test2("1234,56789,0abcdef,ghijklmnopqrstu,vwxyzABC,DEFGHIJ,KLMNOP,QRST,U,V,WXYZ")
		os.Exit(0)
	case strings.Contains(testParam, "test2:"):
		test2(ss[1])
		os.Exit(0)
	}
}

ts1 := time.Now()
// if profile {
// 	proffile, err := os.Create("tvldiff.prof")
// 	if err != nil {
// 		fmt.Println(err)
// 		os.Exit(1)
// 	}
// 	pprof.StartCPUProfile(proffile)
// }

// works with files only, no "Real" process substitution :((
// TODO make this a single function to call for each FD
file3, file3ok := getFDValid(3, "fdold")
if !file3ok {
	os.Exit(1)
}
file4, file4ok := getFDValid(4, "fdnew")
if !file4ok {
	os.Exit(1)
}
file5, file5ok := getFDValid(5, "fdmerge")
if !file5ok {
	fmt.Println("Cannot write to fd5")
	os.Exit(1)
}
file6, file6ok := getFDValid(6, "fddelete")
if !file6ok {
	fmt.Println("Cannot write to fd6")
	os.Exit(1)
}
file7, file7ok := getFDValid(7, "fdsame")
if !file7ok {
	dbg("discard common lines")
}

if testParam != "" {
	switch {
	case testParam == "test3":
		sc := bufio.NewScanner(file3)
		if sc.Scan() {
			log.Println("read ok from file3")
		} else {
			log.Println("could not read from file3")
			log.Println(sc.Err())
		}
		s := sc.Text()
		log.Println(s)

		sc = bufio.NewScanner(file4)
		if sc.Scan() {
			log.Println("read ok from file4")
		} else {
			log.Println("could not read from file4")
			log.Println(sc.Err())
		}
		s = sc.Text()
		log.Println(s)

		os.Exit(0)
	}
}

var (
	line     string
	sc3, sc4 *bufio.Scanner
	// headerNewDone  bool
	// oldEOF, newEOF bool
)

batchMode := batchSize > 0
// read OLD data from fd3 or stdin
sc3 = bufio.NewScanner(file3)
sc4 = bufio.NewScanner(file4)

dbg("allocate memory")
if batchMode {
	linesOld = make(map[string]struct{}, batchSize)
	linesNew = make(map[string]struct{}, int(batchSize/100)) // I expect 1% tags to be new or updated
} else {
	linesOld = make(map[string]struct{})
	linesNew = make(map[string]struct{})
}
newTagsList = make(map[string]struct{}) // ag+tag only from NEW file

if batchMode {
	log.Println("start processing in batch mode, size", batchSize)
} else {
	log.Println("start processing in full mode")
}

// read both headers to get it over with
if headerline {
	if sc3.Scan() {
		log.Println("ignoring old tag data header line", sc3.Text())
	} else {
		// unable to even read one line, and header was specified - problem
		log.Println("unable to read old header line")
		fmt.Println("unable to read old header line")
		os.Exit(1)
	}
	if sc4.Scan() {
		log.Println("ignoring new tag data header line", sc4.Text())
	} else {
		// unable to even read one line, and header was specified - problem
		log.Println("unable to read new header line")
		fmt.Println("unable to read new header line")
		os.Exit(1)
	}
}

if batchMode { ///////////////////////////////////////////////////////////// batch mode

	for { // read from OLD and NEW alternatively until both are done

		// read batchSize lines from OLD
		for sc3.Scan() {
			line = sc3.Text()
			cntLinesOld++
			linesOld[line] = struct{}{}
			if verbose && cntLinesOld%2_000_000 == 0 {
				log.Println("read 2M old tags, total", cntLinesOld)
			}
			if cntLinesOld%batchSize == 0 {
				break
			}
		}

		if err := sc3.Err(); err != nil {
			log.Println("failed reading old tags:", err)
			os.Exit(1)
		}
		log.Println("read", cntLinesOld, "old tags")
		log.Println("old tags", len(linesOld), "new tags", len(linesNew), "matched tags", cntSameLines)

		// check existing linesNew, read in previous loop and not matched
		for line, _ := range linesNew {
			_, found := linesOld[line]

			if found { // same line exists in OLD, delete from OLD and do not add to NEW
				cntSameLines++
				delete(linesOld, line)
				delete(linesNew, line)
				if file7ok {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to fd7", err)
						fmt.Println("failed to write to fd7", err)
						os.Exit(1)
					}
				}
			}
		}
		log.Println("old buffer", len(linesOld), "new buffer", len(linesNew), "matched so far", cntSameLines)

		// read batchSize lines from NEW, and check against linesOld
		for sc4.Scan() {
			line = sc4.Text()
			cntLinesNew++ // keep a count of lines read regardless if they existed in OLD
			if verbose && cntLinesNew%2_000_000 == 0 {
				log.Println("read 2M new tags, total", cntLinesNew)
			}

			_, found := linesOld[line]

			if found { // same line exists in OLD, delete from OLD and do not add to NEW
				cntSameLines++
				delete(linesOld, line)
				if file7ok {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to fd7", err)
						fmt.Println("failed to write to fd7", err)
						os.Exit(1)
					}
				}
			} else { // line does not exist in OLD, add to NEW
				linesNew[line] = struct{}{}
				newTagsList[getKey(line)] = struct{}{}
			}
			if cntLinesNew%batchSize == 0 {
				break
			}
		}

		if err := sc4.Err(); err != nil {
			log.Println("failed reading from old tag file:", err)
			os.Exit(1)
		}

		log.Println("read", cntLinesNew, "new tags")
		log.Println("old buffer", len(linesOld), "new buffer", len(linesNew), "matched", cntSameLines)

		if cntLinesOld%batchSize != 0 && cntLinesNew%batchSize != 0 {
			break
		}
	} // read from OLD and NEW alternatively until both are done

	log.Println("read", cntLinesOld, "old tags,", cntLinesNew, "new tags,", cntSameLines, "matched,", cntNewLines, "preserved,")

} else { /////////////////////////////////////////////////////////////////// full mode

	// read all OLD tags

	for sc3.Scan() {
		line = sc3.Text()
		cntLinesOld++
		linesOld[line] = struct{}{}
		if verbose && cntLinesOld%2_000_000 == 0 {
			log.Println("read 2M old tags, total", cntLinesOld)
		}
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading old tags:", err)
		os.Exit(1)
	}

	log.Println("read", cntLinesOld, "old tags,", len(linesOld), "are unique")

	// read all NEW tags

	for sc4.Scan() {
		line = sc4.Text()
		cntLinesNew++ // keep a count of lines read regardless if they existed in OLD
		if verbose && cntLinesNew%2_000_000 == 0 {
			log.Println("read 2M new tags, total", cntLinesNew)
			log.Println("old tags", len(linesOld), "new tags", len(linesNew), "matched tags", cntSameLines)
		}

		_, found := linesOld[line]

		if found { // same line exists in OLD, delete from OLD and do not add to NEW
			cntSameLines++
			delete(linesOld, line)
			if file7ok {
				if _, err := file7.WriteString(line + "\n"); err != nil {
					log.Println("failed to write to fd7", err)
					fmt.Println("failed to write to fd7", err)
					os.Exit(1)
				}
			}
		} else { // line does not exist in OLD, add to NEW
			cntNewLines++
			linesNew[line] = struct{}{}
			newTagsList[getKey(line)] = struct{}{}
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading new tags:", err)
		os.Exit(1)
	}

	log.Println("read", cntLinesOld, "old tags,", cntLinesNew, "new tags,", cntSameLines, "matched,", cntNewLines, "preserved,")

} /////////////////////////////////////////////////////////////////// batch mode / full mode

// looking now at agency+tag level
// tags in OLD that dont exist in NEW are deleted tags
// tags in NEW that dont exist in OLD are new tags
// tags in NEW that exist in OLD are UPDATED tags

log.Println("searching for new and updated tags")

for line, _ := range linesOld {
	_, found := newTagsList[getKey(line)]
	if found { // same ag+tg exists in NEW and OLD so it was changed, delete from OLD
		updatedTags++
		delete(linesOld, line)
	}
}

s := fmt.Sprintf("new and updated tags: %d (%.2f%%), deleted tags: %d (%.2f%%)\n",
	len(linesNew), float64(len(linesNew))*100/float64(cntLinesOld),
	len(linesOld), float64(len(linesOld))*100/float64(cntLinesOld))
log.Println(s)
fmt.Println(s)

// write the delete and merge files in batch mode
// for the new tag file, I have the full line
// but for the old tag, I may not have it - the data could come from an existing database table
// so for the delete file only write the ag+tg+pl

log.Println("write merge and delete files")

done := make(chan error)

go func() {
	for str, _ := range linesNew {
		_, err := file5.WriteString(str + "\n")
		if err != nil {
			log.Println("failed to write to merge file", err)
			done <- errors.New("failed to write to merge file")
			break
		}
	}
	log.Println("wrote merge file")
	done <- nil
}()

go func() {
	for str, _ := range linesOld {
		_, err := file6.WriteString(str + "\n")
		if err != nil {
			log.Println("failed to write to delete file", err)
			done <- errors.New("failed to write to delete file")
			break
		}
	}
	log.Println("wrote delete file")
	done <- nil
}()

exitcode := 0
if <-done != nil {
	exitcode = 1
}
if <-done != nil {
	exitcode = 1
}

ts2 := time.Now()
if profile {
	pprof.StopCPUProfile()
}

log.Println("end tvldiff, time taken", math.Ceil(ts2.Sub(ts1).Seconds()), "sec")
os.Exit(exitcode)
}

// getFileValid returns a file from a file descriptor and if it ok to use
func isFDValid(fd int, name string) (*os.File, bool) {
f := os.NewFile(uintptr(fd), name)
if f == nil {
	log.Println("invalid fd", fd, name)
	fmt.Println("invalid fd", fd, name)
	return f, false
}
_, err := f.Stat()
if err != nil && verbose {
	log.Println("cannot stat fd", fd, name)
	fmt.Println("cannot stat fd", fd, name)
}
return f, err == nil
}

func Scomm() {
	log.Println("scomm Scomm")
}
