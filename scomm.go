package scomm

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	Profile, Verbose                  bool
	SkipLines, BatchSize              int
	KeyParam, PayloadParam, Delimiter string
	KeyPos, PayloadPos                [][2]int

	cntLinesOld, cntLinesNew, cntSameLines, cntNewLines, updatedTags int
	linesOld                                                         map[string]struct{}
	linesNew                                                         map[string]struct{}
	newKeysList                                                      map[string]struct{}
	file5, file6, file7                                              *os.File
)

// TODO I dont think this is used anymore since KEY is also compound
// getSimpleField extracts from a string line the substring or field defined by pos and optionally delimiter
// pos in this case can look like either [4,7] => extract characters 4 to 7
// or, with delimiter, [3,3]
// func getSimpleField(line string, pos [2]int, delim string) string {

// 	if delim == "" {
// 		var x, y int

// 		if pos[0] == 0 {
// 			x = 0
// 		} else {
// 			x = pos[0] - 1
// 		}
// 		if pos[1] == 0 {
// 			y = len(line)
// 		} else {
// 			y = pos[1]
// 		}
// 		if y > len(line) {
// 			log.Println("invalid data: " + line)
// 			os.Exit(1)
// 		}

// 		return line[x:y]
// 	} else {
// 		ss := strings.Split(line, delim)

// 		if pos[0] > len(ss) {
// 			log.Println("invalid data: " + line)
// 			os.Exit(1)
// 			// return ""
// 		}

// 		return ss[pos[0]-1]
// 	}
// }

func getCompoundField(line string, pos [][2]int, delim string) (string, error) {
	var s string

	if delim == "" { // position-based
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
				y = min(v[1], len(line))
			}
			// if y > len(line) {
			// 	log.Println("invalid data: " + line)
			// 	os.Exit(1)
			// }
			s += line[x:y]
		}
		return s, nil
	} else { // field-based
		ss := strings.Split(line, delim)
		for _, v := range pos {
			if v[0] == v[1] { // single field
				if v[0] > len(ss) {
					strerr := fmt.Sprintf("invalid data: %s for pattern %v delimiter %s", line, pos, delim)
					log.Println(strerr)
					return "", errors.New(strerr)
				}
				s += ss[v[0]-1] + delim
			} else { // interval field like 3-7
				if v[0] == 0 {
					v[0] = 1
				}
				if v[1] == 0 {
					v[1] = len(ss)
				}
				for w := v[0]; w <= v[1]; w++ {
					if w > len(ss) {
						strerr := fmt.Sprintf("invalid data: %s for pattern %v delimiter %s", line, pos, delim)
						log.Println(strerr)
						return "", errors.New(strerr)
					}
					s += ss[w-1] + delim
				}
			}
		}
		// take out the last delimiter
		return strings.TrimRight(s, delim), nil
	}
}

// parseListItem parses one input simple token (int or int-int or int- or -int) interval into an array [2]int
// LIST = ITEM[,ITEM...]
// ITEM = 3   => {3,3}
//
//	4-6 => {4,6}
//	-7  => {0,7}
//	8-  => {8,0}
func parseItem(param string) ([2]int, error) {
	var ret [2]int

	if param == "" {
		return ret, errors.New("option requires range argument")
	}

	if strings.Contains(param, "-") {
		ss := strings.Split(param, "-")

		if len(ss) > 2 {
			return ret, errors.New("invalid range " + param)
		}

		if ss[0] == "" {
			ret[0] = 0 // interval like "-3" which means "1-3"
		} else {
			i, err := strconv.Atoi(ss[0])
			if err != nil {
				return ret, err // TODO use fmt.Errorf
			}
			ret[0] = i
		}

		if ss[1] == "" {
			ret[1] = 0 // interval like "4-" which means "4-end of string"
			return ret, nil
		}

		i, err := strconv.Atoi(ss[1])
		if err != nil {
			return ret, err
		}
		if ret[0] != 0 && ret[0] > i {
			return ret, errors.New("reverted interval " + param)
		}
		ret[1] = i
		return ret, nil
	} else {
		i, err := strconv.Atoi(param)
		if err != nil {
			return ret, err // TODO use fmt.Errorf
		}

		if i == 0 { // positions and fields are 1-based
			return ret, errors.New("field is invalid " + param)
		}

		ret[0], ret[1] = i, i
		return ret, nil
	}
}

func parseList(param string) ([][2]int, error) {

	var ret [][2]int
	ss := strings.Split(param, ",")

	for _, v := range ss {
		y, err := parseItem(v)
		if err != nil {
			return ret, err
		}
		ret = append(ret, y)
	}

	return ret, nil
}

// ////////////////////////////////////////////////
// scomm reads lines from 2 files or pipes and outputs the lines which are common, the ones in first file only and the ones in the second file only
// The input files are received on FD and FD4 respectivelly, or on STDIN; both cannot be in STDIN
// The output lines are generated on FD5 (lines from second file only), FD6 (lines from first file only) and FD7 (lines common)
// If FD5, FD6 or FD7 are not specified, then STDOUT will be used, and if 2 or more are output on STDOUT then the outputDelimiter is mandatory.
// When 2 or 3 of the outputs are going to the same target, the order will be (separated by outputDelimiter):
// 1. lines common to both files
// 2. lines only in the second file
// 3. lines only in the first file
func Scomm(
	verbose bool,
	skipLines int,
	keyParam string,
	payloadParam string,
	dataDelim string,
	batchSize int,
	outputDelim string,
	discardOld, discardNew, discardCommon bool,
) error {

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Start Scomm")

	vrb("start scomm")
	vrb("skipLines", skipLines)
	vrb("keyParam", keyParam)
	vrb("payloadParam", payloadParam)
	vrb("dataDelim", dataDelim)
	vrb("batchSize", batchSize)
	vrb("outputDelim", outputDelim)
	vrb("discardOld", discardOld)
	vrb("discardNew", discardNew)
	vrb("discardCommon", discardCommon)

	keyPos, err := parseList(keyParam)

	if err != nil {
		log.Println(err)
		return err
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
	file3, fd3ok := GetFDFile(3, "oldDataIn")
	if !fd3ok {
		log.Println("bad file descriptor 3, using stdin for old data")
	}
	file4, fd4ok := GetFDFile(4, "newDataIn")
	if !fd4ok {
		if !fd3ok {
			log.Println("cannot receive both files stdin")
			return errors.New("cannot receive both files stdin") // actually I could but it should force FULL mode
		}
		log.Println("bad file descriptor 4, using stdin for new data")
	}

	var (
		cntOnStdout                           int
		fd5ok, fd6ok, fd7ok                   bool
		file5stdout, file6stdout, file7stdout bool
		line                                  string
		sc3, sc4                              *bufio.Scanner
	)

	if !discardNew {
		file5, fd5ok = GetFDFile(5, "newDataOut")
		if fd5ok {
			log.Println("using file descriptor 5 for NEW output data")
		} else {
			log.Println("bad file descriptor 5, do not use for NEW output data")
			file5 = os.Stdout
			cntOnStdout++
			file5stdout = true
		}
	}
	if !discardOld {
		file6, fd6ok = GetFDFile(6, "oldDataOut")
		if fd6ok {
			log.Println("using file descriptor 6 for OLD output data")
		} else {
			log.Println("bad file descriptor 6, do not use for OLD output data")
			file6 = os.Stdout
			cntOnStdout++
			file6stdout = true
		}
	}
	if !discardCommon {
		file7, fd7ok = GetFDFile(7, "commonDataOut")
		if fd7ok {
			log.Println("using file descriptor 7 for COMMON output data")
		} else {
			log.Println("bad file descriptor 7, do not use for COMMON output data")
			file7 = os.Stdout
			cntOnStdout++
			file7stdout = true
		}
	}
	if cntOnStdout >= 2 && outputDelim == "" {
		log.Println("need output delimiter if 2 or more outputs use stdout")
		return errors.New("need output delimiter if 2 or more outputs use stdout")
	}

	batchMode := batchSize > 0
	if fd3ok {
		sc3 = bufio.NewScanner(file3)
	} else {
		sc3 = bufio.NewScanner(os.Stdin)
	}
	if fd4ok {
		sc4 = bufio.NewScanner(file4)
	} else {
		sc4 = bufio.NewScanner(os.Stdin)
	}

	vrb("allocate memory")
	if batchMode {
		linesOld = make(map[string]struct{}, batchSize)
		linesNew = make(map[string]struct{}, 2*int(batchSize/100)) // I expect 1-2% tags to be new or updated
	} else {
		linesOld = make(map[string]struct{})
		linesNew = make(map[string]struct{})
	}
	newKeysList = make(map[string]struct{})

	if batchMode {
		log.Println("start processing in batch mode, size", batchSize)
	} else {
		log.Println("start processing in full mode")
	}

	// read both headers to get it over with
	if skipLines > 0 {
		for i := 1; i <= skipLines; i++ {
			if sc3.Scan() {
				log.Println("ignoring old data header line", sc3.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read old header line")
				return errors.New("unable to read old header line")
			}
		}
		for i := 1; i <= skipLines; i++ {
			if sc4.Scan() {
				log.Println("ignoring new data header line", sc4.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read new header line")
				return errors.New("unable to read new header line")
			}
		}
	}

	if batchMode {

		for { // read from OLD and NEW alternatively until both are done

			// read batchSize lines from OLD
			for sc3.Scan() {
				line = sc3.Text()
				cntLinesOld++
				linesOld[line] = struct{}{}
				if cntLinesOld%2_000_000 == 0 {
					vrb("read 2M old tags, total", cntLinesOld)
				}
				if cntLinesOld%batchSize == 0 {
					break
				}
			}

			if err := sc3.Err(); err != nil {
				log.Println("failed reading old lines:", err)
				return fmt.Errorf("failed reading old tags: %v", err)
			}
			log.Println("read", cntLinesOld, "old lines")
			log.Println("old lines", len(linesOld), "new lines", len(linesNew), "matched lines", cntSameLines)

			// check existing linesNew, read in previous loop and not matched
			for line, _ := range linesNew {
				_, found := linesOld[line]

				if found { // same line exists in OLD, delete from OLD and do not add to NEW
					cntSameLines++
					delete(linesOld, line)
					delete(linesNew, line)
					if !discardCommon {
						if _, err := file7.WriteString(line + "\n"); err != nil {
							log.Println("failed to write common line", err)
							return fmt.Errorf("failed to write common line: %v", err)
						}
					}
				}
			}
			log.Println("old buffer", len(linesOld), "new buffer", len(linesNew), "matched so far", cntSameLines)

			// read batchSize lines from NEW, and check against linesOld
			for sc4.Scan() {
				line = sc4.Text()
				cntLinesNew++ // keep a count of lines read regardless if they existed in OLD
				if cntLinesNew%2_000_000 == 0 {
					vrb("read 2M new tags, total", cntLinesNew)
				}

				_, found := linesOld[line]

				if found { // same line exists in OLD, delete from OLD and do not add to NEW
					cntSameLines++
					delete(linesOld, line)
					if !discardCommon {
						if _, err := file7.WriteString(line + "\n"); err != nil {
							log.Println("failed to write common line", err)
							return fmt.Errorf("failed to write common line: %v", err)
						}
					}
				} else { // line does not exist in OLD, add to NEW
					linesNew[line] = struct{}{}
					if keyParam != "" {
						keyval, err := getCompoundField(line, keyPos, dataDelim)
						if err != nil {
							return err
						}
						newKeysList[keyval] = struct{}{}
					}
				}
				if cntLinesNew%batchSize == 0 {
					break
				}
			}

			if err := sc4.Err(); err != nil {
				log.Println("failed reading from old file:", err)
				return fmt.Errorf("failed reading from old file: %v", err)
			}

			log.Println("read", cntLinesNew, "new lines")
			log.Println("old buffer", len(linesOld), "new buffer", len(linesNew), "matched", cntSameLines)

			if cntLinesOld%batchSize != 0 && cntLinesNew%batchSize != 0 {
				break
			}
		} // read from OLD and NEW alternatively until both are done

		log.Println("read", cntLinesOld, "old tags,", cntLinesNew, "new tags,", cntSameLines, "matched,", cntNewLines, "preserved,")

	} else { // full mode

		// read all OLD tags

		for sc3.Scan() {
			line = sc3.Text()
			cntLinesOld++
			linesOld[line] = struct{}{}
			if cntLinesOld%2_000_000 == 0 {
				vrb("read 2M old tags, total", cntLinesOld)
			}
		}

		if err := sc3.Err(); err != nil {
			log.Println("failed reading old lines:", err)
			return fmt.Errorf("failed reading old lines: %v", err)
		}

		log.Println("read", cntLinesOld, "old lines,", len(linesOld), "are unique")

		// read all NEW tags

		for sc4.Scan() {
			line = sc4.Text()
			cntLinesNew++ // keep a count of lines read regardless if they existed in OLD
			if cntLinesNew%2_000_000 == 0 {
				vrb("read 2M new tags, total", cntLinesNew)
				vrb("old lines", len(linesOld), "new lines", len(linesNew), "matched lines", cntSameLines)
			}

			_, found := linesOld[line]

			if found { // same line exists in OLD, delete from OLD and do not add to NEW
				cntSameLines++
				delete(linesOld, line)
				if !discardCommon {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write common line", err)
						return fmt.Errorf("failed to write common line: %v", err)
					}
				}
			} else { // line does not exist in OLD, add to NEW
				cntNewLines++
				linesNew[line] = struct{}{}
				if keyParam != "" {
					keyval, err := getCompoundField(line, keyPos, dataDelim)
					if err != nil {
						return err
					}
					newKeysList[keyval] = struct{}{}
				}
			}
		}

		if err := sc4.Err(); err != nil {
			log.Println("failed reading new lines:", err)
			return fmt.Errorf("failed reading new lines: %v", err)
		}

		log.Println("read", cntLinesOld, "old lines", cntLinesNew, "new lines,", cntSameLines, "matched,", cntNewLines, "preserved")

	} /////////////////////////////////////////////////////////////////// batch mode / full mode

	if keyParam != "" {
		// looking now at agency+tag level
		// tags in OLD that dont exist in NEW are deleted tags
		// tags in NEW that dont exist in OLD are new tags
		// tags in NEW that exist in OLD are UPDATED tags

		log.Println("searching for new and updated keys")

		for line, _ := range linesOld {
			keyval, err := getCompoundField(line, keyPos, dataDelim)
			if err != nil {
				return err
			}
			_, found := newKeysList[keyval]
			if found { // same key exists in NEW and OLD so something was changed, delete from OLD, keep in NEW
				updatedTags++
				delete(linesOld, line)
			}
		}
	}

	s := fmt.Sprintf("new and updated lines: %d (%.2f%%), deleted lines: %d (%.2f%%)\n",
		len(linesNew), float64(len(linesNew))*100/float64(cntLinesOld),
		len(linesOld), float64(len(linesOld))*100/float64(cntLinesOld))
	log.Println(s)

	if file7stdout && (file5stdout || file6stdout) { // NEW or OLD will come after COMMON on stdout
		file7.WriteString(outputDelim + "\n")
	}

	if file5stdout && file6stdout {
		// TODO do not parallelize this if they both go to stdout

		if err := writeNewData(); err != nil {
			// log.Println(err)
			return err
		}

		file5.WriteString(outputDelim + "\n")

		if err := writeOldData(); err != nil {
			// log.Println(err)
			return err
		}
	} else {
		done := make(chan error)

		go func() {
			done <- writeNewData()
		}()

		go func() {
			done <- writeOldData()
		}()

		err1 := <-done
		err2 := <-done

		if err1 != nil {
			log.Println(err1)
			return err1
		}

		if err2 != nil {
			log.Println(err2)
			return err2
		}
	}

	ts2 := time.Now()
	// if profile {
	// pprof.StopCPUProfile()
	// }

	log.Println("end scomm, time taken", math.Ceil(ts2.Sub(ts1).Seconds()), "sec")
	return nil
}

/////////////

func writeNewData() error {
	log.Println("write newDataOut")
	for str, _ := range linesNew {
		_, err := file5.WriteString(str + "\n")
		if err != nil {
			log.Println("failed to write to new data output:", err)
			return fmt.Errorf("failed to write new data output: %v", err)
		}
	}
	log.Println("wrote new data output")
	return nil
}

func writeOldData() error {
	log.Println("write old data output")
	for str, _ := range linesOld {
		_, err := file6.WriteString(str + "\n")
		if err != nil {
			log.Println("failed to write to old data output:", err)
			return fmt.Errorf("failed to write old data output: %v", err)
		}
	}
	log.Println("wrote old data output")
	return nil
}

func vrb(params ...interface{}) {
	if Verbose {
		log.Println(params...)
	}
}

// ///////////
// TODO remove all calls to this or comment it out
func dbg(params ...interface{}) {
	params = append(params, "")
	copy(params[1:], params[0:])
	params[0] = "DEBUG:"
	log.Println(params...)
}

// GetFDFile returns a file from a file descriptor and if it ok to use
func GetFDFile(fd int, name string) (*os.File, bool) {
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		log.Println("invalid fd", fd, name)
		return f, false
	}
	_, err := f.Stat()
	if err != nil {
		log.Println("cannot stat fd", fd, name)
	}
	return f, err == nil
}
