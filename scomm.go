package scomm

// the tool is meant to DO something, I should not have to enable things, more to disable
// so by default it should output as much as possible, and have options to disable stuff

// INPUT:
// order of the files matter in the output; file1 is considered the "old" one and file2 the "new" one
// FILE1 from FD3
// FILE2 from DF4

// OUTPUT: without -k/-p                     with -k/-p/
// FD5: lines unique to FILE1                FILE1 lines for which key(file1) does not exist in FILE2, or key exists
// FD6: lines unique to FILE2
// FD7: lines common                         lines common
//                                           FILE1 lines for which key(file1) exists in FILE2 but payloads are different
// example without k/p:
// 111   111   111 common FD7
//       222   222 new FD6
// 333         333 old FD5

// with k/p: treat the inputs as this is the "only" info I care about in a line; but I need original line output :(
// example with k/p:
//  111 22222 33333333     111 22222 33333333      111 22222 33333333 FD7
//  111 22222 33333333     111 22222 44444444      111 22222 44444444 FD7
//  111 22222 33333333     111 33333 22222222      111 33333 22222222 FD6 update   111 22222 33333333 FD8 delete (option)
//                         444 22222 33333333      444 22222 33333333 FD6 insert
//  555 22222 33333333                             555 22222 33333333 FD5 delete

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
	cntLinesFile1, cntLinesFile2, cntSameLines, cntNewLines, updatedTags int
	linesFile1                                                           map[string]struct{}
	linesFile2                                                           map[string]struct{}
	newKeysList                                                          map[string]struct{}
	file3, file4, file5, file6, file7, file8                             *os.File
	gverbose                                                             bool
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

// getCompundField returns data from a line, based on the field definition
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
			dbg(x, y)
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

///////////////////////////////////////////////////

// scomm reads lines from 2 files or pipes and outputs the lines which are common, the ones in first file only and the ones in the second file only
func Scomm(
	verbose bool,
	skipLines int,
	keyParam string,
	payloadParam string,
	dataDelim string,
	batchSize int,
	extraFile1 bool,
	discard5, discard6, discard7, discard8, discard9 bool,
) error {

	var (
		fd3ok, fd4ok, fd5ok, fd6ok, fd7ok, fd8ok bool
		line                                     string
		sc3, sc4                                 *bufio.Scanner
		useKey                                   bool
	)

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Start Scomm")

	gverbose = verbose

	ts1 := time.Now()
	// if profile {
	// 	proffile, err := os.Create("tvldiff.prof")
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		os.Exit(1)
	// 	}
	// 	pprof.StartCPUProfile(proffile)
	// }

	vrb("start scomm")
	vrb("skipLines", skipLines)
	vrb("keyParam", keyParam)
	vrb("payloadParam", payloadParam)
	vrb("dataDelim", dataDelim)
	vrb("batchSize", batchSize)
	vrb("extraFile1", extraFile1)
	vrb("discard5", discard5)
	vrb("discard6", discard6)
	vrb("discard7", discard7)
	vrb("discard8", discard8)

	if keyParam != "" && payloadParam == "" && keyParam == "" && payloadParam != "" {
		log.Println("need both key / payload parameters or none")
		return errors.New("need both key / payload parameters or none")
	}

	useKey = true

	keyPos, err := parseList(keyParam)

	if err != nil {
		log.Println(err)
		return err
	}

	// payloadPos, err := parseList(payloadParam)

	// if err != nil {
	// 	log.Println(err)
	// 	return err
	// }

	// works with files only, no "Real" process substitution :((
	file3, fd3ok = GetFDFile(3, "file1DataIn")
	if !fd3ok {
		log.Println("bad file descriptor 3")
		return errors.New("bad file descriptor 3")
	}

	file4, fd4ok = GetFDFile(4, "file2DataIn")
	if !fd4ok {
		log.Println("bad file descriptor 4")
		return errors.New("bad file descriptor 4")
	}

	if !discard5 {
		file5, fd5ok = GetFDFile(5, "file1DataOut")
		if !fd5ok {
			log.Println("bad file descriptor 5")
			return errors.New("bad file descriptor 5")
		}
	}

	if !discard6 {
		file6, fd6ok = GetFDFile(6, "file2DataOut")
		if !fd6ok {
			log.Println("bad file descriptor 6")
			return errors.New("bad file descriptor 6")
		}
	}

	if !discard7 {
		file7, fd7ok = GetFDFile(7, "commonDataOut")
		if !fd7ok {
			log.Println("bad file descriptor 7")
			errors.New("bad file descriptor 7")
		}
	}

	if extraFile1 && discard8 {
		log.Println("extra output requested for FILE1 data and discarded at the same time")
		return errors.New("extra output requested for FILE1 data and discarded at the same time")
	}
	if !extraFile1 && discard8 {
		log.Println("extra output not requested for FILE1 data but discard requested")
		return errors.New("extra output not requested for FILE1 data but discard requested")
	}
	if extraFile1 && !discard8 {
		//  normal if extra requested
		file8, fd8ok = GetFDFile(8, "file1DataOutExtra")
		if !fd8ok {
			log.Println("bad file descriptor 8")
			return errors.New("bad file descriptor 8")
		}
	}
	if !extraFile1 && !discard8 {
		// do not display this data
		fd8ok = false
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
		linesFile1 = make(map[string]struct{}, batchSize)
		linesFile2 = make(map[string]struct{}, 2*int(batchSize/100)) // I expect 1-2% tags to be new or updated
	} else {
		linesFile1 = make(map[string]struct{})
		linesFile2 = make(map[string]struct{})
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
		/*
			for { // read from OLD and NEW alternatively until both are done
				// TODO the FULL mode should be included in the BATCH mode as a special case

				// read batchSize lines from OLD
				for sc3.Scan() {
					line = sc3.Text()
					cntLinesFile1++
					linesFile1[line] = struct{}{}
					if cntLinesFile1%2_000_000 == 0 {
						vrb("read 2M old tags, total", cntLinesFile1)
					}
					if cntLinesFile1%batchSize == 0 {
						break
					}
				}

				if err := sc3.Err(); err != nil {
					log.Println("failed reading old lines:", err)
					return fmt.Errorf("failed reading old lines: %v", err)
				}
				log.Println("read", cntLinesFile1, "old lines")
				log.Println("old lines", len(linesFile1), "new lines", len(linesFile2), "matched lines", cntSameLines)

				// check existing linesNew, read in previous loop and not matched
				for line, _ := range linesFile2 {
					_, found := linesFile1[line]

					if found { // same line exists in OLD, delete from OLD and do not add to NEW
						cntSameLines++
						delete(linesFile1, line)
						delete(linesFile2, line)
						if !discardCommon {
							if _, err := file7.WriteString(line + "\n"); err != nil {
								log.Println("failed to write common line", err)
								return fmt.Errorf("failed to write common line: %v", err)
							}
						}
					}
				}
				log.Println("old buffer", len(linesFile1), "new buffer", len(linesFile2), "matched so far", cntSameLines)

				// read batchSize lines from NEW, and check against linesOld
				for sc4.Scan() {
					line = sc4.Text()
					cntLinesFile2++ // keep a count of lines read regardless if they existed in OLD
					if cntLinesFile2%2_000_000 == 0 {
						vrb("read 2M new lines, total", cntLinesFile2)
					}

					_, found := linesFile1[line]

					if found { // same line exists in OLD, delete from OLD and do not add to NEW
						cntSameLines++
						delete(linesFile1, line)
						if !discardCommon {
							if _, err := file7.WriteString(line + "\n"); err != nil {
								log.Println("failed to write common line", err)
								return fmt.Errorf("failed to write common line: %v", err)
							}
						}
					} else { // line does not exist in OLD, add to NEW
						linesFile2[line] = struct{}{}
						if keyParam != "" {
							keyval, err := getCompoundField(line, keyPos, dataDelim)
							if err != nil {
								return err
							}
							newKeysList[keyval] = struct{}{}
						}
					}
					if cntLinesFile2%batchSize == 0 {
						break
					}
				}

				if err := sc4.Err(); err != nil {
					log.Println("failed reading from old file:", err)
					return fmt.Errorf("failed reading from old file: %v", err)
				}

				log.Println("read", cntLinesFile2, "new lines")
				log.Println("old buffer", len(linesFile1), "new buffer", len(linesFile2), "matched", cntSameLines)

				if cntLinesFile1%batchSize != 0 && cntLinesFile2%batchSize != 0 {
					break
				}
			} // read from OLD and NEW alternatively until both are done

			log.Println("read", cntLinesFile1, "old lines,", cntLinesFile2, "new lines,", cntSameLines, "matched,", cntNewLines, "preserved,")
		*/
	} else { // full mode
		// TODO include this logic in the batch mode and make batch mode default

		// read all FILE1 lines

		for sc3.Scan() {
			line = sc3.Text()
			cntLinesFile1++
			linesFile1[line] = struct{}{}
			if cntLinesFile1%2_000_000 == 0 {
				vrb("read 2M old lines, total", cntLinesFile1)
			}
		}

		if err := sc3.Err(); err != nil {
			log.Println("failed reading FD3:", err)
			return fmt.Errorf("failed reading FD3: %v", err)
		}

		log.Println("read", cntLinesFile1, "file1 lines,", len(linesFile1), "are unique")

		// read all FILE2 lines

		for sc4.Scan() {
			line = sc4.Text()
			cntLinesFile2++ // keep a count of lines read regardless if they existed in OLD
			if cntLinesFile2%2_000_000 == 0 {
				vrb("read 2M file2 lines, total", cntLinesFile2)
				vrb("file1 lines", len(linesFile1), "file2 lines", len(linesFile2), "matched lines", cntSameLines)
			}

			_, found := linesFile1[line]

			if found { // same line exists in OLD, delete from OLD and do not add to NEW
				cntSameLines++
				delete(linesFile1, line)
				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write common line", err)
						return fmt.Errorf("failed to write common line: %v", err)
					}
				}
			} else { // line does not exist in OLD, add to NEW
				cntNewLines++
				linesFile2[line] = struct{}{}

				if useKey {
					// 	keyval, err := getCompoundField(line, keyPos, dataDelim)
					// 	if err != nil {
					// 		return err
					// 	}
					// 	newKeysList[keyval] = struct{}{}
				}
			}
		}

		if err := sc4.Err(); err != nil {
			log.Println("failed reading FD4:", err)
			return fmt.Errorf("failed reading FD4: %v", err)
		}

		log.Println("read", cntLinesFile1, "old lines", cntLinesFile2, "new lines,", cntSameLines, "matched,", cntNewLines, "preserved")

	} /////////////////////////////////////////////////////////////////// batch mode / full mode

	if keyParam != "" {
		// looking now at agency+tag level
		// tags in OLD that dont exist in NEW are deleted tags
		// tags in NEW that dont exist in OLD are new tags
		// tags in NEW that exist in OLD are UPDATED tags

		log.Println("searching for new and updated keys")

		for line, _ := range linesFile1 {
			keyval, err := getCompoundField(line, keyPos, dataDelim)
			if err != nil {
				return err
			}
			_, found := newKeysList[keyval]
			if found { // same key exists in NEW and OLD so something was changed, delete from OLD, keep in NEW
				updatedTags++
				delete(linesFile1, line)
			}
		}
	}

	s := fmt.Sprintf("new and updated lines: %d (%.2f%%), deleted lines: %d (%.2f%%)\n",
		len(linesFile2), float64(len(linesFile2))*100/float64(cntLinesFile1),
		len(linesFile1), float64(len(linesFile1))*100/float64(cntLinesFile1))
	log.Println(s)

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
	for str, _ := range linesFile2 {
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
	for str, _ := range linesFile1 {
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
	if gverbose {
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
		log.Println("invalid FD", fd, name)
		return f, false
	}
	// _, err := f.Stat()
	// if err != nil {
	// log.Println("cannot stat fd", fd, name)
	// }
	// return f, err == nil
	return f, true
}
