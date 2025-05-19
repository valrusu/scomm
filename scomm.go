package scomm

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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
)

// TODO I dont think this is used anymore since KEY is also compound
// getSimpleField extracts from a string line the substring or field defined by pos and optionally delimiter
// pos in this case can look like either [4,7] => extract characters 4 to 7
// or, with delimiter, [3,3]
func getSimpleField(line string, pos [2]int, delim string) string {

	if delim == "" {
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
	} else {
		ss := strings.Split(line, delim)

		if pos[0] > len(ss) {
			log.Println("invalid data: " + line)
			os.Exit(1)
			// return ""
		}

		return ss[pos[0]-1]
	}
}

func getCompoundField(line string, pos [][2]int, delim string) string {
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
				y = v[1]
			}
			if y > len(line) {
				log.Println("invalid data: " + line)
				os.Exit(1)
			}
			s += line[x:y]
		}
		return s
	} else { // field-based
		ss := strings.Split(line, delim)
		for _, v := range pos {
			if v[0] == v[1] { // single field
				if v[0] > len(ss) {
					log.Println("invalid data: " + line)
					os.Exit(1)
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
						log.Println("invalid data: " + line)
						os.Exit(1)
					}
					s += ss[w-1] + delim
				}
			}
		}
		// take out the last delimiter
		return strings.TrimRight(s, delim)
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

//////////////////////////////////////////////////

func Scomm(
	verbose bool,
	skipLines int,
	keyParam string,
	payloadParam string,
	dataDelim string,
	batchSize int,
	outputDelim string,
	oldDataIn, newDataIn io.Reader,
	newDataOut, oldDataOut, commDataOut io.Writer,
) error {

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("Start Scomm")

	vrb("start scomm")

	keyPos, err := parseList(keyParam)

	if err != nil {
		log.Println(err)
		os.Exit(1)
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
	file3, file3ok := GetFDFile(3, "oldDataIn")
	if !file3ok {
		return errors.New("bad file descriptor 3")
	}
	file4, file4ok := GetFDFile(4, "newDataIn")
	if !file4ok {
		return errors.New("bad file descriptor 4")
	}
	file5, file5ok := GetFDFile(5, "newDataOut")
	if !file5ok {
		return errors.New("bad file descriptor 5")
	}
	file6, file6ok := GetFDFile(6, "oldDataOut")
	if !file6ok {
		return errors.New("bad file descriptor 6")
	}
	file7, file7ok := GetFDFile(7, "commonDataOut")
	if !file7ok {
		return errors.New("bad file descriptor 7")
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
				log.Println("ignoring old tag data header line", sc3.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read old header line")
				os.Exit(1)
			}
		}
		for i := 1; i <= skipLines; i++ {
			if sc4.Scan() {
				log.Println("ignoring new tag data header line", sc4.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read new header line")
				os.Exit(1)
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
					newKeysList[getCompoundField(line, keyPos, dataDelim)] = struct{}{}
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

	} else { // full mode

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
				if keyParam != "" {
					newKeysList[getCompoundField(line, keyPos, dataDelim)] = struct{}{}
				}
			}
		}

		if err := sc4.Err(); err != nil {
			log.Println("failed reading new tags:", err)
			os.Exit(1)
		}

		log.Println("read", cntLinesOld, "old tags,", cntLinesNew, "new tags,", cntSameLines, "matched,", cntNewLines, "preserved,")

	} /////////////////////////////////////////////////////////////////// batch mode / full mode

	if keyParam != "" {
		// looking now at agency+tag level
		// tags in OLD that dont exist in NEW are deleted tags
		// tags in NEW that dont exist in OLD are new tags
		// tags in NEW that exist in OLD are UPDATED tags

		log.Println("searching for new and updated keys")

		for line, _ := range linesOld {
			_, found := newKeysList[getCompoundField(line, keyPos, dataDelim)]
			if found { // same ag+tg exists in NEW and OLD so it was changed, delete from OLD
				updatedTags++
				delete(linesOld, line)
			}
		}
	}

	s := fmt.Sprintf("new and updated tags: %d (%.2f%%), deleted tags: %d (%.2f%%)\n",
		len(linesNew), float64(len(linesNew))*100/float64(cntLinesOld),
		len(linesOld), float64(len(linesOld))*100/float64(cntLinesOld))
	log.Println(s)

	log.Println("write newDataOut and oldDataOut files")

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

	err = <-done

	if err != nil {
		log.Println(err)
		<-done
		return err
	}

	err = <-done

	if err != nil {
		log.Println(err)
		<-done
		return err
	}

	ts2 := time.Now()
	// if profile {
	// pprof.StopCPUProfile()
	// }

	log.Println("end scomm, time taken", math.Ceil(ts2.Sub(ts1).Seconds()), "sec")
	return nil
}

/////////////

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
