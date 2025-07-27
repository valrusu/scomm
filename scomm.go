package scomm

// the tool is meant to DO something, I should not have to enable things, more to disable
// so by default it should output as much as possible, and have options to disable stuff

// INPUT:
// order of the files matter in the output; file1 is considered the "old" one and file2 the "new" one
// FILE1 from FD3
// FILE2 from DF4

// Example OUTPUT: without -k/-p             with -k/-p/
// FD5: lines unique to FILE1
// FD6: lines unique to FILE2
// FD7: lines common                         lines common
//                                           FILE1 lines for which key(file1) exists in FILE2 but payloads are different
//
// the output is meant like either "merge + delete" (no -e) or "delete + insert" (with -e)
// I could change -e to -m for merge and reverse its logic
//
// Example OUTPUT: without -k/-p; ignore -f -e (all defaults):n
//  AAA BBBBB CCCCCCCC     AAA BBBBB CCCCCCCC      AAA BBBBB CCCCCCCC FD7 (same line)
//  DDD EEEEE FFFFFFFF     DDD EEEEE GGGGGGGG      DDD EEEEE FFFFFFFF FD5 (only in file1)   DDD EEEEE GGGGGGGG FD6 (only in file2)
//  HHH IIIII JJJJJJJJ     HHH KKKKK LLLLLLLL      HHH IIIII JJJJJJJJ FD5 (only in file1)   HHH KKKKK LLLLLLLL FD6 (only in file2)
//                         MMM NNNNN OOOOOOOO      MMM NNNNN OOOOOOOO FD6 (only in file2)
//  PPP QQQQQ RRRRRRRR                             PPP QQQQQ RRRRRRRR FD5 (only in file1)
//
// Example OUTPUT: with -k/-p, without -f, with -m (merge+delete) (-k/-p + defaults)
//  AAA BBBBB CCCCCCCC     AAA BBBBB CCCCCCCC      AAA BBBBB FD7 (same k+p)
//  DDD EEEEE FFFFFFFF     DDD EEEEE GGGGGGGG      DDD EEEEE FD7 (same k+p)
//  HHH IIIII JJJJJJJJ     HHH KKKKK LLLLLLLL      HHH KKKKK FD6 (same k, diff p: insert)
//                         MMM NNNNN OOOOOOOO      MMM NNNNN FD6 (only in file2: insert)
//  PPP QQQQQ RRRRRRRR                             PPP QQQQQ FD5 (only in file1: delete)
//
// Example OUTPUT: with -k/-p, without -f, without -m (delete+insert)
//  AAA BBBBB CCCCCCCC     AAA BBBBB CCCCCCCC      AAA BBBBB FD7 (same k+p)
//  DDD EEEEE FFFFFFFF     DDD EEEEE GGGGGGGG      DDD EEEEE FD7 (same k+p)
//  HHH IIIII JJJJJJJJ     HHH KKKKK LLLLLLLL      HHH KKKKK FD6 (same k, diff p: merge)    HHH IIIII FD5 (delete)
//                         MMM NNNNN OOOOOOOO      MMM NNNNN FD6 (only in file2: merge)
//  PPP QQQQQ RRRRRRRR                             PPP QQQQQ FD5 (only in file1: delete)
//
// Example OUTPUT: with -k/-p, with -f, with -m (merge+delete)
//  AAA BBBBB CCCCCCCC     AAA BBBBB CCCCCCCC      AAA BBBBB CCCCCCCC FD7 (same line)
//  DDD EEEEE FFFFFFFF     DDD EEEEE GGGGGGGG      DDD EEEEE GGGGGGGG FD7 (same k+p, display G because of how I search)
//  HHH IIIII JJJJJJJJ     HHH KKKKK LLLLLLLL      HHH KKKKK LLLLLLLL FD6 (same k, diff p: merge)
//                         MMM NNNNN OOOOOOOO      MMM NNNNN OOOOOOOO FD6 (only in file2: merge)
//  PPP QQQQQ RRRRRRRR                             PPP QQQQQ RRRRRRRR FD5 (only in file1: delete)
//
// Example OUTPUT: with -k/-p, with -f, without -m (delete+insert)
//  AAA BBBBB CCCCCCCC     AAA BBBBB CCCCCCCC      AAA BBBBB CCCCCCCC FD7 (same line)
//  DDD EEEEE FFFFFFFF     DDD EEEEE GGGGGGGG      DDD EEEEE GGGGGGGG FD7 (same k+p, display G because of how I search)
//  HHH IIIII JJJJJJJJ     HHH KKKKK LLLLLLLL      HHH KKKKK LLLLLLLL FD6 (same k, diff p: insert)   HHH IIIII JJJJJJJJ FD5 (delete)
//                         MMM NNNNN OOOOOOOO      MMM NNNNN OOOOOOOO FD6 (only in file2: insert)
//  PPP QQQQQ RRRRRRRR                             PPP QQQQQ RRRRRRRR FD5 (only in file1: delete)
//
// TODO if k/p-based input with delimiter, the output has to contain the delimiter too (because fields will not be fixed length)
//

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

type lineParts struct {
	payLoad, line string
}

var (
	cntLinesFile1, cntLinesFile2, cntSameLines, cntNewLines, updatedTags int

	linesFile1KP                      map[string]string // used for key compare, key+payload output
	linesFile2KP                      map[string]string
	linesFile1KLP                     map[string]lineParts // used for key compare, full line output
	linesFile2KLP                     map[string]lineParts
	linesFile1L                       map[string]struct{} // used for line compare, full line output
	linesFile2L                       map[string]struct{}
	file3, file4, file5, file6, file7 *os.File
	verbose                           bool
	batchSize                         int
	useKey, fullLineOut, outModeMerge bool
	keyPos, payloadPos                [][2]int
	dataDelim                         string

	fd3ok, fd4ok, fd5ok, fd6ok, fd7ok bool
	sc3, sc4                          *bufio.Scanner
	discard5, discard6, discard7      bool
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
func getCompoundFieldValue(line string, pos [][2]int, delim string) (string, error) {
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
			// dbg(x, y)
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

func lineSearch() error {
	vrb("lineSearch: allocate memory")
	linesFile1L = make(map[string]struct{}, 100_000_000)
	linesFile2L = make(map[string]struct{}, 100_000_000)

	for sc3.Scan() {
		line := sc3.Text()
		cntLinesFile1++

		if cntLinesFile1%2_000_000 == 0 {
			vrb("read 2M lines from file1, total", cntLinesFile1)
		}

		linesFile1L[line] = struct{}{}
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading FD3:", err)
		return fmt.Errorf("failed reading FD3: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines,", len(linesFile1L), "are unique")

	for sc4.Scan() {
		line := sc4.Text()
		cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

		if cntLinesFile2%2_000_000 == 0 {
			vrb("read 2M lines from file2, total", cntLinesFile2)
			// loop stats
			log.Println(
				"file1 read", cntLinesFile1,
				"buffered", len(linesFile1L),
				percentage(len(linesFile1L), cntLinesFile1),
				"file2 read", cntLinesFile2,
				"buffered", len(linesFile2L),
				percentage(len(linesFile2L), cntLinesFile2),
				"matched", cntSameLines,
				percentage(cntSameLines, cntLinesFile2),
			)
		}

		_, found := linesFile1L[line]

		if found {
			cntSameLines++
			delete(linesFile1L, line)

			if !discard7 {
				if _, err := file7.WriteString(line + "\n"); err != nil {
					log.Println("failed to write to FD7:", err)
					return fmt.Errorf("failed to write to FD7: %v", err)
				}
			}
		} else {
			cntNewLines++
			linesFile2L[line] = struct{}{}
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading FD4:", err)
		return fmt.Errorf("failed reading FD4: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines", cntLinesFile2, "file2 lines")
	log.Println(cntSameLines, "matched,", len(linesFile1L), "only in file1", cntNewLines, "only in file2")
	// I like a percentage stat: file1 left vs max(file1, file2), f2 left vs same, same lines vs same
	m := max(cntLinesFile1, cntLinesFile2)
	p5 := percentage(len(linesFile1L), m)
	p6 := percentage(len(linesFile2L), m)
	p7 := percentage(cntSameLines, m)
	log.Printf("%s matched, %s only in file1, %s only in file2\n", p7, p5, p6)

	done := make(chan error)

	go func() {
		done <- writeFile2DataL()
	}()

	go func() {
		done <- writeFile1DataL()
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

	return nil
}

func lineSearchBatch() error {
	vrb("lineSearchBatch: batchsize", batchSize)

	linesFile1L = make(map[string]struct{}, batchSize) // it may grow beyond batchSize
	linesFile2L = make(map[string]struct{}, batchSize) // it may grow beyond batchSize
	var loopAgain bool

	// read alternatively file1 and file2 until BOTH are done
	for {
		loopAgain = false

		// read max batchSize lines from file1
		for sc3.Scan() {
			loopAgain = true // at least one line read, will loop again
			line := sc3.Text()
			cntLinesFile1++

			if batchSize > 2_000_000 && cntLinesFile1%2_000_000 == 0 {
				vrb("read 2M lines from file1, total", cntLinesFile1)
			}

			linesFile1L[line] = struct{}{}

			if cntLinesFile1%batchSize == 0 {
				break
			}
		}

		if err := sc3.Err(); err != nil {
			log.Println("failed reading FD3:", err)
			return fmt.Errorf("failed reading FD3: %v", err)
		}

		// loop stats
		log.Println(
			"file1 read", cntLinesFile1,
			"buffered", len(linesFile1L),
			percentage(len(linesFile1L), cntLinesFile1),
			"file2 read", cntLinesFile2,
			"buffered", len(linesFile2L),
			percentage(len(linesFile2L), cntLinesFile2),
			"matched", cntSameLines,
			percentage(cntSameLines, cntLinesFile2),
		)

		// check existing file2 lines (read in previous loop) against the file1 lines, just read plus eventual old ones
		for line, _ := range linesFile2L { // initially will be empty, then it will accumulate data
			_, found := linesFile1L[line]

			if found {
				cntSameLines++
				delete(linesFile1L, line)
				delete(linesFile2L, line)

				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			}
		}
		if len(linesFile2L) > 0 { // or cntLinesFile2 ?
			// loop stats
			log.Println(
				"file1 read", cntLinesFile1,
				"buffered", len(linesFile1L),
				percentage(len(linesFile1L), cntLinesFile1),
				"file2 read", cntLinesFile2,
				"buffered", len(linesFile2L),
				percentage(len(linesFile2L), cntLinesFile2),
				"matched", cntSameLines,
				percentage(cntSameLines, cntLinesFile2),
			)
		}

		// now read from file2, checking against file1
		for sc4.Scan() {
			loopAgain = true // at least one line read, will loop again
			line := sc4.Text()
			cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

			if batchSize > 2_000_000 && cntLinesFile2%2_000_000 == 0 {
				vrb("read 2M lines from file2, total", cntLinesFile2)
			}

			_, found := linesFile1L[line]

			if found {
				cntSameLines++
				delete(linesFile1L, line)

				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			} else {
				cntNewLines++
				linesFile2L[line] = struct{}{}
			}

			if cntLinesFile2%batchSize == 0 {
				break
			}
		}
		// loop stats
		log.Println(
			"file1 read", cntLinesFile1,
			"buffered", len(linesFile1L),
			percentage(len(linesFile1L), cntLinesFile1),
			"file2 read", cntLinesFile2,
			"buffered", len(linesFile2L),
			percentage(len(linesFile2L), cntLinesFile2),
			"matched", cntSameLines,
			percentage(cntSameLines, cntLinesFile2),
		)

		if err := sc4.Err(); err != nil {
			log.Println("failed reading FD4:", err)
			return fmt.Errorf("failed reading FD4: %v", err)
		}

		if !loopAgain {
			break
		}
	}

	log.Println("read", cntLinesFile1, "file1 lines", cntLinesFile2, "file2 lines")
	log.Println(cntSameLines, "matched,", len(linesFile1L), "only in file1", cntNewLines, "only in file2")
	// I like a percentage stat: file1 left vs max(file1, file2), f2 left vs same, same lines vs same
	m := max(cntLinesFile1, cntLinesFile2)
	p5 := percentage(len(linesFile1L), m)
	p6 := percentage(len(linesFile2L), m)
	p7 := percentage(cntSameLines, m)
	log.Printf("%s matched, %s only in file1, %s only in file2\n", p7, p5, p6)

	done := make(chan error)

	go func() {
		done <- writeFile2DataL()
	}()

	go func() {
		done <- writeFile1DataL()
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

	return nil
}

func keySearchPayloadOutput() error {
	vrb("keySearchPayloadOutput: allocate memory")
	linesFile1KP = make(map[string]string)
	linesFile2KP = make(map[string]string)

	for sc3.Scan() {
		line := sc3.Text()
		cntLinesFile1++

		if cntLinesFile1%2_000_000 == 0 {
			vrb("read 2M lines from file1, total", cntLinesFile1)
		}

		k1, err := getCompoundFieldValue(line, keyPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		p1, err := getCompoundFieldValue(line, payloadPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		linesFile1KP[k1] = p1
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading FD3:", err)
		return fmt.Errorf("failed reading FD3: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines,", len(linesFile1KP), "are unique")

	for sc4.Scan() {
		line := sc4.Text()
		cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

		if cntLinesFile2%2_000_000 == 0 {
			vrb("read 2M lines from file2, total", cntLinesFile2)
			vrb("file1 lines", len(linesFile1KP), "file2 lines", len(linesFile2KLP), "matched lines", cntSameLines)
		}

		k2, err := getCompoundFieldValue(line, keyPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		p2, err := getCompoundFieldValue(line, payloadPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		p1, found := linesFile1KP[k2]

		if found { // key found in file1
			if p1 == p2 { // key found and payload same
				cntSameLines++
				delete(linesFile1KP, k2)
				if !discard7 {
					if _, err := file7.WriteString(k2 + dataDelim + p2 + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			} else { // key found and payload diff
				cntNewLines++
				linesFile2KP[k2] = p2
				if outModeMerge { // dont write deletes
					delete(linesFile1KP, k2)
				}
			}
		} else { // key not found
			cntNewLines++
			linesFile2KP[k2] = p2
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading FD4:", err)
		return fmt.Errorf("failed reading FD4: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines", cntLinesFile2, "file2 lines,")
	log.Println(cntSameLines, "matched,", len(linesFile1L), "file1 preserved", cntNewLines, "file2 preserved")

	done := make(chan error)

	go func() {
		done <- writeFile2DataKP()
	}()

	go func() {
		done <- writeFile1DataKP()
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

	return nil
}

func keySearchFullOutput() error {
	vrb("keySearchFullOutput: allocate memory")
	linesFile1KLP = make(map[string]lineParts)
	linesFile2KLP = make(map[string]lineParts)

	for sc3.Scan() {
		line := sc3.Text()
		cntLinesFile1++

		if cntLinesFile1%2_000_000 == 0 {
			vrb("read 2M lines from file1, total", cntLinesFile1)
		}

		k1, err := getCompoundFieldValue(line, keyPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		p1, err := getCompoundFieldValue(line, payloadPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		linesFile1KLP[k1] = lineParts{payLoad: p1, line: line}
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading FD3:", err)
		return fmt.Errorf("failed reading FD3: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines,", len(linesFile1KLP), "are unique")

	for sc4.Scan() {
		line := sc4.Text()
		cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

		if cntLinesFile2%2_000_000 == 0 {
			vrb("read 2M lines from file2, total", cntLinesFile2)
			vrb("file1 lines", len(linesFile1KLP), "file2 lines", len(linesFile2KLP), "matched lines", cntSameLines)
		}

		k2, err := getCompoundFieldValue(line, keyPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		p2, err := getCompoundFieldValue(line, payloadPos, dataDelim)
		if err != nil {
			log.Println(err)
			return err
		}

		lp1, found := linesFile1KLP[k2]

		if found { // key found in file1
			if p2 == lp1.payLoad { // key found and payload same
				cntSameLines++
				delete(linesFile1KLP, k2)
				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			} else { // key found and payload diff
				cntNewLines++
				linesFile2KLP[k2] = lineParts{payLoad: p2, line: line}
				if outModeMerge { // dont write deletes
					delete(linesFile1KLP, k2)
				}
			}
		} else {
			cntNewLines++
			linesFile2KLP[k2] = lineParts{payLoad: p2, line: line}
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading FD4:", err)
		return fmt.Errorf("failed reading FD4: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines", cntLinesFile2, "file2 lines,")
	log.Println(cntSameLines, "matched,", len(linesFile1L), "file1 preserved", cntNewLines, "file2 preserved")

	done := make(chan error)

	go func() {
		done <- writeFile2DataF()
	}()

	go func() {
		done <- writeFile1DataF()
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

	return nil
}

func keySearchPayloadOutputBatch() error {
	return nil
}

func keySearchFullOutputBatch() error {
	return nil
}

///////////////////////////////////////////////////

// scomm reads lines from 2 files or pipes and outputs the lines which are common, the ones in first file only and the ones in the second file only
func Scomm(
	verboseParam bool,
	skipLines int,
	keyParam string,
	payloadParam string,
	dataDelimParam string,
	batchSizeParam int,
	outModeMergeParam bool,
	fullLineOutParam bool,
	discard5Param, discard6Param, discard7Param bool,
) error {

	var err error

	log.SetFlags(log.Ldate | log.Ltime)
	log.Println("start scomm")

	verbose = verboseParam
	dataDelim = dataDelimParam
	fullLineOut = fullLineOutParam
	discard5 = discard5Param
	discard6 = discard6Param
	discard7 = discard7Param
	if batchSizeParam == 0 {
		batchSize = 1_000_000 // default value
		// -1 = full mode
	}
	outModeMerge = outModeMergeParam

	// init pkg level vars in case of multiple calls to scomm
	cntLinesFile1 = 0
	cntLinesFile2 = 0
	cntSameLines = 0
	cntNewLines = 0

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
	vrb("key", keyParam)
	vrb("payload", payloadParam)
	vrb("dataDelim", dataDelim)
	vrb("batchSize", batchSize)
	vrb("outModeMerge", outModeMergeParam)
	vrb("discard5", discard5)
	vrb("discard6", discard6)
	vrb("discard7", discard7)
	vrb("fullLineOut", fullLineOut)

	if keyParam != "" && payloadParam == "" && keyParam == "" && payloadParam != "" {
		log.Println("need both key / payload parameters or none")
		return errors.New("need both key / payload parameters or none")
	}
	if keyParam == "" && payloadParam == "" {
		useKey = false
	} else {
		useKey = true
	}

	if useKey {
		keyPos, err = parseList(keyParam)

		if err != nil {
			log.Println(err)
			return err
		}

		payloadPos, err = parseList(payloadParam)

		if err != nil {
			log.Println(err)
			return err
		}
	}

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
			return errors.New("bad file descriptor 7")
		}
	}

	batchMode := batchSize > 0

	sc3 = bufio.NewScanner(file3)
	sc4 = bufio.NewScanner(file4)

	if batchMode {
		log.Println("start processing in batch mode, size", batchSize)
	} else {
		log.Println("start processing in full mode")
	}

	// read both headers to get it over with
	if skipLines > 0 {
		for i := 1; i <= skipLines; i++ {
			if sc3.Scan() {
				log.Println("ignoring file1 header", sc3.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read file1 header")
				return errors.New("unable to read file1 header")
			}
		}
		for i := 1; i <= skipLines; i++ {
			if sc4.Scan() {
				log.Println("ignoring file2 header", sc4.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read file2 header")
				return errors.New("unable to read file2 header")
			}
		}
	}

	if batchMode {
		if useKey {
			if fullLineOut {
				keySearchFullOutputBatch()
			} else {
				keySearchPayloadOutputBatch()
			}
		} else {
			lineSearchBatch()
		}
	} else {
		if useKey {
			if fullLineOut {
				keySearchFullOutput()
			} else {
				keySearchPayloadOutput()
			}
		} else {
			lineSearch()
		}
	}

	ts2 := time.Now()
	// if profile {
	// pprof.StopCPUProfile()
	// }

	log.Println("end scomm, time taken", math.Ceil(ts2.Sub(ts1).Seconds()), "sec")
	return nil
}

func writeFile2DataKP() error {
	log.Println("write newFile2DataOut")
	for k, p := range linesFile2KP {
		_, err := file6.WriteString(k + dataDelim + p + "\n")
		if err != nil {
			log.Println("failed to write to FD6:", err)
			return fmt.Errorf("failed to write to FD6: %v", err)
		}
	}
	log.Println("wrote file2 data output")
	return nil
}

func writeFile1DataKP() error {
	log.Println("write newFile1DataOut")
	for k, p := range linesFile1KP {
		_, err := file5.WriteString(k + dataDelim + p + "\n")
		if err != nil {
			log.Println("failed to write to FD5:", err)
			return fmt.Errorf("failed to write to FD5: %v", err)
		}
	}
	log.Println("wrote file1 data output")
	return nil
}

func writeFile2DataL() error {
	log.Println("write newFile2DataOut")
	for line := range linesFile2L {
		_, err := file6.WriteString(line + "\n")
		if err != nil {
			log.Println("failed to write to FD6:", err)
			return fmt.Errorf("failed to write to FD6: %v", err)
		}
	}
	log.Println("wrote file2 data output")
	return nil
}

func writeFile1DataL() error {
	log.Println("write newFile1DataOut")
	for line := range linesFile1L {
		_, err := file5.WriteString(line + "\n")
		if err != nil {
			log.Println("failed to write to FD5:", err)
			return fmt.Errorf("failed to write to FD5: %v", err)
		}
	}
	log.Println("wrote file1 data output")
	return nil
}

func writeFile2DataF() error {
	log.Println("write newFile2DataOut")
	for _, lp := range linesFile2KLP {
		_, err := file6.WriteString(lp.line + "\n")
		if err != nil {
			log.Println("failed to write to FD6:", err)
			return fmt.Errorf("failed to write to FD6: %v", err)
		}
	}
	log.Println("wrote file2 data output")
	return nil
}

func writeFile1DataF() error {
	log.Println("write newFile1DataOut")
	for _, lp := range linesFile1KLP {
		_, err := file5.WriteString(lp.line + "\n")
		if err != nil {
			log.Println("failed to write to FD5:", err)
			return fmt.Errorf("failed to write to FD5: %v", err)
		}
	}
	log.Println("wrote file1 data output")
	return nil
}

func vrb(params ...interface{}) {
	if verbose {
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
// works with files only, no "Real" process substitution :((
// for now, I will get errors when I really read from the file, and will have to deal with them at that point
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

func percentage(n1, n2 int) string {
	if n2 == 0 {
		return "0%"
	}
	return fmt.Sprintf("%.2f%%", float64(n1)*100/float64(n2))
}
