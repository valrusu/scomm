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

const (
	MAPSIZE   = 10_000_000
	BATCHSIZE = 1_000_000
	STATSINT  = 2_000_000
)

var (
	cntLinesFile1, cntLinesFile2, cntSameLines, cntNewLines, updatedTags int

	linesFile1KP                         map[string]string // used for key compare, key+payload output
	linesFile2KP                         map[string]string
	linesFile1KL                         map[string]lineParts // used for key compare, full line output
	linesFile2KL                         map[string]lineParts
	linesFile1LL                         map[string]struct{} // used for line compare, full line output
	linesFile2LL                         map[string]struct{}
	file3, file4, file5, file6, file7    *os.File
	verbose                              bool
	batchSize                            int
	useKey, fullLineOutput, outModeMerge bool
	keyPos, payloadPos                   [][2]int
	dataDelim                            string

	fd3ok, fd4ok, fd5ok, fd6ok, fd7ok bool
	sc3, sc4                          *bufio.Scanner
	discard5, discard6, discard7      bool
)

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
// a compound field is defined as a LIST, like 1 2,3 4-5 6- -7 and comma separated combinations (similar to the cut Linux command)
// if the delimiter is set, then it will extract delimited delimited fields, and return them separated by the same delimiter
// if the delimiter is not set, it will extract characters based on fixed width, and the return will be width based too
// see scomm_test.go for examples
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
			if y > len(line) {
				strerr := fmt.Sprintf("invalid data: %s for pattern %v without delimiters", line, pos)
				log.Println(strerr)
				return "", errors.New(strerr)
			}
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

func lineMatchLineOutput() error {
	vrb("lineMatchLineOutput: allocate memory")
	linesFile1LL = make(map[string]struct{}, MAPSIZE*10)
	linesFile2LL = make(map[string]struct{}, MAPSIZE/5)

	for sc3.Scan() {
		line := sc3.Text()
		cntLinesFile1++

		linesFile1LL[line] = struct{}{}

		if cntLinesFile1%STATSINT == 0 {
			vrb("read 2M lines from file1, total", cntLinesFile1, "unique", len(linesFile1LL))
		}
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading FD3:", err)
		return fmt.Errorf("failed reading FD3: %v", err)
	}

	vrb("read", cntLinesFile1, "lines from file1, unique", len(linesFile1LL))

	for sc4.Scan() {
		line := sc4.Text()
		cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

		_, found := linesFile1LL[line]

		if found {
			cntSameLines++
			delete(linesFile1LL, line)

			if !discard7 {
				if _, err := file7.WriteString(line + "\n"); err != nil {
					log.Println("failed to write to FD7:", err)
					return fmt.Errorf("failed to write to FD7: %v", err)
				}
			}
		} else {
			// cntNewLines++
			linesFile2LL[line] = struct{}{}
		}

		if cntLinesFile2%STATSINT == 0 {
			vrb("read 2M lines from file2, total", cntLinesFile2)
			// loop stats
			vrb(
				"file1 kept", len(linesFile1LL),
				"file2 kept", len(linesFile2LL),
				"matched", cntSameLines,
			)
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading FD4:", err)
		return fmt.Errorf("failed reading FD4: %v", err)
	}

	if verbose {
		fmt.Println("File1: total", cntLinesFile1, "kept", len(linesFile1LL), percentage(len(linesFile1LL), cntLinesFile1))
		fmt.Println("File2: total", cntLinesFile2, "kept", len(linesFile2LL), percentage(len(linesFile2LL), cntLinesFile2))
		fmt.Println("Common:", cntSameLines, percentage(cntSameLines, cntLinesFile1), percentage(cntSameLines, cntLinesFile2))
	} else {
		log.Println("File1: total", cntLinesFile1, "kept", len(linesFile1LL), percentage(len(linesFile1LL), cntLinesFile1))
		log.Println("File2: total", cntLinesFile2, "kept", len(linesFile2LL), percentage(len(linesFile2LL), cntLinesFile2))
		log.Println("Common:", cntSameLines, percentage(cntSameLines, cntLinesFile1), percentage(cntSameLines, cntLinesFile2))
	}

	done := make(chan error)
	waitFor := 0

	if !discard5 {
		go func() {
			done <- writeFile1DataL()
		}()
		waitFor++
	}

	if !discard6 {
		go func() {
			done <- writeFile2DataL()
		}()
		waitFor++
	}

	for i := 1; i <= waitFor; i++ {
		err := <-done

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func lineSearchLineOutputBatch() error {
	vrb("lineMatchLineOutputBatch: batchsize", batchSize)

	linesFile1LL = make(map[string]struct{}, MAPSIZE)
	linesFile2LL = make(map[string]struct{}, MAPSIZE/5)
	var loopAgain bool

	// read alternatively file1 and file2 until BOTH are done
	for {
		loopAgain = false

		// read max batchSize lines from file1
		for sc3.Scan() {
			loopAgain = true // at least one line read, will loop again
			line := sc3.Text()
			cntLinesFile1++

			linesFile1LL[line] = struct{}{}

			if batchSize > STATSINT && cntLinesFile1%STATSINT == 0 {
				vrb("read 2M lines from file1, total", cntLinesFile1)
			}

			if cntLinesFile1%batchSize == 0 {
				break
			}
		}

		if err := sc3.Err(); err != nil {
			log.Println("failed reading FD3:", err)
			return fmt.Errorf("failed reading FD3: %v", err)
		}

		// loop stats
		vrb(
			"file1", cntLinesFile1, len(linesFile1LL),
			"file2", cntLinesFile2, len(linesFile2LL),
			"matched", cntSameLines,
		)

		// check existing file2 lines (read in previous loop) against the file1 lines, just read plus eventual old ones
		for line, _ := range linesFile2LL { // initially will be empty, then it will accumulate data
			_, found := linesFile1LL[line]

			if found {
				cntSameLines++
				delete(linesFile1LL, line)
				delete(linesFile2LL, line)

				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			}
		}
		if len(linesFile2LL) > 0 { // or cntLinesFile2 ?
			// loop stats
			vrb(
				"file1", cntLinesFile1, len(linesFile1LL),
				"file2", cntLinesFile2, len(linesFile2LL),
				"matched", cntSameLines,
			)
		}

		// now read from file2, checking against file1
		for sc4.Scan() {
			loopAgain = true // at least one line read, will loop again
			line := sc4.Text()
			cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

			if batchSize > STATSINT && cntLinesFile2%STATSINT == 0 {
				vrb("read 2M lines from file2, total", cntLinesFile2)
			}

			_, found := linesFile1LL[line]

			if found {
				cntSameLines++
				delete(linesFile1LL, line)

				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			} else {
				cntNewLines++
				linesFile2LL[line] = struct{}{}
			}

			if cntLinesFile2%batchSize == 0 {
				break
			}
		}
		// loop stats
		vrb(
			"file1", cntLinesFile1, len(linesFile1LL),
			"file2", cntLinesFile2, len(linesFile2LL),
			"matched", cntSameLines,
		)

		if err := sc4.Err(); err != nil {
			log.Println("failed reading FD4:", err)
			return fmt.Errorf("failed reading FD4: %v", err)
		}

		if !loopAgain {
			break
		}
	}

	if verbose {
		fmt.Println("File1: total", cntLinesFile1, "kept", len(linesFile1LL), percentage(len(linesFile1LL), cntLinesFile1))
		fmt.Println("File2: total", cntLinesFile2, "kept", len(linesFile2LL), percentage(len(linesFile2LL), cntLinesFile2))
		fmt.Println("Common:", cntSameLines, percentage(cntSameLines, cntLinesFile1), percentage(cntSameLines, cntLinesFile2))
	} else {
		log.Println("File1: total", cntLinesFile1, "kept", len(linesFile1LL), percentage(len(linesFile1LL), cntLinesFile1))
		log.Println("File2: total", cntLinesFile2, "kept", len(linesFile2LL), percentage(len(linesFile2LL), cntLinesFile2))
		log.Println("Common:", cntSameLines, percentage(cntSameLines, cntLinesFile1), percentage(cntSameLines, cntLinesFile2))
	}

	done := make(chan error)
	waitFor := 0

	if !discard5 {
		go func() {
			done <- writeFile1DataL()
		}()
		waitFor++
	}

	if !discard6 {
		go func() {
			done <- writeFile2DataL()
		}()
		waitFor++
	}

	for i := 1; i <= waitFor; i++ {
		err := <-done

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func keyMatchPayloadOutput() error {
	vrb("keySearchPayloadOutput: allocate memory")
	linesFile1KP = make(map[string]string, 100_000_000)
	linesFile2KP = make(map[string]string, 100_000_000)

	for sc3.Scan() {
		line := sc3.Text()
		cntLinesFile1++

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

		if cntLinesFile1%STATSINT == 0 {
			vrb("read 2M lines from file1, total", cntLinesFile1)
		}
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading FD3:", err)
		return fmt.Errorf("failed reading FD3: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines,", len(linesFile1KP), "are unique")

	for sc4.Scan() {
		line := sc4.Text()
		cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

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

		if cntLinesFile2%STATSINT == 0 {
			vrb("read 2M lines from file2, total", cntLinesFile2)
			vrb("file1 lines", len(linesFile1KP), "file2 lines", len(linesFile2KL), "matched lines", cntSameLines)
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading FD4:", err)
		return fmt.Errorf("failed reading FD4: %v", err)
	}

	log.Println("read", cntLinesFile1, "file1 lines", cntLinesFile2, "file2 lines,")
	log.Println(cntSameLines, "matched,", len(linesFile1LL), "file1 preserved", cntNewLines, "file2 preserved")

	done := make(chan error)
	waitFor := 0

	if !discard5 {
		go func() {
			done <- writeFile1DataKP()
		}()
		waitFor++
	}

	if !discard6 {
		go func() {
			done <- writeFile2DataKP()
		}()
		waitFor++
	}

	for i := 1; i <= waitFor; i++ {
		err := <-done

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func keyMatchLineOutput() error {
	vrb("keySearchLineOutput: allocate memory")
	linesFile1KL = make(map[string]lineParts, MAPSIZE*5)
	linesFile2KL = make(map[string]lineParts, MAPSIZE/2)

	for sc3.Scan() {
		line := sc3.Text()
		cntLinesFile1++

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

		linesFile1KL[k1] = lineParts{payLoad: p1, line: line}

		if cntLinesFile1%STATSINT == 0 {
			vrb("read 2M lines from file1, total", cntLinesFile1, "unique keys", len(linesFile1KL))
		}
	}

	if err := sc3.Err(); err != nil {
		log.Println("failed reading FD3:", err)
		return fmt.Errorf("failed reading FD3: %v", err)
	}

	vrb("read", cntLinesFile1, "lines from file1, unique keys", len(linesFile1KL))

	for sc4.Scan() {
		line := sc4.Text()
		cntLinesFile2++ // keep a count of lines read regardless if they existed in FILE1

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

		lp1, found := linesFile1KL[k2]

		if found { // key2 found in file1
			if p2 == lp1.payLoad { // key found and payload same
				cntSameLines++
				delete(linesFile1KL, k2)

				if !discard7 {
					if _, err := file7.WriteString(line + "\n"); err != nil {
						log.Println("failed to write to FD7:", err)
						return fmt.Errorf("failed to write to FD7: %v", err)
					}
				}
			} else { // key found and payload diff
				cntNewLines++
				linesFile2KL[k2] = lineParts{payLoad: p2, line: line}

				if outModeMerge { // dont save deletes
					delete(linesFile1KL, k2)
				}
			}
		} else { // key2 not found in file1
			cntNewLines++
			linesFile2KL[k2] = lineParts{payLoad: p2, line: line}
		}

		if cntLinesFile2%STATSINT == 0 {
			vrb("read 2M lines from file2, total", cntLinesFile2)
			vrb("file1 lines", len(linesFile1KL), "file2 lines", len(linesFile2KL), "matched lines", cntSameLines)
			// loop stats - TODO make this a vrb call
			vrb(
				"file1 kept", len(linesFile1KL),
				"file2 kept", len(linesFile2KL),
				"matched", cntSameLines,
			)
		}
	}

	if err := sc4.Err(); err != nil {
		log.Println("failed reading FD4:", err)
		return fmt.Errorf("failed reading FD4: %v", err)
	}

	if verbose {
		fmt.Println("File1: total", cntLinesFile1, "kept", len(linesFile1KL), percentage(len(linesFile1KL), cntLinesFile1))
		fmt.Println("File2: total", cntLinesFile2, "kept", len(linesFile2KL), percentage(len(linesFile2KL), cntLinesFile2))
		fmt.Println("Common:", cntSameLines, percentage(cntSameLines, cntLinesFile1), percentage(cntSameLines, cntLinesFile2))
	} else {
		log.Println("File1: total", cntLinesFile1, "kept", len(linesFile1KL), percentage(len(linesFile1KL), cntLinesFile1))
		log.Println("File2: total", cntLinesFile2, "kept", len(linesFile2KL), percentage(len(linesFile2KL), cntLinesFile2))
		log.Println("Common:", cntSameLines, percentage(cntSameLines, cntLinesFile1), percentage(cntSameLines, cntLinesFile2))
	}

	done := make(chan error)
	waitFor := 0

	if !discard5 {
		go func() {
			done <- writeFile1DataF()
		}()
		waitFor++
	}

	if !discard6 {
		go func() {
			done <- writeFile2DataF()
		}()
		waitFor++
	}

	for i := 1; i <= waitFor; i++ {
		err := <-done

		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func keySearchPayloadOutputBatch() error {
	return errors.New("sorry, not implemented yet")
}

func keySearchFullOutputBatch() error {
	return errors.New("sorry, not implemented yet")
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
	fullLineOutputParam bool,
	discard5Param, discard6Param, discard7Param bool,
) error {

	var err error

	log.SetFlags(log.Ldate | log.Ltime)

	verbose = verboseParam
	dataDelim = dataDelimParam
	fullLineOutput = fullLineOutputParam
	discard5 = discard5Param
	discard6 = discard6Param
	discard7 = discard7Param
	if batchSizeParam == 0 {
		batchSize = BATCHSIZE // default value
	} else {
		batchSize = batchSizeParam
		// -1 = full mode
	}
	outModeMerge = outModeMergeParam

	vrb("start scomm")

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
	vrb("fullLineOutput", fullLineOutput)

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

	sc3 = bufio.NewScanner(file3)
	sc4 = bufio.NewScanner(file4)

	// read and skip both headers to get it over with
	if skipLines > 0 {
		for i := 1; i <= skipLines; i++ {
			if sc3.Scan() {
				vrb("ignoring file1 header", sc3.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read file1 header")
				return errors.New("unable to read file1 header")
			}
		}
		for i := 1; i <= skipLines; i++ {
			if sc4.Scan() {
				vrb("ignoring file2 header", sc4.Text())
			} else {
				// unable to even read one line, and header was specified - problem
				log.Println("unable to read file2 header")
				return errors.New("unable to read file2 header")
			}
		}
	}

	switch {
	case batchSize > 0 && useKey && fullLineOutput:
		keySearchFullOutputBatch()
	case batchSize > 0 && useKey && !fullLineOutput:
		keySearchPayloadOutputBatch()
	case batchSize > 0 && !useKey:
		lineSearchLineOutputBatch()
	case batchSize <= 0 && useKey && fullLineOutput:
		keyMatchLineOutput()
	case batchSize <= 0 && useKey && !fullLineOutput:
		keyMatchPayloadOutput()
	case batchSize <= 0 && !useKey:
		lineMatchLineOutput()
	default:
		log.Println("huh???")
		return errors.New("impossible")
	}

	ts2 := time.Now()
	// if profile {
	// pprof.StopCPUProfile()
	// }

	log.Println("End scomm, time taken", math.Ceil(ts2.Sub(ts1).Seconds()), "sec")
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
	vrb("wrote file2 data output")
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
	vrb("wrote file1 data output")
	return nil
}

func writeFile1DataL() error {
	vrb("write newFile1DataOut")
	for line := range linesFile1LL {
		_, err := file5.WriteString(line + "\n")
		if err != nil {
			log.Println("failed to write to FD5:", err)
			return fmt.Errorf("failed to write to FD5: %v", err)
		}
	}
	vrb("wrote file1 data output")
	return nil
}

func writeFile2DataL() error {
	vrb("write newFile2DataOut")
	for line := range linesFile2LL {
		_, err := file6.WriteString(line + "\n")
		if err != nil {
			log.Println("failed to write to FD6:", err)
			return fmt.Errorf("failed to write to FD6: %v", err)
		}
	}
	vrb("wrote file2 data output")
	return nil
}

func writeFile2DataF() error {
	log.Println("write newFile2DataOut")
	for _, lp := range linesFile2KL {
		_, err := file6.WriteString(lp.line + "\n")
		if err != nil {
			log.Println("failed to write to FD6:", err)
			return fmt.Errorf("failed to write to FD6: %v", err)
		}
	}
	vrb("wrote file2 data output")
	return nil
}

func writeFile1DataF() error {
	log.Println("write newFile1DataOut")
	for _, lp := range linesFile1KL {
		_, err := file5.WriteString(lp.line + "\n")
		if err != nil {
			log.Println("failed to write to FD5:", err)
			return fmt.Errorf("failed to write to FD5: %v", err)
		}
	}
	vrb("wrote file1 data output")
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
	return fmt.Sprintf("%.4f%%", float64(n1)*100/float64(n2))
}
