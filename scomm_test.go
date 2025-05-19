package scomm

import (
	"fmt"
	"testing"
)

func TestDbg(t *testing.T) {
	dbg("first test")
}

func TestParseItem(t *testing.T) {

	data := []struct {
		in string
		// out string
		ok bool
	}{
		{"1", true},
		{"-2", true},
		{"3-", true},
		{"5-8", true},
		{"", false},
		{"-", true},
		{"1.2", false},
		{"a", false},
		{"0", false},
		{"2-1", false},
		{"2--", false},
		{"5-8.4", false},
	}

	for _, v := range data {
		fmt.Print(v.in, " => ")
		x, err := parseItem(v.in)
		fmt.Println(x, err)
		if v.ok == (err != nil) {
			t.Error(err)
		}
	}
}

func TestParseList(t *testing.T) {
	data := []struct {
		in string
		ok bool
	}{
		{"1", true},
		{"1,2", true},
		{"1-3,5-9", true},
		{"1-3,5-", true},
		{"", false},
		{"-", true},
		{",,", false},
		{",", false},
		{"1,2,3", true},
	}

	for _, v := range data {
		fmt.Print(v.in, " => ")
		x, err := parseList(v.in)
		fmt.Println(x, err)
		if v.ok == (err != nil) {
			fmt.Println("FAILED ^^^")
			t.Error(err)
		}
	}
}

func TestVrb(t *testing.T) {
	Verbose = true
	vrb("test verbose")
}

func TestGetCompoundField(t *testing.T) {
	data := []struct {
		line     string
		pos      [][2]int
		delim    string
		ok       bool
		expected string
	}{
		{"1234567890", [][2]int{{1, 1}}, "", true, "1"},
		{"1234567890", [][2]int{{2, 2}}, "", true, "2"},
		{"1234567890", [][2]int{{2, 4}}, "", true, "234"},
		{"1234567890", [][2]int{{2, 4}, {6}}, "", true, "234"},
		{"1234567890", [][2]int{{2, 4}, {6, 9}}, "", true, "2346789"},
		{"1234567890", [][2]int{{2, 4}, {6, 0}}, "", true, "23467890"},
		{"A1A2,A3A4A5,A6A7,A8A9A0,B1,B2B3,B4B5B6,B7B8B9B0", [][2]int{{2, 2}}, ",", true, "A3A4A5"},
		{"A1A2,A3A4A5,A6A7,A8A9A0,B1,B2B3,B4B5B6,B7B8B9B0", [][2]int{{2, 2}, {4, 4}}, ",", true, "A3A4A5,A8A9A0"},
		{"A1A2,A3A4A5,A6A7,A8A9A0,B1,B2B3,B4B5B6,B7B8B9B0", [][2]int{{5, 0}}, ",", true, "B1,B2B3,B4B5B6,B7B8B9B0"},
		{"A1A2,A3A4A5,A6A7,A8A9A0,B1,B2B3,B4B5B6,B7B8B9B0", [][2]int{{5, 0}, {5, 0}}, ",", true, "B1,B2B3,B4B5B6,B7B8B9B0,B1,B2B3,B4B5B6,B7B8B9B0"},
		{"A1A2,A3A4A5,A6A7,A8A9A0,B1,B2B3,B4B5B6,B7B8B9B0", [][2]int{{9, 9}}, ",", false, ""},
		{"A1A2,A3A4A5,A6A7,A8A9A0,B1,B2B3,B4B5B6,B7B8B9B0", [][2]int{{8, 8}}, ",", true, "B7B8B9B0"},
	}

	for _, v := range data {
		fmt.Println(v.line)
		fmt.Print(v.pos, " => ")
		keyval, err := getCompoundField(v.line, v.pos, v.delim)
		if err != nil {
			fmt.Println(err, "     ERROR")
			continue
		}
		fmt.Print(keyval)
		if (v.ok && keyval == v.expected) || (!v.ok && keyval != v.expected) {
			fmt.Println("     OK")
		} else {
			fmt.Println("     ERROR")
		}
		// if v.ok == (err != nil) {
		// 	fmt.Println("FAILED ^^^")
		// 	t.Error(err)
		// }
	}
}
