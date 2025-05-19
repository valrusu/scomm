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
