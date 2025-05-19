package scomm

import (
	"fmt"
	"testing"
)

func TestDbg(t *testing.T) {
	dbg("first test")
}

func TestParseItem(t *testing.T) {

	x, err := parseItem("1")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("-2")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("3-")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("5-8")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

	x, err = parseItem("1.2")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

	x, err = parseItem("a")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

	x, err = parseItem("0")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

	x, err = parseItem("2-1")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

	x, err = parseItem("2--")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

	x, err = parseItem("5-8.4")
	if err == nil {
		t.Error(err)
	}
	fmt.Println(err)

}

func TestParseList(t *testing.T) {
	t1 := []string{"1", "1,2", "1-3,5-9", "1-3,5-"}
	t2 := []string{"-"}

	for _, v := range t1 {
		fmt.Print(v, " => ")
		x, err := parseList(v)
		if err != nil {
			t.Error(err)
		}
		fmt.Println(x)
	}

	for _, v := range t2 {
		fmt.Print(v, " => ")
		x, err := parseList(v)
		if err == nil {
			t.Error(err)
		}
		fmt.Println(x)
	}
}
