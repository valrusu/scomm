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
	fmt.Print(x, " => ")
	if err != nil {
		t.Error(err)
	}
	if x == [2]int{1,1} {
		fmt.Println(x, "ok")
	}
	
	x, err = parseItem("-2")
	fmt.Print(x, " => ")
	if err != nil {
		t.Error(err)
	}
	if x == [2]int{0,2} {
		fmt.Println(x, "ok")
	}

	x, err = parseItem("3-")
	fmt.Print(x, " => ")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("5-8")
	fmt.Print(x, " => ")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("1.2")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("a")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("0")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("2-1")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("2--")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("5-8.4")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)

	x, err = parseItem("-")
	fmt.Print(x, " => ")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)

	x, err = parseItem("4-2")
	fmt.Print(x, " => ")
	if err == nil {
		t.Error()
	}
	fmt.Println(err)
}

func TestParseList(t *testing.T) {
	goodones := []string{"1", "1,2", "1-3,5-9", "1-3,5-","-"}
	badones := []string{"4-2"}

	for _, v := range goodones {
		fmt.Print(v, " => ")
		x, err := parseList(v)
		if err != nil {
			t.Error(err)
		}
		fmt.Println(x)
	}

	for _, v := range badones {
		fmt.Print(v, " => ")
		x, err := parseList(v)
		if err == nil {
			t.Error("failed")
		}
		fmt.Println(x)
	}
}
