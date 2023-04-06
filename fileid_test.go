package wal

import (
	"math/rand"
	"testing"
)

func TestIdGenerator(t *testing.T) {
	t.Run("rand/sametime", func(tt *testing.T) {
		gen := newIdGenerator(rand.NewSource(1))
		r1 := gen.rand(0)
		r2 := gen.rand(0)
		tt.Logf("1,2 = %d, %d", r1, r2)

		if r1 != r2 {
			tt.Errorf("r1 == r2")
		}
	})
	t.Run("rand/difftime", func(tt *testing.T) {
		gen := newIdGenerator(rand.NewSource(1))
		r1 := gen.rand(0)
		r2 := gen.rand(1)
		tt.Logf("1,2 = %d, %d", r1, r2)

		if r1 == r2 {
			tt.Errorf("r1 != r2")
		}
	})
	t.Run("next", func(tt *testing.T) {
		gen := newIdGenerator(rand.NewSource(1))
		idt1, idr1 := gen.Next()
		idt2, idr2 := gen.Next()
		idt3, idr3 := gen.Next()
		if (idt1 <= idt2 && idr1 < idr2) != true {
			t.Errorf("id1('%s') is less than id2('%s')", CreateFileID(idt1, idr1), CreateFileID(idt2, idr2))
		}
		if (idt2 <= idt3 && idr2 < idr3) != true {
			t.Errorf("id2('%s') is less than id3('%s')", CreateFileID(idt2, idr2), CreateFileID(idt3, idr3))
		}
		t.Logf("next=%s", CreateFileID(idt1, idr1))
		t.Logf("next=%s", CreateFileID(idt2, idr2))
		t.Logf("next=%s", CreateFileID(idt3, idr3))
	})
}

func TestNextFileID(t *testing.T) {
	id1 := NextFileID()
	id2 := NextFileID()
	id3 := NextFileID()

	if id1.Newer(id2) != true {
		t.Errorf("id1('%s') is less than id2('%s')", id1, id2)
	}
	if id2.Newer(id3) != true {
		t.Errorf("id2('%s') is less than id3('%s')", id2, id3)
	}
	t.Logf("%s", id1)
	t.Logf("%s", id2)
	t.Logf("%s", id3)
}

func TestFileID(t *testing.T) {
	t.Run("equal", func(tt *testing.T) {
		testcase := []struct {
			a, b   FileID
			expect bool
		}{
			{
				a:      FileID{0, 0},
				b:      FileID{0, 0},
				expect: true,
			},
			{
				a:      FileID{0, 0},
				b:      FileID{0, 1},
				expect: false,
			},
			{
				a:      FileID{0, 1},
				b:      FileID{0, 0},
				expect: false,
			},
			{
				a:      FileID{0x3fff_ffff_ffff_ffff, 0x3fff_ffff_ffff_ffff},
				b:      FileID{0x3fff_ffff_ffff_ffff, 0x3fff_ffff_ffff_ffff},
				expect: true,
			},
		}
		for id, tc := range testcase {
			if actual := tc.a.Equal(tc.b); tc.expect != actual {
				tt.Errorf("[%d] a:%s equal b:%s expect:%v actual:%v", id, tc.a, tc.b, tc.expect, actual)
			}
		}
	})
	t.Run("newer", func(tt *testing.T) {
		testcase := []struct {
			a, b   FileID
			name   string
			expect bool
		}{
			{
				a:      FileID{0, 0},
				b:      FileID{0, 0},
				expect: false,
			},
			{
				a:      FileID{0, 0},
				b:      FileID{0, 1},
				expect: true,
			},
			{
				a:      FileID{0, 1},
				b:      FileID{0, 2},
				expect: true,
			},
			{
				a:      FileID{0, 0},
				b:      FileID{1, 0},
				expect: true,
			},
			{
				a:      FileID{0, 1},
				b:      FileID{1, 0},
				expect: true,
			},
			{
				a:      FileID{1, 1},
				b:      FileID{1, 0},
				expect: false,
			},
			{
				a:      FileID{12, 1},
				b:      FileID{18, 0},
				expect: true,
			},
		}
		for id, tc := range testcase {
			if actual := tc.a.Newer(tc.b); tc.expect != actual {
				tt.Errorf("[%d] a:%s newer b:%s expect:%v actual:%v", id, tc.a, tc.b, tc.expect, actual)
			}
		}
	})
}

func TestCreateFileID(t *testing.T) {
	testcase := []struct {
		t, r int64
	}{
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{0x3fff_ffff_ffff_ffff, 0x3fff_ffff_ffff_ffff},
		{0, 0x3fff_ffff_ffff_ffff},
		{0x3fff_ffff_ffff_ffff, 0},
		{128, 0},
		{1234567890, 0},
		{0, 64},
		{0, 1234567890},
	}
	for _, s := range testcase {
		a := CreateFileID(s.t, s.r)
		if len(a.String()) != defaultIdFormatLen {
			t.Errorf("%s is not len %d", a, defaultIdFormatLen)
		}
		t.Logf("%s", a)
	}
}

func TestParseFileID(t *testing.T) {
	testcase := []struct {
		filename string
		expect   bool
	}{
		{"0000000000000000-0000000000000000", true},
		{"0000000000000000-0000000000000001", true},
		{"0000000000000001-0000000000000000", true},
		{"3fffffffffffffff-3fffffffffffffff", true},
		{"3fffffffffffffff-0000000000000000", true},
		{"0000000000000000-3fffffffffffffff", true},
		{"0000000000000001-3fffffffffffffff", true},
		{"0000000000000001@3fffffffffffffff", false},
		{"0000000000000001@3fffffffffffffff", false},
		{"fffffffffffffffff0000000000000000", false},
		{"zyxwvfffffffffff-0000000000000000", false},
		{"!yxwvfffffffffff-0000000000000000", false},
		{"0000000000000000-zyx0000000000000", false},
		{"3fffffffffffffff-!#00000000000000", false},
	}
	for _, tc := range testcase {
		if _, actual := ParseFileID(tc.filename); tc.expect != actual {
			t.Errorf("%s expect:%v actual:%v", tc.filename, tc.expect, actual)
		}
	}
}
