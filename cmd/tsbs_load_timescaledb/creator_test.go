package main

import (
	"bufio"
	"bytes"
	"log"
	"testing"
)

func TestDBCreatorReadDataHeader(t *testing.T) {
	cases := []struct {
		desc         string
		input        string
		wantTags     string
		wantCols     []string
		wantBuffered int
		shouldFatal  bool
	}{
		{
			desc:         "min case: exactly three lines",
			input:        "tags,tag1,tag2\ncols,col1,col2\n\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2"},
			wantBuffered: 0,
		},
		{
			desc:         "min case: more than the header 3 lines",
			input:        "tags,tag1,tag2\ncols,col1,col2\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2"},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:         "multiple tables: more than 3 lines for header",
			input:        "tags,tag1,tag2\ncols,col1,col2\ncols2,col21,col22\n\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2", "cols2,col21,col22"},
			wantBuffered: 0,
		},
		{
			desc:         "multiple tables: more than 3 lines for header w/ extra",
			input:        "tags,tag1,tag2\ncols,col1,col2\ncols2,col21,col22\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2", "cols2,col21,col22"},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:        "too few lines",
			input:       "tags\ncols\n",
			shouldFatal: true,
		},
		{
			desc:        "no line ender",
			input:       "tags",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dbc.readDataHeader(br)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			dbc.readDataHeader(br)
			if dbc.tags != c.wantTags {
				t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s", c.desc, dbc.tags, c.wantTags)
			}
			if len(dbc.cols) != len(c.wantCols) {
				t.Errorf("%s: incorrect cols len: got %d want %d", c.desc, len(dbc.cols), len(c.wantCols))
			}
			for i := range dbc.cols {
				if got := dbc.cols[i]; got != c.wantCols[i] {
					t.Errorf("%s: cols row %d incorrect: got\n%s\nwant\n%s\n", c.desc, i, got, c.wantCols[i])
				}
			}
			if br.Buffered() != c.wantBuffered {
				t.Errorf("%s: incorrect amt buffered: got\n%d\nwant\n%d", c.desc, br.Buffered(), c.wantBuffered)
			}
		}
	}
}

func TestDBCreatorGetCreateIndexOnFieldSQL(t *testing.T) {
	hypertable := "htable"
	field := "foo"
	valueTime := "CREATE INDEX ON htable (foo, time DESC)"
	timeValue := "CREATE INDEX ON htable (time DESC, foo)"
	cases := []struct {
		desc        string
		idxType     string
		want        []string
		shouldFatal bool
	}{
		{
			desc:    "no indexes",
			idxType: "",
			want:    []string{},
		},
		{
			desc:    "single TIME-VALUE index",
			idxType: timeValueIdx,
			want:    []string{timeValue},
		},
		{
			desc:    "single VALUE-TIME index",
			idxType: valueTimeIdx,
			want:    []string{valueTime},
		},
		{
			desc:    "two indexes",
			idxType: timeValueIdx + "," + valueTimeIdx,
			want:    []string{timeValue, valueTime},
		},
		{
			desc:        "bad idxType",
			idxType:     "baz",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dbc.getCreateIndexOnFieldCmds(hypertable, field, c.idxType)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			cmds := dbc.getCreateIndexOnFieldCmds(hypertable, field, c.idxType)
			if len(cmds) != len(c.want) {
				t.Errorf("%s: incorrect cmds length: got %d want %d", c.desc, len(cmds), len(c.want))
			}
			for i, cmd := range cmds {
				if cmd != c.want[i] {
					t.Errorf("%s: incorrect cmd at idx %d: got %s want %s", c.desc, i, cmd, c.want[i])
				}
			}
		}
	}
}
