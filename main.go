package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

var clean bool
var merge string

func main() {
	flag.BoolVar(&clean, "clean", false, "Drop tables before recreating them")
	flag.StringVar(&merge, "merge", "", "Attempt to merge all imported data into this table")
	flag.Parse()
	sqlFiles := []string{}
	for _, f := range flag.Args() {
		err := handle(f)
		if err != nil {
			log.Printf("Failed to handle %s: %s", f, err)
		} else {
			sqlFiles = append(sqlFiles, strings.TrimSuffix(f, ".csv")+".sql")
		}
	}
	all, err := os.Create("alltables.sql")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = fmt.Fprintf(all, "-- -*-sql-*-\n")
	for _, file := range sqlFiles {
		_, _ = fmt.Fprintf(all, "\\i '%s'\n", file)
	}
	if merge != "" {
		if clean {
			_, _ = fmt.Fprintf(all, "drop table if exists %s;\n", merge)
		}
		selects := make([]string, len(sqlFiles))
		for i, t := range sqlFiles {
			selects[i] = fmt.Sprintf("select * from %s\n", slug(strings.TrimSuffix(t, ".sql")))
		}
		_, _ = fmt.Fprintf(all, "create table %s as\n%s;", merge, strings.Join(selects, "union\n"))
	}

	_ = all.Close()
}

type fieldType struct {
	Date     bool
	Int      bool
	Float    bool
	Percent  bool
	Empty    bool
	NonEmpty bool
}

func newFieldType() *fieldType {
	return &fieldType{
		Date:     true,
		Int:      true,
		Float:    true,
		Percent:  true,
		Empty:    false,
		NonEmpty: false,
	}
}

func (f fieldType) Parse(s string) (string, error) {
	if s == "#DIV/0!" {
		s = ""
	}
	if s == "" {
		return s, nil
	}
	switch {
	case f.Date:
		return parseDate(s)
	case f.Int:
		return parseInt(s)
	case f.Float:
		return parseFloat(s)
	case f.Percent:
		return parsePercent(s)
	default:
		return s, nil
	}
}

func (f *fieldType) Check(s string) {
	if s == "#DIV/0!" {
		s = ""
	}
	if s == "" {
		f.Empty = true
		return
	} else {
		f.NonEmpty = true
	}
	if f.Date {
		_, err := parseDate(s)
		if err != nil {
			f.Date = false
		}
	}
	if f.Int {
		_, err := parseInt(s)
		if err != nil {
			f.Int = false
		}
	}
	if f.Float {
		_, err := parseFloat(s)
		if err != nil {
			f.Float = false
		}
	}
	if f.Percent {
		_, err := parsePercent(s)
		if err != nil {
			f.Percent = false
		}
	}
}

func (f fieldType) SqlType() string {
	mod := ""
	if !f.Empty {
		mod = " not null"
	}
	if f.Date {
		return "timestamptz" + mod
	}
	if f.Int {
		return "integer" + mod
	}
	if f.Float || f.Percent {
		return "float" + mod
	}
	return "text"
}

func parseDate(s string) (string, error) {
	layouts := []string{
		"2006-01-02",
	}
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t.Format(time.RFC3339), nil
		}
	}
	return "", errors.New("not a timestamp")
}

func parseInt(s string) (string, error) {
	s = strings.ReplaceAll(s, ",", "")
	n, err := strconv.Atoi(s)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(n), nil
}

func parseFloat(s string) (string, error) {
	s = strings.ReplaceAll(s, ",", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%f", f), nil
}

func parsePercent(s string) (string, error) {
	if !strings.HasSuffix(s, "%") {
		return "", errors.New("no trailing percent")
	}
	s = strings.TrimSuffix(s, "%")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%f", f), nil
}

func handle(filename string) error {
	in, err := os.Open(filename)
	if err != nil {
		return err
	}
	outfile := strings.TrimSuffix(filename, ".csv") + ".sql"
	out, err := os.Create(outfile)
	if err != nil {
		return err
	}

	r := csv.NewReader(in)
	r.LazyQuotes = true
	records, err := r.ReadAll()
	if err != nil {
		return err
	}
	types := make([]*fieldType, len(records[0]))
	columnNames := make([]string, len(records[0]))
	columnSeen := map[string]struct{}{}
	for i, col := range records[0] {
		types[i] = newFieldType()
		sl := slug(col)
		if sl == "" {
			sl = "x"
		}
		_, ok := columnSeen[sl]
		if ok {
			i := 2
			for {
				suffix := fmt.Sprintf("_%d", i)
				_, ok := columnSeen[sl+suffix]
				if !ok {
					sl += suffix
					break
				}
				i++
			}
		}
		columnSeen[sl] = struct{}{}
		columnNames[i] = sl
	}

	for _, row := range records[1:] {
		for i, field := range row {
			types[i].Check(field)
		}
	}

	tablename := slug(strings.TrimSuffix(filename, ".csv"))

	_, _ = fmt.Fprintf(out, "-- -*-sql-*-\n-- Created from %s\n\nbegin;\n\n", filename)
	if clean {
		_, _ = fmt.Fprintf(out, "drop table if exists %s;\n", tablename)
	}
	_, _ = fmt.Fprintf(out, "create table %s (\n", tablename)
	w := tabwriter.NewWriter(out, 4, 4, 1, ' ', 0)
	for i, name := range records[0] {
		comma := ","
		if i == len(records[0])-1 {
			comma = ""
		}
		_, _ = w.Write([]byte(fmt.Sprintf("\t%s\t%s%s\t-- %s\n", columnNames[i], types[i].SqlType(), comma, name)))
	}
	_ = w.Flush()
	_, _ = fmt.Fprintf(out, ");\n\n")
	_, _ = fmt.Fprintf(out, "copy %s (%s) from stdin csv header;\n", tablename, strings.Join(columnNames, ", "))
	csvWriter := csv.NewWriter(out)
	for _, row := range records[1:] {
		fields := make([]string, len(row))
		for i, col := range row {
			fields[i], err = types[i].Parse(col)
			if err != nil {
				log.Fatalf("internal error handling [%s] in column %d", col, i)
			}
		}
		err := csvWriter.Write(fields)
		if err != nil {
			return err
		}
	}
	csvWriter.Flush()
	_, _ = fmt.Fprintf(out, "\\.\n\ncommit;\n\n")

	return out.Close()
}

func slug(s string) string {
	punctRe := regexp.MustCompile(`[^a-z0-9]+`)
	sl := punctRe.ReplaceAllString(strings.ToLower(s), "_")
	return strings.Trim(sl, "_")
}
