package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/kpawlik/geojson"
)

const posBatch = 10000000

var aliases = map[*regexp.Regexp]string{
	regexp.MustCompile(`(?i)(Big.*P.*|Isaac.*|moto)`): "ia",
	regexp.MustCompile("(?i)Rye.*"):                   "jl",
	regexp.MustCompile("(?i)Bob.*"):                   "bob",
	regexp.MustCompile("(?i)Ric"):                     "ric",
}

func aliasOrName(name string) string {
	for r, a := range aliases {
		if r.MatchString(name) {
			return a
		}
	}
	return name
}

// city,user,count,latest_date
// state_name,county,user,count,latest_date
// country_name,user,count,latest_date
// activity,user,count,latest_date

var activityGlobal = make(map[string]uint64)
var activityCat = make(map[string]map[string]uint64)

func tallyMap(m map[string]uint64, k string) {
	if _, ok := m[k]; !ok {
		m[k] = 0
	}
	m[k]++
}

func tallyMapMap(m map[string]map[string]uint64, k1, k2 string) {
	if _, ok := m[k1]; !ok {
		m[k1] = make(map[string]uint64)
	}
	tallyMap(m[k1], k2)
}

var storePosFilepath = "/tmp/tallycatpos.txt"

func storePos(pos int64) error {
	return os.WriteFile(storePosFilepath, []byte(fmt.Sprintf("%d", pos)), 0644)
}

func getPos() int64 {
	b, err := os.ReadFile(storePosFilepath)
	if err != nil {
		return 0
	}
	var pos int64
	_, _ = fmt.Sscanf(string(b), "%d", &pos)
	return pos
}

func withReader(input io.Reader, start int64, forEach func([]byte) error) error {
	fmt.Println("--READER, start:", start)

	// Doesn't work with stdin.
	// if _, err := input.Seek(start, 0); err != nil {
	// 	return err
	// }

	r := bufio.NewReader(input)
	pos := start
	for {
		data, err := r.ReadBytes('\n')
		pos += int64(len(data))
		if pos%posBatch == 0 {
			_ = storePos(pos)
		}
		if err == nil || err == io.EOF {
			if len(data) > 0 && data[len(data)-1] == '\n' {
				data = data[:len(data)-1]
			}
			if len(data) > 0 && data[len(data)-1] == '\r' {
				data = data[:len(data)-1]
			}
			// fmt.Printf("Pos: %d, Read: %s\n", pos, data)
		}
		if err == nil {
			if err := forEach(data); err != nil {
				return err
			}
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}

func tallyCatLine(b []byte) error {
	// fmt.Println(string(b))

	f := geojson.Feature{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		log.Fatalln(err)
	}

	a, ok := f.Properties["Activity"]
	if ok {
		tallyMap(activityGlobal, a.(string))

		n, ok := f.Properties["Name"]
		if ok {
			tallyMapMap(activityCat, aliasOrName(n.(string)), a.(string))
		}
	}
	return nil
}

var flagTargetFilepathDefault = filepath.Join(os.Getenv("HOME"), "tdata", "master.json.gz")
var flagTargetFilepath = flag.String("target", flagTargetFilepathDefault, "Target filepath")

func main() {

	flag.Parse()

	// Read all of the file into memory.
	readStart := time.Now()
	bs, err := os.ReadFile(*flagTargetFilepath)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Read %d bytes in %s\n", len(bs), time.Since(readStart))

	buf := bytes.NewBuffer(bs)

	withReader(buf /* getPos() */, 0, tallyCatLine)

	fmt.Println("Global")
	for k, v := range activityGlobal {
		fmt.Printf("%s,%d\n", k, v)
	}

	fmt.Println("Cat")
	for k, v := range activityCat {
		for kk, vv := range v {
			fmt.Printf("%s,%s,%d\n", k, kk, vv)
		}
	}

}
