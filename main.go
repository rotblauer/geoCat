package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"

	"github.com/kpawlik/geojson"
)

const posBatch = 100000

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

var aliases = map[*regexp.Regexp]string{
	regexp.MustCompile(`(?i)(Big.*P.*|Isaac.*|.*moto.*|iha)`): "ia",
	regexp.MustCompile(`(?i)(Big.*Ma.*)`):                     "jr",
	regexp.MustCompile("(?i)(Rye.*|Kitty.*)"):                 "jl",
	regexp.MustCompile("(?i)Kayleigh.*"):                      "kd",
	regexp.MustCompile("(?i)(KK.*|kek)"):                      "kk",
	regexp.MustCompile("(?i)Bob.*"):                           "rj",
	regexp.MustCompile("(?i)(Pam.*|Rathbone.*)"):              "pr",
	regexp.MustCompile("(?i)Ric"):                             "ric",
	regexp.MustCompile("(?i)Twenty7.*"):                       "mat",
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

var mapRWLock = sync.RWMutex{}
var activityGlobal = make(map[string]uint64)
var activityCat = make(map[string]map[string]uint64)

func tallyMap(m map[string]uint64, k string, incr uint64) {
	if _, ok := m[k]; !ok {
		m[k] = 0
	}
	m[k] += incr
}

func tallyMapMap(m map[string]map[string]uint64, k1, k2 string, incr uint64) {
	if _, ok := m[k1]; !ok {
		m[k1] = make(map[string]uint64)
	}
	tallyMap(m[k1], k2, incr)
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

// func withReader(input io.Reader, start int64, forEach func([]byte) error) error {
// 	fmt.Println("--READER, start:", start)
//
// 	// Doesn't work with stdin.
// 	// if _, err := input.Seek(start, 0); err != nil {
// 	// 	return err
// 	// }
//
// 	// r, err := gzran.NewReader(input)
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// r := bufio.NewReader(input)
//
// 	r, err := gzip.NewReader(input)
// 	if err != nil {
// 		return err
// 	}
//
// 	pos := start
// 	for {
// 		data, err := r.ReadBytes('\n')
// 		pos += int64(len(data))
// 		if pos%posBatch == 0 {
// 			_ = storePos(pos)
// 		}
// 		if err == nil || err == io.EOF {
// 			if len(data) > 0 && data[len(data)-1] == '\n' {
// 				data = data[:len(data)-1]
// 			}
// 			if len(data) > 0 && data[len(data)-1] == '\r' {
// 				data = data[:len(data)-1]
// 			}
// 			// fmt.Printf("Pos: %d, Read: %s\n", pos, data)
// 		}
// 		if err == nil {
// 			if err := forEach(data); err != nil {
// 				return err
// 			}
// 		}
// 		if err != nil {
// 			if err != io.EOF {
// 				return err
// 			}
// 			break
// 		}
// 	}
// 	return nil
// }

type progress struct {
	pos   int64
	total int64
}

// GZLines iterates over lines of a file that's gzip-compressed.
// Iterating lines of an io.Reader is one of those things that Go
// makes needlessly complex.
// https://gist.github.com/lovasoa/38a207ecdefa1d60225403a644800818
func GZLines(rawf io.Reader) (chan []byte, chan error, error) {
	rawContents, err := gzip.NewReader(rawf)
	if err != nil {
		return nil, nil, err
	}

	bufferedContents := bufio.NewReaderSize(rawContents, 1024*1024*1024*8) // default 4096

	ch := make(chan []byte)
	errs := make(chan error)

	go func(ch chan []byte, errs chan error, contents *bufio.Reader) {
		defer func(ch chan []byte, errs chan error) {
			close(ch)
			close(errs)
		}(ch, errs)

		for {
			line, err := contents.ReadBytes('\n')
			if err != nil {
				errs <- err
				if err != io.EOF {
					return
				}
			} else {
				ch <- line
			}
		}
	}(ch, errs, bufferedContents)

	return ch, errs, nil
}

func tallyCatLine(global map[string]uint64, cat map[string]map[string]uint64, b []byte) error {
	// fmt.Println(string(b))

	f := geojson.Feature{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	a, ok := f.Properties["Activity"]
	if ok {
		tallyMap(global, a.(string), 1)

		n, ok := f.Properties["Name"]
		if ok {
			tallyMapMap(cat, aliasOrName(n.(string)), a.(string), 1)
		}
	}
	return nil
}

func lineCounter(r io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	count := int64(0)
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += int64(bytes.Count(buf[:c], lineSep))

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

var flagTargetFilepathDefault = filepath.Join(os.Getenv("HOME"), "tdata", "master.json.gz")
var flagTargetFilepath = flag.String("target", flagTargetFilepathDefault, "Target filepath")

func tallyBatch(readLines [][]byte) error {
	var ag = make(map[string]uint64)
	var ac = make(map[string]map[string]uint64)

	for _, line := range readLines {
		if err := tallyCatLine(ag, ac, line); err != nil {
			log.Fatalln(err)
		}
	}

	mapRWLock.Lock()
	defer mapRWLock.Unlock()

	for k, v := range ag {
		if _, ok := activityGlobal[k]; !ok {
			activityGlobal[k] = 0
		}
		activityGlobal[k] += v
	}
	for k, v := range ac {
		if _, ok := activityCat[k]; !ok {
			activityCat[k] = make(map[string]uint64)
		}
		for kk, vv := range v {
			if _, ok := activityCat[k][kk]; !ok {
				activityCat[k][kk] = 0
			}
			activityCat[k][kk] += vv
		}
	}

	return nil
}

func main() {

	flag.Parse()

	// Read all the file into memory.
	readStart := time.Now()
	bs, err := os.ReadFile(*flagTargetFilepath)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Read %d bytes in %s\n", len(bs), time.Since(readStart))

	// bufCount := bytes.NewBuffer(bs)
	// count, err := lineCounter(bufCount)
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	buf := bytes.NewBuffer(bs)

	// withReader(buf /* getPos() */, 0, tallyCatLine)
	lineCh, errCh, err := GZLines(buf)
	if err != nil {
		log.Fatalln(err)
	}

	readLines := make([][]byte, 0)
	n := int64(0)
readLoop:
	for {
		select {

		case line := <-lineCh:
			if n == 0 {
				fmt.Println("First line:", string(line))
			}
			n++
			readLines = append(readLines, line)
			if n%posBatch == 0 {
				fmt.Printf("Read %d lines. GOROUTINES=%d\n", n, runtime.NumGoroutine())
				go tallyBatch(readLines)
				readLines = make([][]byte, 0)
			}

			if n > posBatch*1000 {
				break readLoop
			}

		case err := <-errCh:
			if err == io.EOF {
				break readLoop
			}
			log.Fatal(err)
		}
	}

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
