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

// city,user,count,latest_date
// state_name,county,user,count,latest_date
// country_name,user,count,latest_date
// activity,user,count,latest_date

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

const posBatch = 500000

var flagTargetFilepath = flag.String("target", filepath.Join(os.Getenv("HOME"), "tdata", "master.json.gz"), "Target filepath")
var flagOutputRootFilepath = flag.String("output-root", filepath.Join(".", "go-output"), "Output root dir")

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

func tallyCatLine(global map[string]uint64, cat map[string]map[string]uint64, catLast map[string]time.Time, b []byte) error {
	// fmt.Println(string(b))

	f := geojson.Feature{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	name, nameOk := f.Properties["Name"]
	if !nameOk {
		return nil
	}

	catName := aliasOrName(name.(string))

	t, ok := f.Properties["Time"]
	if ok {
		catLast[catName], err = time.Parse(time.RFC3339, t.(string))
		if err != nil {
			return err
		}
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

func tallyBatch(batchN int64, readLines [][]byte) error {

	outputFile := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_activity_count.csv", batchN, len(readLines)))

	// short circuit if file exists
	if _, err := os.Stat(outputFile); err == nil {
		log.Println("File exists, skipping:", outputFile)
		return nil
	}

	var ag = make(map[string]uint64)
	var ac = make(map[string]map[string]uint64)
	var acLast = make(map[string]time.Time)

	for _, line := range readLines {
		if err := tallyCatLine(ag, ac, acLast, line); err != nil {
			log.Fatalln(err)
		}
	}

	writeBuf := bytes.NewBuffer([]byte{})
	writeBuf.Write([]byte("Activity,Name,date,counts\n")) // header

	for catName, catActivityMap := range ac {
		for activity, count := range catActivityMap {
			p := fmt.Sprintf("%s,%s,%s,%d\n", activity, catName, acLast[catName].Format("2006-01-02"), count)
			// log.Println(p)
			_, err := writeBuf.Write([]byte(p))
			if err != nil {
				log.Fatalln(err)
			}
		}
	}

	// writeBuf to file
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatalln(err)
	}
	_, _ = f.Write(writeBuf.Bytes())
	_ = f.Close()
	log.Printf("Wrote %s\n", outputFile)

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

	_ = os.MkdirAll(*flagOutputRootFilepath, 0755)

	// Read all the file into memory.
	readStart := time.Now()
	bs, err := os.ReadFile(*flagTargetFilepath)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Read %d bytes in %s\n", len(bs), time.Since(readStart))

	buf := bytes.NewBuffer(bs)

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
				go tallyBatch(n/posBatch, readLines)
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
