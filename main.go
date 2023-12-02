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

	"github.com/paulmach/orb/geojson"
	"github.com/sams96/rgeo"
)

// city,user,count,latest_date
// state_name,county,user,count,latest_date
// country_name,user,count,latest_date
// activity,user,count,latest_date

var rg *rgeo.Rgeo

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	rg, _ = rgeo.New(rgeo.Cities10, rgeo.Provinces10, rgeo.Countries10)
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

func tallyCatLine(globalActivity map[string]uint64, catActivity map[string]map[string]uint64,
	globalState map[string]uint64, catState map[string]map[string]uint64,
	catLast map[string]time.Time, b []byte) error {
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
		tallyMap(globalActivity, a.(string), 1)
		tallyMapMap(catActivity, catName, a.(string), 1)
	}

	// {"type":"Feature","id":1,"geometry":{"type":"Point","coordinates":[-122.392033,37.789189]},"properties":{"Accur
	// acy":0,"Elevation":0,"Heading":0,"Name":"jl","Speed":0,"Time":"2010-05-04T09:15:12Z","UUID":"","UnixTime":1272964512,"Versi
	// on":""}}
	pt := f.Point()
	loc, _ := rg.ReverseGeocode([]float64{pt.Lon(), pt.Lat()})
	// (rgeo.Location) <Location> San Francisco1, California, United States of America (USA), North America
	// country := loc.Country
	state := loc.Province
	tallyMap(globalState, state, 1)
	tallyMapMap(catState, catName, state, 1)

	// shp.Open("data/naturalearthdata/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp")
	return nil
}

func tallyBatch(batchN int64, readLines [][]byte) error {
	err := tallyBatchActivity(batchN, readLines)
	if err != nil {
		return err
	}

	return nil
}

func tallyBatchActivity(batchN int64, readLines [][]byte) error {

	// Activity
	activityOutputFile := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_activity_count.csv", batchN, len(readLines)))

	// short circuit if file exists
	if _, err := os.Stat(activityOutputFile); err == nil {
		log.Println("File exists, skipping:", activityOutputFile)
		return nil
	}
	var acLast = make(map[string]time.Time)
	var activityG = make(map[string]uint64)
	var activityC = make(map[string]map[string]uint64)
	// var countryG = make(map[string]uint64)
	// var countryC = make(map[string]map[string]uint64)
	var stateG = make(map[string]uint64)
	var stateC = make(map[string]map[string]uint64)

	for _, line := range readLines {
		if err := tallyCatLine(activityG, activityC, stateG, stateC, acLast, line); err != nil {
			log.Fatalln(err)
		}
	}

	writeBuf := bytes.NewBuffer([]byte{})
	writeBuf.Write([]byte("Activity,Name,date,counts\n")) // header

	for catName, catActivityMap := range activityC {
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
	f, err := os.Create(activityOutputFile)
	if err != nil {
		log.Fatalln(err)
	}
	_, _ = f.Write(writeBuf.Bytes())
	_ = f.Close()
	log.Printf("Wrote %s\n", activityOutputFile)

	mapRWLock.Lock()
	defer mapRWLock.Unlock()

	for k, v := range activityG {
		if _, ok := activityGlobal[k]; !ok {
			activityGlobal[k] = 0
		}
		activityGlobal[k] += v
	}
	for k, v := range activityC {
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
				// {"type":"Feature","id":1,"geometry":{"type":"Point","coordinates":[-122.392033,37.789189]},"properties":{"Accur
				// acy":0,"Elevation":0,"Heading":0,"Name":"jl","Speed":0,"Time":"2010-05-04T09:15:12Z","UUID":"","UnixTime":1272964512,"Versi
				// on":""}}
			}
			n++
			readLines = append(readLines, line)
			if n%posBatch == 0 {
				fmt.Printf("Read %d lines. GOROUTINES=%d\n", n, runtime.NumGoroutine())
				go tallyBatchActivity(n/posBatch, readLines)
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
