package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
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

// var flagTargetFilepath = flag.String("target", filepath.Join(os.Getenv("HOME"), "tdata", "master.json.gz"), "Target filepath")
var flagOutputRootFilepath = flag.String("output-root", filepath.Join(".", "go-output"), "Output root dir")
var flagBatchSize = flag.Int64("batch-size", 500_000, "Batch size")

func getActivityOutputFilepath(batchSize, batchNumber int64) string {
	return filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_activity_count.csv", batchNumber, batchSize))
}

func getStateOutputFilepath(batchSize, batchNumber int64) string {
	return filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_state_count.csv", batchNumber, batchSize))
}

func getCountryOutputFilepath(batchSize, batchNumber int64) string {
	return filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_country_count.csv", batchNumber, batchSize))
}

func rootOutputComplete(batchSize, batchNumber int64) bool {
	if _, err := os.Stat(getActivityOutputFilepath(batchSize, batchNumber)); err != nil {
		return false
	}
	if _, err := os.Stat(getStateOutputFilepath(batchSize, batchNumber)); err != nil {
		return false
	}
	if _, err := os.Stat(getCountryOutputFilepath(batchSize, batchNumber)); err != nil {
		return false
	}
	return true
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

// // GZLines iterates over lines of a file that's gzip-compressed.
// // Iterating lines of an io.Reader is one of those things that Go
// // makes needlessly complex.
// // https://gist.github.com/lovasoa/38a207ecdefa1d60225403a644800818
// func GZLines(rawf io.Reader) (chan []byte, chan error, error) {
// 	// rawContents, err := gzip.NewReader(rawf)
// 	// if err != nil {
// 	// 	return nil, nil, err
// 	// }
//
// 	pr, pw := io.Pipe()
// 	gzR, _ := gzip.NewReader(pr)
//
// 	// bufferedContents := bufio.NewReaderSize(rawContents, 1024*1024*8) // default 4096
//
// 	// pr, pw := io.Pipe()
// 	// rawPiped, _ := gzip.NewReader(pr)
//
// 	ch := make(chan []byte, 2)
// 	errs := make(chan error)
//
// 	go func(ch chan []byte, errs chan error, contents *bufio.Reader) {
// 		defer func(ch chan []byte, errs chan error) {
// 			close(ch)
// 			close(errs)
// 		}(ch, errs)
//
// 		for {
// 			line, err := contents.ReadBytes('\n')
// 			if err != nil {
// 				errs <- err
// 				if err != io.EOF {
// 					return
// 				}
// 			} else {
// 				ch <- line
// 			}
// 		}
// 	}(ch, errs, bufferedContents)
//
// 	return ch, errs, nil
// }

func readLines(raw io.Reader, batchSize int64, workers int) (chan [][]byte, chan error, error) {
	bufferedContents := bufio.NewReaderSize(raw, 4096) // default 4096

	ch := make(chan [][]byte, workers)
	errs := make(chan error)

	go func(ch chan [][]byte, errs chan error, contents *bufio.Reader) {
		defer func(ch chan [][]byte, errs chan error) {
			close(ch)
			close(errs)
		}(ch, errs)

		lines := [][]byte{}
		for {
			line, err := contents.ReadBytes('\n')
			if err != nil {
				errs <- err
				if err != io.EOF {
					return
				}
			} else {
				lines = append(lines, line)
				if int64(len(lines)) == batchSize {
					ch <- lines
					lines = [][]byte{}
				}
			}
		}
		if len(lines) > 0 {
			ch <- lines
		}
	}(ch, errs, bufferedContents)

	return ch, errs, nil
}

func mustGetTrackTime(rawTrack []byte) time.Time {
	f := geojson.Feature{}
	if err := f.UnmarshalJSON(rawTrack); err != nil {
		log.Fatalln(err)
	}
	t, ok := f.Properties["Time"]
	if !ok {
		log.Fatalln("No time property")
	}
	trackTime, err := time.Parse(time.RFC3339, t.(string))
	if err != nil {
		log.Fatalln(err)
	}
	return trackTime
}

func tallyCatActivity(catActivity map[string]map[string]uint64, catTimes map[string]time.Time, f geojson.Feature) error {
	// fmt.Println(string(b))

	name, nameOk := f.Properties["Name"]
	if !nameOk {
		return nil
	}
	catName := aliasOrName(name.(string))

	t, ok := f.Properties["Time"]
	if ok {
		t, err := time.Parse(time.RFC3339, t.(string))
		if err != nil {
			return err
		}
		catTimes[catName] = t // overwrite will yield latest for cat name
	}

	a, ok := f.Properties["Activity"]
	if ok {
		tallyMapMap(catActivity, catName, a.(string), 1)
	}

	return nil
}

func tallyCatActivities(features []geojson.Feature) (counts map[string]map[string]uint64, times map[string]time.Time, err error) {
	counts = make(map[string]map[string]uint64)
	times = make(map[string]time.Time)
	for _, f := range features {
		if err := tallyCatActivity(counts, times, f); err != nil {
			log.Fatalln(err)
		}
	}
	return counts, times, err
}

func tallyBatchActivity(batchN int64, features []geojson.Feature) error {
	activityOutputFile := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_activity_count.csv", batchN, len(features)))

	counts, times, err := tallyCatActivities(features)

	writeBuf := bytes.NewBuffer([]byte{})
	writeBuf.Write([]byte("Activity,Name,date,counts\n")) // header

	for catName, catActivityMap := range counts {
		for activity, count := range catActivityMap {
			p := fmt.Sprintf("%s,%s,%s,%d\n", activity, catName, times[catName].Format("2006-01-02"), count)
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

	return nil
}

func tallyCatLoc(catStates, catCountries map[string]map[string]uint64, catTimes map[string]time.Time, f geojson.Feature) error {
	// fmt.Println(string(b))

	name, nameOk := f.Properties["Name"]
	if !nameOk {
		return nil
	}
	catName := aliasOrName(name.(string))

	t, ok := f.Properties["Time"]
	if ok {
		t, err := time.Parse(time.RFC3339, t.(string))
		if err != nil {
			return err
		}
		catTimes[catName] = t // overwrite will yield latest for cat name
	}

	// {"type":"Feature","id":1,"geometry":{"type":"Point","coordinates":[-122.392033,37.789189]},"properties":{"Accur
	// acy":0,"Elevation":0,"Heading":0,"Name":"jl","Speed":0,"Time":"2010-05-04T09:15:12Z","UUID":"","UnixTime":1272964512,"Versi
	// on":""}}
	pt := f.Point()

	loc, _ := rg.ReverseGeocode([]float64{pt.Lon(), pt.Lat()})
	// => (rgeo.Location) <Location> San Francisco1, California, United States of America (USA), North America

	state := loc.Province
	country := loc.Country

	tallyMapMap(catStates, catName, state, 1)
	tallyMapMap(catCountries, catName, country, 1)

	// shp.Open("data/naturalearthdata/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp")
	return nil
}

func tallyCatLocs(features []geojson.Feature) (states map[string]map[string]uint64, countries map[string]map[string]uint64, times map[string]time.Time, err error) {
	states = make(map[string]map[string]uint64)
	countries = make(map[string]map[string]uint64)
	times = make(map[string]time.Time)
	for _, f := range features {
		if err := tallyCatLoc(states, countries, times, f); err != nil {
			log.Fatalln(err)
		}
	}
	return states, countries, times, err
}

func tallyBatchLoc(batchN int64, features []geojson.Feature) error {
	stateOutputFile := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_state_count.csv", batchN, len(features)))
	countryOutputFile := filepath.Join(*flagOutputRootFilepath, fmt.Sprintf("batch.%d.size.%d_country_count.csv", batchN, len(features)))

	states, countries, times, err := tallyCatLocs(features)

	// States
	writeBuf := bytes.NewBuffer([]byte{})
	writeBuf.Write([]byte("State,Name,date,counts\n")) // header

	for catName, m := range states {
		for state, count := range m {
			// sanitize state names, eg. "Rhondda, Cynon, Taff" => "Rhondda/ Cynon/ Taff"
			if strings.Contains(state, ",") {
				state = strings.ReplaceAll(state, ",", "/")
			}
			p := fmt.Sprintf("%s,%s,%s,%d\n", state, catName, times[catName].Format("2006-01-02"), count)
			// log.Println(p)
			_, err := writeBuf.Write([]byte(p))
			if err != nil {
				log.Fatalln(err)
			}
		}
	}

	// writeBuf to file
	f, err := os.Create(stateOutputFile)
	if err != nil {
		log.Fatalln(err)
	}
	_, _ = f.Write(writeBuf.Bytes())
	_ = f.Close()
	log.Printf("Wrote %s\n", stateOutputFile)

	// Countries
	writeBuf.Reset()
	writeBuf.Write([]byte("Country,Name,date,counts\n")) // header

	for catName, m := range countries {
		for country, count := range m {
			p := fmt.Sprintf("%s,%s,%s,%d\n", country, catName, times[catName].Format("2006-01-02"), count)
			// log.Println(p)
			_, err := writeBuf.Write([]byte(p))
			if err != nil {
				log.Fatalln(err)
			}
		}
	}

	// writeBuf to file
	f, err = os.Create(countryOutputFile)
	if err != nil {
		log.Fatalln(err)
	}
	_, _ = f.Write(writeBuf.Bytes())
	_ = f.Close()
	log.Printf("Wrote %s\n", countryOutputFile)

	return nil
}

// tallyBatch runs on a batch of lines.
// It short-circuits before unmarshalling the json if the ACTIVITY file exists.
// I think the json ummarshalling is the bottleneck.
func tallyBatch(batchN int64, readLines [][]byte) error {
	if rootOutputComplete(*flagBatchSize, batchN) {
		log.Printf("Skipping batch %d, output already exists\n", batchN)
		return nil
	}

	readFeatures := make([]geojson.Feature, 0)
	for _, line := range readLines {
		f := geojson.Feature{}
		if err := f.UnmarshalJSON(line); err != nil {
			log.Fatalln(err)
		}
		readFeatures = append(readFeatures, f)
	}
	go tallyBatchActivity(batchN, readFeatures)
	go tallyBatchLoc(batchN, readFeatures)
	return nil
}

func main() {

	flag.Parse()

	_ = os.MkdirAll(*flagOutputRootFilepath, 0755)

	// // Read all the file into memory.
	// readStart := time.Now()
	// bs, err := os.ReadFile(*flagTargetFilepath)
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// log.Printf("Read %d bytes in %s\batchCount", len(bs), time.Since(readStart))
	//
	// buf := bytes.NewBuffer(bs)
	// lineCh, errCh, err := GZLines(buf)
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	linesCh, errCh, err := readLines(os.Stdin, *flagBatchSize, 4)
	if err != nil {
		log.Fatalln(err)
	}

	batchCount := int64(0)

readLoop:
	for {
		select {

		case lines := <-linesCh:
			if batchCount == 0 {
				log.Println("First line:", string(lines[0]))
				// => {"type":"Feature","id":1,"geometry":{"type":"Point","coordinates":[-122.392033,37.789189]},"properties":{"Accuracy":0,"Elevation":0,"Heading":0,"Name":"jl","Speed":0,"Time":"2010-05-04T09:15:12Z","UUID":"","UnixTime":1272964512,"Version":""}}
			}
			batchCount++

			lastTrackTime := mustGetTrackTime(lines[len(lines)-1])

			log.Println("Batch", batchCount, "GOROUTINES", runtime.NumGoroutine(), lastTrackTime.Format(time.RFC3339))
			tallyBatch(batchCount, lines)
			// if batchCount%posBatch == 0 {
			// 	// // Poor man's throttle.
			// 	// once := sync.Once{}
			// 	// nRoutines := runtime.NumGoroutine()
			// 	// vmStat, _ := mem.VirtualMemory()
			// 	// freeMB := vmStat.Free / (1024 * 1024)
			// 	// for nRoutines > runtime.GOMAXPROCS(0) || freeMB < 4096 {
			// 	// 	once.Do(func() {
			// 	// 		log.Println("Throttling...")
			// 	// 	})
			// 	// 	time.Sleep(500 * time.Millisecond)
			// 	// 	nRoutines = runtime.NumGoroutine()
			// 	// 	freeMB = vmStat.Free / (1024 * 1024)
			// 	// }
			// 	// fmt.Printf("Read %d lines. GOROUTINES=%d Free memory=%s\n", batchCount, nRoutines, strconv.FormatUint(freeMB, 10)+" MB")
			//
			// }

		case err := <-errCh:
			if err == io.EOF {
				break readLoop
			}
			log.Fatal(err)
		}
	}
	// if len(didReadLines) > 0 {
	// 	fmt.Printf("Read %d lines. GOROUTINES=%d\n", batchCount, runtime.NumGoroutine())
	// 	tallyBatch(batchCount/posBatch, didReadLines)
	// }

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
