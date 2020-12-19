package gtfs

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/MKuranowski/WarsawGTFS/realtime/util"
)

// ReaderAtCloser is an interface implementing both io.ReaderAt and io.Closer
type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

// Gtfs is an object with access to GTFS data
type Gtfs struct {
	Routes   map[string]sort.StringSlice // route_type → [route_id route_id ...]
	Stops    map[string][2]float64       // stop_id → [stop_lat stop_lon]
	Services map[string]bool             // service_id → true
	Trips    map[string][2]string        // trip_id → [route_id service_id]

	fileObj  ReaderAtCloser
	ZipFile  *zip.Reader
	SyncTime time.Time
}

// NewGtfsFromFile automatically creates a Gtfs object from a file on disk
func NewGtfsFromFile(fname string) (gtfs *Gtfs, err error) {
	// Make maps for some fields
	gtfs = &Gtfs{
		Routes:   make(map[string]sort.StringSlice),
		Stops:    make(map[string][2]float64),
		Services: make(map[string]bool),
		Trips:    make(map[string][2]string),
	}

	// Open the file
	fileObj, err := os.Open(fname)
	if err != nil {
		return
	}
	gtfs.fileObj = fileObj

	// Get file's size
	stat, err := fileObj.Stat()
	if err != nil {
		gtfs.fileObj.Close()
		return
	}
	size := stat.Size()

	// Create a zipfile
	zipFile, err := zip.NewReader(gtfs.fileObj, size)
	if err != nil {
		gtfs.fileObj.Close()
		return
	}

	gtfs.ZipFile = zipFile
	gtfs.SyncTime = time.Now()
	return
}

// NewGtfsFromURL automatically creates a Gtfs object from a URL
func NewGtfsFromURL(url string, client *http.Client) (gtfs *Gtfs, err error) {
	// Make maps for some fields
	gtfs = &Gtfs{
		Routes:   make(map[string]sort.StringSlice),
		Stops:    make(map[string][2]float64),
		Services: make(map[string]bool),
		Trips:    make(map[string][2]string),
	}

	// Request the URL
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	gtfs.SyncTime = time.Now()

	// Check response code
	if resp.StatusCode <= 199 || resp.StatusCode >= 300 {
		err = util.RequestError{URL: url, Status: resp.Status, StatusCode: resp.StatusCode}
		return
	}

	// Make a tempfile
	tempFile, err := ioutil.TempFile("", "warsawgtfsrt_*.zip")
	gtfs.fileObj = tempFile

	// Write URL content to the tempfile
	_, err = io.Copy(tempFile, resp.Body)
	if err != nil {
		return
	}
	tempFile.Sync()

	// Get file's size
	stat, err := tempFile.Stat()
	if err != nil {
		tempFile.Close()
		return
	}
	size := stat.Size()

	// Create a zipfile
	zipFile, err := zip.NewReader(gtfs.fileObj, size)
	if err != nil {
		tempFile.Close()
		return
	}

	gtfs.ZipFile = zipFile
	return
}

// Close closes the underlaying file object
func (g *Gtfs) Close() error {
	return g.fileObj.Close()
}

// GetZipFileByName will loop over every file in the zip.Reader object,
// and return the first pointer to zip.File taht matches the provided filename.
// A nil-pointer is returned if no matching file was found.
func (g *Gtfs) GetZipFileByName(fileName string) (filePointer *zip.File) {
	for _, file := range g.ZipFile.File {
		if file.Name == fileName {
			return file
		}
	}
	return nil
}

// LoadRoutes loads routes.txt from provided zip.File
func (g *Gtfs) LoadRoutes(file *zip.File) (err error) {
	fileReader, err := file.Open()
	if err != nil {
		return
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	header, err := csvReader.Read()

	if err != nil {
		return
	}

	for {
		// Retrieve next row
		rowSlice, errI := csvReader.Read()
		err = errI
		if err == io.EOF {
			break
		} else if err != nil {
			return
		}

		// Convert row to a map and assert all requires columns are there
		row := util.ZipStrings(header, rowSlice)
		routeType, has1 := row["route_type"]
		routeID, has2 := row["route_id"]

		if !has1 || !has2 {
			err = errors.New("routes.txt is missing route_type or route_id columns")
			return
		}

		// Save data
		g.Routes[routeType] = util.StringSliceInsert(g.Routes[routeType], routeID)
	}
	return
}

// LoadStops loads stops.txt from provided zip.File
func (g *Gtfs) LoadStops(file *zip.File) (err error) {
	fileReader, err := file.Open()
	if err != nil {
		return
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	header, err := csvReader.Read()

	if err != nil {
		return
	}

	for {
		// Retrieve next row
		rowSlice, errI := csvReader.Read()
		err = errI
		if err == io.EOF {
			break
		} else if err != nil {
			return
		}

		// Convert row to a map and assert all requires columns are there
		row := util.ZipStrings(header, rowSlice)
		stopID, has1 := row["stop_id"]
		stopLatS, has2 := row["stop_lat"]
		stopLonS, has3 := row["stop_lon"]

		if !has1 || !has2 || !has3 {
			err = errors.New("stops.txt is missing stop_id or stop_lat or stop_lon columns")
			return
		}

		// Convert stop_lat and stop_lon to floats
		stopLatF, errInner := strconv.ParseFloat(stopLatS, 64)
		if errInner != nil {
			return errInner
		}

		stopLonF, errInner := strconv.ParseFloat(stopLonS, 64)
		if errInner != nil {
			return errInner
		}

		// Save data
		g.Stops[stopID] = [2]float64{stopLatF, stopLonF}
	}
	return
}

// LoadServices loads calendar_dates.txt from provided zip.File
func (g *Gtfs) LoadServices(file *zip.File) (err error) {
	expectedDate := g.SyncTime.Format("20060102")

	fileReader, err := file.Open()
	if err != nil {
		return
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	header, err := csvReader.Read()

	if err != nil {
		return
	}

	for {
		// Retrieve next row
		rowSlice, errI := csvReader.Read()
		err = errI
		if err == io.EOF {
			break
		} else if err != nil {
			return
		}

		// Convert row to a map and assert all requires columns are there
		row := util.ZipStrings(header, rowSlice)
		serviceID, has1 := row["service_id"]
		serviceDate, has2 := row["date"]

		if !has1 || !has2 {
			err = errors.New("calendar_dates.txt is missing service_id or date columns")
			return
		}

		// Save data, only if data matches
		if serviceDate == expectedDate {
			g.Services[serviceID] = true
		}
	}
	return
}

// LoadTrips loads trips.txt from provided zip.File
func (g *Gtfs) LoadTrips(file *zip.File) (err error) {
	fileReader, err := file.Open()
	if err != nil {
		return
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)
	header, err := csvReader.Read()

	if err != nil {
		return
	}

	for {
		// Retrieve next row
		rowSlice, errI := csvReader.Read()
		err = errI
		if err == io.EOF {
			break
		} else if err != nil {
			return
		}

		// Convert row to a map and assert all requires columns are there
		row := util.ZipStrings(header, rowSlice)
		tripID, has1 := row["trip_id"]
		routeID, has2 := row["route_id"]
		serviceID, has3 := row["service_id"]

		if !has1 || !has2 || !has3 {
			err = errors.New("trips.txt is missing trip_id or route_id or service_id columns")
			return
		}

		// Save data
		g.Trips[tripID] = [2]string{routeID, serviceID}
	}
	return
}

// LoadAll attempts to load all file
func (g *Gtfs) LoadAll() (err error) {
	var wg sync.WaitGroup
	expectedFiles := map[string]func(*zip.File) error{
		"routes.txt":         g.LoadRoutes,
		"stops.txt":          g.LoadStops,
		"trips.txt":          g.LoadTrips,
		"calendar_dates.txt": g.LoadServices,
	}

	for _, f := range g.ZipFile.File {
		loader, ok := expectedFiles[f.Name]
		if !ok {
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			err := loader(f)
			if err != nil {
				panic(err)
			}
		}()

		delete(expectedFiles, f.Name)
	}

	wg.Wait()
	if len(expectedFiles) != 0 {
		err = errors.New("Missing files in GTFS (required routes.txt, stops.txt, trips.txt and calendar_dates.txt)")
	}
	return
}

// ListGtfsRoutes will automatically open the GTFS file from a given source,
// and try to read routes.txt to extract a mapping route_type → sort.StringSlice[route_id, ...]
func ListGtfsRoutes(source string) (routeMap map[string]sort.StringSlice, err error) {
	// Open the GTFS file
	gtfs, err := NewGtfsFromFile(source)
	if err != nil {
		return
	}

	// Find routes.txt
	for _, f := range gtfs.ZipFile.File {
		if f.Name == "routes.txt" {
			err = gtfs.LoadRoutes(f)
			if err != nil {
				return
			}
			break
		}
	}

	// Extract the mapping
	routeMap = gtfs.Routes
	gtfs.Close()
	return
}
