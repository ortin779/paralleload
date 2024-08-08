package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	FILE_PART_SIZE = 30 * 1000 * 1000
)

func main() {

	noGoRoutines := flag.Int("gcount", runtime.NumCPU()/2, "Specifies the max no of goroutines to download at a time")

	chunkSize := flag.Int64("size", FILE_PART_SIZE, "chunk size in bytes to fetch per request")

	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		log.Printf("url can not be empty")
		os.Exit(1)
	}

	url := args[0]
	fileMetadata, err := getMetadata(url)
	if err != nil {
		log.Fatalf("error: %s\n", err)
	}

	parts := strings.Split(url, "/")[:]
	filename := parts[len(parts)-1]
	usrHome, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("error: %s\n", err)
	}

	timer := time.Now()

	filePath := path.Join(usrHome, filename)

	createFileWithSize(filePath, fileMetadata.contentSize)
	noOfParts := fileMetadata.contentSize / *chunkSize
	if fileMetadata.contentSize%(*chunkSize) != 0 {
		noOfParts += 1
	}

	var wg sync.WaitGroup

	wg.Add(int(noOfParts))

	bufChan := make(chan int64, *noGoRoutines)

	for i := int64(0); i < noOfParts; i++ {
		bufChan <- i
		go func() {

			defer func() {
				wg.Done()
				<-bufChan
			}()

			offSet := i * (*chunkSize)
			end := (i+1)*(*chunkSize) - 1
			if end > fileMetadata.contentSize {
				end = fileMetadata.contentSize
			}
			downloadFilePart(url, filePath, offSet, end)
		}()
	}

	wg.Wait()
	fmt.Println(time.Since(timer).Seconds())

}

type urlMetadata struct {
	contentSize  int64
	acceptRanges string
	signature    string
}

// type filePartResponse struct {
// 	data       []byte
// 	err        error
// 	statusCode int
// 	offset     int64
// 	end        int64
// 	signature  string
// }

// checkUrl function takes an url and returns the information
// regarding the content-size, accept-range of the response and any error
func getMetadata(url string) (urlMetadata, error) {
	resp, err := http.Head(url)
	if err != nil {
		return urlMetadata{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return urlMetadata{}, fmt.Errorf("errored with status %s", resp.Status)
	}

	return urlMetadata{
		contentSize:  int64(resp.ContentLength),
		acceptRanges: resp.Header.Get("accept-ranges"),
		signature:    resp.Header.Get("etag"),
	}, nil
}

func createFileWithSize(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	file.Seek(size-1, io.SeekStart)
	file.Write([]byte{0})
	return nil
}

func downloadFilePart(url, filepath string, offset, end int64) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	req.Header.Set("range", fmt.Sprintf("bytes=%d-%d", offset, end))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	file, err := os.Open(filepath)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	file.WriteAt(data, offset)
	defer file.Close()
}
