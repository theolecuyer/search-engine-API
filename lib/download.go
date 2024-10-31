package lib

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
)

type downloadResults struct {
	url  string
	data string
}

func Download(url string, chExtract chan downloadResults, wg *sync.WaitGroup) {
	defer wg.Done()
	if rsp, err := http.Get(url); err != nil {
		log.Printf("Download returned %v", err)
	} else {
		defer rsp.Body.Close()
		if bts, err := io.ReadAll(rsp.Body); err != nil {
			log.Printf("Io ReadAll returned %v", err)
		} else {
			result := downloadResults{
				url:  url,
				data: string(bts),
			}
			chExtract <- result
		}
	}
}

func downloadRobots(url string) (string, error) {
	if rsp, err := http.Get(url); err != nil {
		fmt.Println("Here at: ", url)
		return "", err
	} else {
		defer rsp.Body.Close()
		if bts, err := io.ReadAll(rsp.Body); err != nil {
			return "", err
		} else {
			return string(bts), err
		}
	}
}
