package lib

import (
	"log"
	"sort"
	"sync"

	"github.com/kljensen/snowball"
)

type Indexes interface {
	AddToIndex(url string, currWords []string)
	Search(query string) hits
}

// Types used for inmem index
type Frequency map[string]int           //Maps links to their word frequency
type InvertedIndex map[string]Frequency //Maps each word and their correpsonding links and frequencies

type InMemoryIndex struct {
	wordFreq InvertedIndex
	doclen   Frequency
	mu       sync.Mutex
}

func MakeInMemoryIndex() *InMemoryIndex {
	return &InMemoryIndex{wordFreq: make(InvertedIndex), doclen: make(Frequency)}
}

func (i *InMemoryIndex) AddToIndex(url string, currWords []string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, word := range currWords {
		if i.wordFreq[word] == nil {
			i.wordFreq[word] = make(Frequency)
		}
		i.wordFreq[word][url]++
	}
	i.doclen[url] = len(currWords)
}

func (i *InMemoryIndex) Search(query string) hits {
	results := hits{}
	resultFrequencies := Frequency{} // Return map "link" : # of word hits
	if stemmedWordQuery, err := snowball.Stem(query, "english", true); err == nil {
		if word, exists := i.wordFreq[stemmedWordQuery]; exists {
			for link, frequency := range word {
				tfIDFScore := TfIDF(frequency, i.doclen[link], len(i.doclen), len(i.wordFreq[link]))
				results = append(results, searchHit{link, frequency, tfIDFScore})
				resultFrequencies[link] = frequency
			}
		}
	} else {
		log.Printf("Snowball returned %v", err)
	}
	sort.Sort(results)
	return results
}
