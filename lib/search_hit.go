package lib

import "math"

type searchHit struct {
	URL   string
	Count int
	tfIDF float64
}

type hits []searchHit

func (results hits) Len() int {
	return len(results)
}

func (results hits) Less(i, j int) bool {
	if results[i].tfIDF == results[j].tfIDF {
		return results[i].URL < results[j].URL
	}
	return results[i].tfIDF > results[j].tfIDF
}

func (results hits) Swap(i, j int) {
	results[i], results[j] = results[j], results[i]
}

func TfIDF(wordFrequency int, documentLength int, totalDocs int, amtOfDocWithWord int) float64 {
	tf := float64(wordFrequency) / float64(documentLength)
	idf := math.Log10(float64(totalDocs)) / float64(amtOfDocWithWord+1)
	tfIDFScore := (tf * idf)
	return tfIDFScore
}
