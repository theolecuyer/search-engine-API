package lib

import (
	"database/sql"
	"log"
	"sort"

	"github.com/kljensen/snowball"
	_ "github.com/lib/pq"
)

type DatabaseIndex struct {
	db                  *sql.DB
	sessionID           string
	insertURLStmt       *sql.Stmt
	insertWordStmt      *sql.Stmt
	insertFreqStmt      *sql.Stmt
	getWordIDStmt       *sql.Stmt
	getURLStmt          *sql.Stmt
	getURLWordCountStmt *sql.Stmt
	getWordFreqStmt     *sql.Stmt
}

func MakeDBIndex(db *sql.DB, sessionID string) *DatabaseIndex {
	//Insert statements
	insertURLStmt := prepare(db, `INSERT INTO urls (url, word_count, session_id) VALUES ($1, $2, $3) ON CONFLICT (url, session_id) DO NOTHING RETURNING id`)
	insertWordStmt := prepare(db, `INSERT INTO words (word, session_id) VALUES ($1, $2) ON CONFLICT (word, session_id) DO NOTHING RETURNING id`)
	insertFreqStmt := prepare(db, `INSERT INTO mapping (word_id, url_id, frequency) VALUES ($1, $2, 1) ON CONFLICT (word_id, url_id) DO UPDATE SET frequency = mapping.frequency + 1`)

	//Queries
	getWordIDStmt := prepare(db, `SELECT id FROM words WHERE word = $1 AND session_id = $2`)
	getURLStmt := prepare(db, `SELECT url FROM urls WHERE id = $1 AND session_id = $2`)
	getURLWordCountStmt := prepare(db, `SELECT word_count FROM urls WHERE id = $1 AND session_id = $2`)
	getWordFreqStmt := prepare(db, `SELECT url_id, frequency FROM mapping WHERE word_id = $1`)
	return &DatabaseIndex{
		db:                  db,
		sessionID:           sessionID,
		insertURLStmt:       insertURLStmt,
		insertWordStmt:      insertWordStmt,
		insertFreqStmt:      insertFreqStmt,
		getWordIDStmt:       getWordIDStmt,
		getURLStmt:          getURLStmt,
		getURLWordCountStmt: getURLWordCountStmt,
		getWordFreqStmt:     getWordFreqStmt,
	}
}

func (d *DatabaseIndex) AddToIndex(url string, currWords []string) {
	//Use transactions to batch apply queries to the db
	tx, err := d.db.Begin()
	if err != nil {
		log.Printf("Transaction begin failed: %v\n", err)
		return
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("Transaction rollback failed: %v\n", rbErr)
			}
			return
		}
		if commitErr := tx.Commit(); commitErr != nil {
			log.Printf("Transaction commit failed: %v\n", commitErr)
		}
	}()

	var urlID int64
	err = tx.Stmt(d.insertURLStmt).QueryRow(url, len(currWords), d.sessionID).Scan(&urlID)
	if err != nil {
		if err == sql.ErrNoRows {
			err = tx.QueryRow("SELECT id FROM urls WHERE url = $1 AND session_id = $2", url, d.sessionID).Scan(&urlID)
			if err != nil {
				log.Printf("Failed to get existing URL ID: %v\n", err)
				return
			}
		} else {
			log.Printf("URL insert failed: %v\n", err)
			return
		}
	}

	for _, word := range currWords {
		var wordID int64
		err = tx.Stmt(d.insertWordStmt).QueryRow(word, d.sessionID).Scan(&wordID)
		if err != nil {
			if err == sql.ErrNoRows {
				err = tx.Stmt(d.getWordIDStmt).QueryRow(word, d.sessionID).Scan(&wordID)
				if err != nil {
					log.Printf("Failed to get existing word ID: %v\n", err)
					return
				}
			} else {
				log.Printf("Word insert failed: %v\n", err)
				return
			}
		}

		_, err = tx.Stmt(d.insertFreqStmt).Exec(wordID, urlID)
		if err != nil {
			log.Printf("Insert frequency returned %v\n", err)
			return
		}
	}
}

func (d *DatabaseIndex) Search(query string) hits {
	results := hits{}
	if stemmedWordQuery, err := snowball.Stem(query, "english", true); err == nil {
		wordId := d.getWordID(stemmedWordQuery)
		rows, err := d.getWordFreqStmt.Query(wordId)
		if err != nil {
			log.Panicf("Lookup word freq returned %v\n", err)
		}
		defer rows.Close()

		resultUrl := make(map[int]int)
		for rows.Next() {
			var wordFreq int
			var urlID int
			err := rows.Scan(&urlID, &wordFreq)
			if err != nil {
				log.Printf("Failed to scan row %v\n", err)
			}
			if d.isURLInSession(urlID) {
				resultUrl[urlID] = wordFreq
			}
		}

		row := d.db.QueryRow("SELECT COUNT(*) FROM urls WHERE session_id = $1", d.sessionID)
		var totalDocCount int
		if err := row.Scan(&totalDocCount); err != nil {
			log.Printf("Error counting rows: %v", err)
		}

		for url, frequency := range resultUrl {
			currURL := d.getURL(url)
			docLen := d.getURLWordCount(url)
			tfIDFScore := TfIDF(frequency, docLen, totalDocCount, len(resultUrl))
			results = append(results, searchHit{currURL, frequency, tfIDFScore})
		}
	}
	sort.Sort(results)
	return results
}

func (d *DatabaseIndex) isURLInSession(urlID int) bool {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM urls WHERE id = $1 AND session_id = $2", urlID, d.sessionID).Scan(&count)
	if err != nil {
		log.Printf("Error checking URL in session: %v", err)
		return false
	}
	return count > 0
}

func prepare(db *sql.DB, statement string) *sql.Stmt {
	stmt, err := db.Prepare(statement)
	if err != nil {
		log.Printf("Prepare returned %v\n", err)
	}
	return stmt
}

func (d *DatabaseIndex) getWordID(word string) int {
	var wordID int
	err := d.getWordIDStmt.QueryRow(word, d.sessionID).Scan(&wordID)
	if err != nil {
		log.Printf("Lookup for %s returned %v\n", word, err)
	}
	return wordID
}

func (d *DatabaseIndex) getURL(urlID int) string {
	var url string
	err := d.getURLStmt.QueryRow(urlID, d.sessionID).Scan(&url)
	if err != nil {
		log.Printf("Lookup for URL ID %d returned %v\n", urlID, err)
	}
	return url
}

func (d *DatabaseIndex) getURLWordCount(urlID int) int {
	var wordCount int
	err := d.getURLWordCountStmt.QueryRow(urlID, d.sessionID).Scan(&wordCount)
	if err != nil {
		log.Printf("Lookup for URL ID %d returned %v\n", urlID, err)
	}
	return wordCount
}
