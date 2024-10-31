package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"mvp-seachengine/lib"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type SearchRequest struct {
	Website string `json:"website"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/", Handler)

	log.Printf("Server starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func Handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Called")

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		log.Fatal("No POSTGRES_URL environment variable")
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	tables := []string{
		`CREATE TABLE IF NOT EXISTS sessions (
			session_id UUID PRIMARY KEY,
			created_at TIMESTAMP DEFAULT NOW(),
			expires_at TIMESTAMP NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS urls (
		id SERIAL PRIMARY KEY,
		url TEXT NOT NULL,
		word_count INTEGER NOT NULL,
		session_id UUID NOT NULL,
		FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
		UNIQUE (url, session_id)
	);`,
		`CREATE TABLE IF NOT EXISTS words (
		id SERIAL PRIMARY KEY,
		word TEXT NOT NULL,
		session_id UUID NOT NULL,
		FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
		UNIQUE (word, session_id)
	);`,
		`CREATE TABLE IF NOT EXISTS mapping (
		word_id INTEGER,
		url_id INTEGER,
		frequency INTEGER NOT NULL,
		FOREIGN KEY(word_id) REFERENCES words(id) ON DELETE CASCADE,
		FOREIGN KEY(url_id) REFERENCES urls(id) ON DELETE CASCADE,
		UNIQUE (word_id, url_id)
	);`,
	}
	createTables(db, tables)
	deleteExpiredSessions(db)

	sessionID := uuid.New().String()
	//Insert the current session into the database
	if _, err := db.Exec(
		`INSERT INTO sessions (session_id, expires_at) VALUES ($1, $2)`,
		sessionID, time.Now().Add(15*time.Minute),
	); err != nil {
		log.Fatalf("Failed to insert session: %v", err)
	}
	indx := lib.MakeDBIndex(db, sessionID)
	fmt.Println(req.Website)
	lib.Crawl(req.Website, indx)

	res := lib.Indexes.Search(indx, "simple")
	var urls []string
	for _, result := range res {
		urls = append(urls, result.URL)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(urls)
}

func deleteExpiredSessions(db *sql.DB) {
	_, err := db.Exec(`DELETE FROM sessions WHERE expires_at < NOW()`)
	if err != nil {
		log.Printf("SQL Delete returned %v\n", err)
	}
}

func createTables(db *sql.DB, tables []string) {
	for _, query := range tables {
		_, err := db.Exec(query)
		if err != nil {
			log.Fatalf("Table error: %v", err)
		}
	}
}
