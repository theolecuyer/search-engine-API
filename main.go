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
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type SearchRequest struct {
	SearchTerm string `json:"search_term"`
}

func main() {
	if err := godotenv.Load("postgres_key.env"); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	port := os.Getenv("PORT")
	fmt.Println(port)
	if port == "" {
		port = "8080"
	}

	router := mux.NewRouter()

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Handle Pinged")
		//Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		HandleSearch(w, r)
	}).Methods("POST", "OPTIONS")

	log.Printf("Server starting on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}

func HandleSearch(w http.ResponseWriter, r *http.Request) {
	log.Printf("Searching")
	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	log.Printf(req.SearchTerm)

	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		log.Fatal("No POSTGRES_URL environment variable")
	}
	//Open the database
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close() //Close the db once out of scope
	indx := lib.MakeDBIndex(db)
	res := lib.Indexes.Search(indx, "simple")
	var urls []string
	for _, result := range res {
		urls = append(urls, result.URL)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(urls)
}

func Handler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Crawling")

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
		`CREATE TABLE IF NOT EXISTS urls (
		id SERIAL PRIMARY KEY,
		url TEXT NOT NULL,
		word_count INTEGER NOT NULL,
		session_id UUID NOT NULL,
		UNIQUE (url)
	);`,
		`CREATE TABLE IF NOT EXISTS words (
		id SERIAL PRIMARY KEY,
		word TEXT NOT NULL,
		UNIQUE (word)
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
	indx := lib.MakeDBIndex(db)
	lib.Crawl(req.SearchTerm, indx)

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
