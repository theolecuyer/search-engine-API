package lib

import (
	"fmt"
	"html/template"
	"net/http"
)

func Webserver(index Indexes) {
	http.Handle("/top10/", http.StripPrefix("/top10/", http.FileServer(http.Dir("top10"))))
	//Stand up a server that is called by listen and serve for /search at localhost:8080/search
	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		searchTerm := r.URL.Query().Get("term")
		resultFrequencies := index.Search(searchTerm)
		w.Header().Set("Content-Type", "text/html")
		//Modified so url can be clicked on and opened in a new tab
		body := "<ol> {{range .}} <li> <a href='{{.URL}}' target='_blank'>{{.URL}}</a> {{.Count}}</li> {{end}} </ol>"
		tmpl, err := template.New("demo").Parse(body)
		if err != nil {
			fmt.Printf("Parse returned %v\n", err)
		}
		tmpl.Execute(w, resultFrequencies)
	})
	http.Handle("/", http.FileServer(http.Dir("static"))) //Handler at / that takes user input
	http.ListenAndServe(":8080", nil)
}
