package main

import (
	"fmt"
	"net/http"
	"encoding/json"
	"log"
	"strconv"
)

const DISTANCE = "200km"

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
}

func main() {
	fmt.Println("Service started")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)
		return
	}
	fmt.Fprintf(w, "Post received: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a search request")
	lat, err := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	if err != nil {
		panic(err)
		return
	}
	lon, err := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	if err != nil {
		panic(err)
		return
	}

	ran := DISTANCE

	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Fprintf(w, "search received: %v %v %v\n", lat, lon, ran)

	//return a fake post
	p := &Post{
		User: "gaobin",
		Message: "hello",
		Location: Location{
			Lat:lat,
			Lon:lon,
		},
	}

	js, err := json.Marshal(p)
	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
