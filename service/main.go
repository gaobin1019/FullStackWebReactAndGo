package main

import (
	"fmt"
	"net/http"
	"encoding/json"
	"log"
	"strconv"
	"gopkg.in/olivere/elastic.v3"
	"github.com/pborman/uuid"
	"reflect"
	"strings"
	"context"
	//"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"io"
)

const(
	DISTANCE = "200km"
	ES_URL = "http://35.196.156.82:9200"
	INDEX = "around"
	TYPE = "post"
	//PROJECT_ID = "aroundreact-190120"
	//BT_INSTANCE = "around-post"
	BUCKET_NAME = "post-images-190120"

)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
	Url string `json:"url"`
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}

	if !exists {
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`

		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Service started at 8080")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
	//32MB
	r.ParseMultipartForm(32 << 20)

	fmt.Println("Received a post request: %s\n", r.FormValue("message"))
	lat,_ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon,_ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User: r.FormValue("user"),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()

	file, _, err := r.FormFile("image")
	if err != nil {	//force a post to have an image
		http.Error(w, "Image is not available", http.StatusInternalServerError) //500 error
		fmt.Printf("Image is not available %v.\n", err)
		return
	}

	ctx := context.Background()

	defer file.Close()
	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError) //500 error
		fmt.Printf("GCS is not setup %v.\n", err)
		return
	}

	p.Url = attrs.MediaLink

	//save to Elastic Search
	saveToES(p, id)

	//save to big table
	//saveToBT(&p, id)

	//save to google cloud storage
}

func saveToGCS(ctx context.Context, file io.Reader, bucket, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error){
	//create a client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	bh := client.Bucket(bucket)
	//check if the bucket exists
	if _, err = bh.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bh.Object(name)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, file); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}

	//set access control to public read
	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
}

//save to google bigtable, VERY EXPANSIVE!!! was not enabled for now
/*
func saveToBT(p *Post, id string) {
	ctx := context.Background()
	bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE)
	if err != nil {
		panic(err)
		return
	}

	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	mut.Set("post", "user", bigtable.Now(), []byte(p.User))
	mut.Set("post", "message", bigtable.Now(), []byte(p.Message))

	mut.Set("location", "lat", bigtable.Now(), []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", bigtable.Now(), []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	err = tbl.Apply(ctx, id, mut)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
}
*/

func saveToES(p *Post, id string) {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	_, err = client.
		Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Post is saved to elasticSearch: %s\n", p.Message)
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

	//connect to elastic search
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	//define geo query
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()

	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("found a total of %d posts\n", searchResult.TotalHits())

	var typ Post
	var ps []Post

	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post)
		fmt.Printf("Post with message %s: %s, at lat: %v and lon: %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)
		}
	}

	js, err := json.MarshalIndent(ps, "", "\t")
	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
}

func containsFilteredWords(s *string) bool {
	filterWords := []string {
		"fuck",
	}

	for _, word := range filterWords {
		if strings.Contains(*s, word) {
			return true
		}
	}

	return false
}
