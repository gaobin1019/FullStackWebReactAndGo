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
	"github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/go-redis/redis"
	"time"
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

	fmt.Printf("***Service started at %s.\n", TCP_PORT)

	r := mux.NewRouter()

	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return privateKey, nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost)))
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch)))
	r.Handle("/login", http.HandlerFunc(handlerLogin))
	r.Handle("/signup", http.HandlerFunc(handlerSignUp))

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(TCP_PORT, nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	if r.Method != "POST" {
		return
	}

	//32MB
	r.ParseMultipartForm(32 << 20)

	fmt.Printf("***Received a post request: %v.\n", r.FormValue("message"))
	lat,_ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon,_ := strconv.ParseFloat(r.FormValue("lon"), 64)

	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	p := &Post{
		User: username.(string),
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
		fmt.Printf("***Image is not available %v.\n", err)
		return
	}

	ctx := context.Background()

	defer file.Close()
	//save to google cloud storage
	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError) //500 error
		fmt.Printf("***GCS is not setup %v.\n", err)
		return
	}

	p.Url = attrs.MediaLink

	//save to Elastic Search
	saveToES(p, id)

	//save to big table
	//saveToBT(&p, id)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	if r.Method != "GET" {
		return
	}

	fmt.Println("***Received a search request")
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

	//search on redis first
	key := r.URL.Query().Get("lat") + ":" + r.URL.Query().Get("lon") + ":" + ran

	if ENABLE_MEMCACHE {
		rs_client := redis.NewClient(&redis.Options{
			Addr: REDIS_URL,
			Password:"",
			DB: 0, //default
		})

		val, err := rs_client.Get(key).Result()
		if err != nil {
			fmt.Printf("***Redis cannot find the key %s as %v.\n", key, err)
		} else {
			fmt.Printf("***Redis find the key %s.\nReturning value from cache.\n", key)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
			return
		}
	}

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

	fmt.Printf("***Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("***found a total of %d posts\n", searchResult.TotalHits())

	var typ Post
	var ps []Post

	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post)
		fmt.Printf("***Post with message %s: %s, at lat: %v and lon: %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)
		}
	}

	js, err := json.MarshalIndent(ps, "", "\t")
	if err != nil {
		panic(err)
		return
	}

	//save to redis
	if ENABLE_MEMCACHE {
		rs_client := redis.NewClient(&redis.Options{
			Addr: REDIS_URL,
			Password: "",
			DB: 0,
		})

		err := rs_client.Set(key, string(js), 1*time.Second).Err()		//ttl 1 second
		if err != nil {
			fmt.Printf("***Redis cannot save the key %s because %v. \n", key, err)
		}
	}

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
	fmt.Printf("***Post is saved to elasticSearch: %s\n", p.Message)
}

func saveToGCS(ctx context.Context, file io.Reader, bucket, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error){
	//create a client
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("***Failed to create client: %v", err)
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
	fmt.Printf("***Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
}

//save to google bigtable, VERY EXPANSIVE!!! disabled for now
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