package main

import (
	"gopkg.in/olivere/elastic.v3"
	"fmt"
	"reflect"
	"net/http"
	"encoding/json"
	"github.com/dgrijalva/jwt-go"
	"time"
)

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

var privateKey = []byte("secrete") // put this in db in production, todo

//check whether user's credential is valid when signing in
func checkUser(username, password string) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false;
	}

	//Search with a match query
	termQuery := elastic.NewMatchQuery("username", username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do()

	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false
	}

	var tyu User
	for _, item := range queryResult.Each(reflect.TypeOf(tyu)) {
		u := item.(User)
		return u.Password == password && u.Username == username
	}

	//if no user exist, return false
	return false;
}

//Add a new user. Return true if successfully.
func addUser(username, password string) bool {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		fmt.Printf("ES is not setup %v\n", err)
		return false;
	}

	user := &User {
		Username: username,
		Password: password,
	}

	//Search with a match query
	termQuery := elastic.NewMatchQuery("username", username)
	queryResult, err := es_client.Search().
		Index(INDEX).
		Query(termQuery).
		Pretty(true).
		Do()
	if err != nil {
		fmt.Printf("ES query failed %v\n", err)
		return false;
	}

	if queryResult.TotalHits() > 0 {
		fmt.Printf("User %s has existed, cannot create duplicate user.\n", username)
		return false
	}

	// otherwise, save to index
	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE_USER).
		Id(username).
		BodyJson(user).
		Refresh(true).
		Do()
	if err != nil {
		fmt.Printf("ES save failed %v\n", err)
		return false
	}

	return true
}

func handlerSignUp(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a signup request")
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "POST" {
		return
	}

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
		return
	}

	if u.Username != "" && u.Password != "" {
		if addUser(u.Username, u.Password) {
			fmt.Println("User added successfully")
			w.Write([]byte("User added successfully"))
		} else {
			fmt.Println("Failed to add a new user.")
			http.Error(w, "Failed to add a new user", http.StatusInternalServerError)
		}
	} else {
		fmt.Println("Empty password or username")
		http.Error(w, "Empty password or username", http.StatusInternalServerError)
	}
}

func handlerLogin(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a login request")

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != "POST" {
		return
	}

	decoder := json.NewDecoder(r.Body)
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
		return
	}

	if checkUser(u.Username, u.Password) {
		token := jwt.New(jwt.SigningMethodHS256)
		claims := token.Claims.(jwt.MapClaims)
		// Set token claims
		claims["username"] = u.Username
		claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

		//sign token with private key
		tokenString, _ := token.SignedString(privateKey)

		//write the token to the browser
		w.Write([]byte(tokenString))
	} else {
		fmt.Println("invalid password or username.")
		http.Error(w, "Invalid password or username", http.StatusForbidden)
	}
}
