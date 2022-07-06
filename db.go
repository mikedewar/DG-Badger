package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strconv"

	"github.com/dgraph-io/badger"
)

func WriteEdges(edgeChan chan Edge) int {

	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(dir)
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	i := 0
	for edge := range edgeChan {

		txn := db.NewTransaction(true)
		defer txn.Discard()

		edgeBytes, err := json.Marshal(edge)
		if err != nil {
			log.Fatal(err)
		}

		key := []byte(strconv.Itoa(i))

		err = txn.Set(key, edgeBytes)
		if err != nil {
			log.Fatal(err)
		}

		err = txn.Commit()
		if err != nil {
			log.Println(err)
		}

		i++

	}

	return i

}
