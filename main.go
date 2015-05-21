// Package main provides ...
package main

import (
	"log"

	"github.com/gocql/gocql"
	"github.com/icrowley/fake"
)

func main() {
	fake.SetLang("en")

	cluster := gocql.NewCluster("104.197.8.3")
	cluster.Keyspace = "honestdollar"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("session creation failed: %+v", err)
	}
	defer session.Close()

	for range make([]int, 1000) {
		if err = addUser(session, fake.LastName(), fake.FirstName(), fake.Phone(), fake.EmailAddress()); err != nil {
			log.Printf("can't create user %+v", err)
		}
	}
}

func addUser(session *gocql.Session, lastname, firstname, phone, email string) error {
	if isUser(session, phone, email) {
		return nil
	}

	uuid, err := gocql.RandomUUID()
	if err != nil {
		log.Fatalf("UUID creation failed: %+v", err)
	}

	if err := session.Query("insert into users (id, lastname, firstname, phone, email) values (?, ?, ?, ?, ?)", uuid, lastname, firstname, phone, email).Exec(); err != nil {
		log.Fatalf("addUser: [%s]: insert failed: %+v", "users", err)
	}

	if err := session.Query("insert into user_from_email (email, userid, lastname, firstname) values (?, ?, ?, ?)", email, uuid, lastname, firstname).Exec(); err != nil {
		log.Fatalf("addUser: [%s]: insert failed: %+v", "user_from_email", err)
	}

	if err := session.Query("insert into user_from_phone (phone, userid, lastname, firstname) values (?, ?, ?, ?)", phone, uuid, lastname, firstname).Exec(); err != nil {
		log.Fatalf("addUser: [%s]:  insert failed: %+v", "user_from_phone", err)
	}

	return nil
}

func isUser(session *gocql.Session, phone, email string) bool {
	var userid gocql.UUID

	iter := session.Query("select userid from user_from_email where email = ?", email).Iter()
	defer iter.Close()

	for iter.Scan(&userid) {
		log.Printf("user exists: %s", userid)
		return true
	}

	iter = session.Query("select userid from user_from_phone where phone = ?", phone).Iter()

	for iter.Scan(&userid) {
		log.Printf("user exists: %s", userid)
		return true
	}

	return false
}
