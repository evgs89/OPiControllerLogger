package main

import (
	"OPiControllerLogger/utils"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"strconv"
	"sync"
)

const rabbitMqConnection = "amqp://guest:guest@localhost:5672/"

var lock sync.Mutex
var rlock sync.RWMutex

func main() {
	conn, err := amqp.Dial(rabbitMqConnection)
	if err != nil {
		log.Fatal("Failed to connect RabbitMQ")
	}
	channel, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to create channel RabbitMQ")
	}
	queue, err := channel.QueueDeclare(
		"log",
		true,
		false,
		false,
		false,
		nil,
	)
	messages, err := channel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Failed to register a consumer")
	}

	db, err := sql.Open("sqlite3", "logbase.sqlite")
	if err != nil {
		log.Fatal("Can't open DB file! Error: ", err)
	}
	defer db.Close()
	forever := make(chan bool)
	go ConsumeMessages(messages, db)
	go StartHttpServer(db, err)
	<-forever
}

func StartHttpServer(db *sql.DB, err error) {
	http.HandleFunc("/", HTTPHandler(db))
	err = http.ListenAndServe(":48700", nil)
	if err != nil {
		fmt.Println("http server error: ", err)
	}
}

func ConsumeMessages(messages <-chan amqp.Delivery, db *sql.DB) {
	lock.Lock()
	defer lock.Unlock()
	for msg := range messages {
		_ = msg.Ack(false)
		lm := utils.NewLogMessageFromRabbit(msg.Body)
		_, err := db.Exec("Insert into log (datetime, sender, message) "+
			"values ($1, $2, $3)", lm.Datetime, lm.Sender, lm.Message)
		if err != nil {
			log.Println("Error writing to DB: ", err)
		}
	}
}

func HTTPHandler(db *sql.DB) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Methods", "GET")
		fmt.Println("requested: ", req.URL)
		q := req.URL.Query()
		num, err := strconv.Atoi(q.Get("num"))
		if err != nil {
			num = 100
		}
		page, err := strconv.Atoi(q.Get("page"))
		if err != nil {
			page = 0
		}
		_, err = w.Write(ReturnNLogMessages(num, page, *db))
	}
}

func ReturnNLogMessages(num int, page int, db sql.DB) []byte {
	rlock.RLock()
	defer rlock.RUnlock()
	rows, err := db.Query("SELECT * FROM log LIMIT $1 OFFSET $2", num, num*page)
	if err != nil {
		log.Println("Error getting data from DB: ", err)
	}
	defer rows.Close()
	var messages []utils.LogMessage
	for rows.Next() {
		messages = append(messages, *utils.NewLogMessageFromSql(rows))
	}
	data, err := json.Marshal(messages)
	if err != nil {
		log.Println("Error serializing data to JSON: ", err)
	}
	return data
}
