package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/streadway/amqp"
	//"io"
	"log"
	//"net/http"
	"strings"
	"sync"
)

const rabbitMqConnection = "amqp://guest:guest@localhost:5672/"

var lock sync.Mutex
var rlock sync.RWMutex

type LogMessage struct {
	datetime string
	sender   string
	message  string
}

func (lm *LogMessage) MarshalJSON() ([]byte, error) {
	dt, _ := json.Marshal(lm.datetime)
	sender, _ := json.Marshal(lm.sender)
	msg, _ := json.Marshal(lm.message)
	return []byte(fmt.Sprintf("[%v, %v, %v]", string(dt), string(sender), string(msg))), nil
}

func NewLogMessageFromRabbit(msg []byte) *LogMessage {
	var l LogMessage
	data := strings.Split(string(msg), "::")
	l.datetime = data[0]
	l.sender = data[1]
	l.message = data[2]
	return &l
}

func NewLogMessageFromSql(row *sql.Rows) *LogMessage {
	var l LogMessage
	var id int
	err := row.Scan(id, &l.datetime, &l.sender, &l.message)
	if err != nil {
		log.Println("Error parsing row: ", err)
	}
	return &l
}

func main() {
	var err error
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

	go func() {
		lock.Lock()
		defer lock.Unlock()
		for msg := range messages {
			_ = msg.Ack(false)
			lm := NewLogMessageFromRabbit(msg.Body)
			_, err := db.Exec("Insert into log (datetime, sender, message) "+
				"values ($1, $2, $3)", lm.datetime, lm.sender, lm.message)
			if err != nil {
				log.Println("Error writing to DB: ", err)
			}
		}
	}()
	<-forever
}

func ReturnNLogMessages(num int, page int, db sql.DB) []LogMessage {
	rlock.RLock()
	defer rlock.RUnlock()
	rows, err := db.Query("SELECT * FROM log LIMIT $1 OFFSET $2", string(num), string(num * page))
	if err != nil {
		log.Println("Error getting data from DB: ", err)
	}
	defer rows.Close()
	var messages []LogMessage
	for rows.Next() {
		messages = append(messages, *NewLogMessageFromSql(rows))
	}
	return messages
}
