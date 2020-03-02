package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

type LogMessage struct {
	Datetime string
	Sender   string
	Message  string
}

func (lm *LogMessage) MarshalJSON() ([]byte, error) {
	dt, _ := json.Marshal(lm.Datetime)
	sender, _ := json.Marshal(lm.Sender)
	msg, _ := json.Marshal(lm.Message)
	return []byte(fmt.Sprintf("[%v, %v, %v]", string(dt), string(sender), string(msg))), nil
}

func NewLogMessageFromRabbit(msg []byte) *LogMessage {
	var l LogMessage
	data := strings.Split(string(msg), "::")
	l.Datetime = data[0]
	l.Sender = data[1]
	l.Message = data[2]
	return &l
}

func NewLogMessageFromSql(rows *sql.Rows) *LogMessage {
	var l LogMessage
	var id int
	err := rows.Scan(&id, &l.Datetime, &l.Sender, &l.Message)
	if err != nil {
		log.Println("Error parsing row: ", err)
	}
	return &l
}