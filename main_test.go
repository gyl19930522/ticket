package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"testing"
	"time"
)

func TestHandler_Read(t *testing.T) {
	if conn, _, err := websocket.DefaultDialer.Dial("wss://fstream.yshyqxx.com/ws/btcusdt@kline_1m",
		nil); err != nil {
		t.Error(err)
	} else {
		var err error
		var message []byte
		for {
			_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
			_, message, err = conn.ReadMessage()
			if err != nil {
				t.Error(err)
				return
			} else {
				fmt.Println(string(message))
			}
		}
	}
}