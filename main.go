package main

import (
	"encoding/csv"
	"fmt"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	errorpath = "./log/error.log"
	urlpath = "wss://fstream.binance.com/ws/btcusdt@depth20@100ms"
)

type Handler struct {
	err  			chan error
	conn 			chan *websocket.Conn
	logfile	 		*os.File
	pricefile		*os.File
	logger 			*log.Logger
	timer 			*time.Timer
}

func main() {
	handler := initialization()
	defer handler.Close()
	go handler.LoopReconnect()
	for {
		select {
		case <- handler.timer.C:
			handler.pricefile.Close()
			handler.timer.Reset(handler.createNewFile())
		default:
			writer := csv.NewWriter(handler.pricefile)
			if err := writer.Write(handler.Read()); err != nil {
				handler.logger.SetPrefix("[Error]")
				handler.logger.Fatalln(err)
			} else {
				writer.Flush()
			}
		}
	}
}

func initialization() *Handler {
	var err error
	handler := new(Handler)
	if handler.logfile, err = os.Create(errorpath); err != nil {
		handler.logger.SetPrefix("[Error]")
		log.Fatalln(err)
	} else {
		handler.logger = log.New(handler.logfile, "[Warning]", log.LstdFlags)
	}

	handler.timer = time.NewTimer(handler.createNewFile())
	handler.conn = make(chan *websocket.Conn, 1)
	handler.err = make(chan error)
	if conn, _, err := websocket.DefaultDialer.Dial(urlpath,nil); err != nil {
		handler.logger.SetPrefix("[Error]")
		handler.logger.Fatalln(err)
	} else {
		handler.conn <- conn
	}
	return handler
}

func (handler *Handler) createNewFile() time.Duration {
	var err error
	t := time.Now()
	year, month, day := t.Date()
	path := fmt.Sprintf("./tick-%d-%d-%d.csv", year, month, day)
	if handler.pricefile, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		handler.logger.SetPrefix("[Error]")
		handler.logger.Fatalln(err)
	}
	return time.Second * time.Duration((t.Unix() / 86400 + 1) * 86400 - t.Unix() + 2)
}

func (handler *Handler) reconnect() {
	handler.close()
	if conn, _, err := websocket.DefaultDialer.Dial(urlpath, nil); err != nil {
		handler.reconnect()
	} else {
		handler.conn <- conn
	}
}

func (handler *Handler) LoopReconnect() {
	for {
		err := <- handler.err
		handler.logger.Println(err)
		handler.reconnect()
		<- handler.err
	}
}

func (handler *Handler) close() {
	conn := <- handler.conn
	_ = conn.Close()
}

func (handler *Handler) Read() []string {
	var err error
	var message []byte
	data := make([]string, 81)
	conn := <- handler.conn
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, message, err = conn.ReadMessage()
	handler.conn <- conn
	if err != nil {
		handler.err <- err
		handler.err <- nil
		return handler.Read()
	} else {
		data[0] = strconv.FormatInt(jsoniter.Get(message, "E").ToInt64(), 10)
		for i := 0; i < 20; i++ {
			data[2 * i + 1] = jsoniter.Get(message, "b", i, 0, "f").ToString()
			data[2 * i + 2] = jsoniter.Get(message, "b", i, 1, "f").ToString()
		}
		for i := 20; i < 40; i++ {
			data[2 * i + 1] = jsoniter.Get(message, "a", i, 0, "f").ToString()
			data[2 * i + 2] = jsoniter.Get(message, "a", i, 1, "f").ToString()
		}
		return data
	}
}

func (handler *Handler) Close() {
	handler.logfile.Close()
	handler.pricefile.Close()
	select {
	case <- handler.timer.C:
		handler.timer.Stop()
	default:
		handler.timer.Stop()
	}
	handler.close()
	close(handler.err)
}
