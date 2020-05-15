package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	futurebasepath = "wss://fstream.binance.com/ws/"
	spotbasepath = "wss://stream.binance.com:9443/ws/"
)

var kind = flag.Bool("k", true, "true is future, false is spot")
var symbol = flag.String("s", "btc", "symbol name")
var urlpath, errpath string

type Handler struct {
	err  			chan error
	conn 			chan *websocket.Conn
	logfile	 		*os.File
	pricefile		*os.File
	logger 			*log.Logger
	timer 			*time.Timer
}

func main() {
	flag.Parse()
	errpath = fmt.Sprintf("./log/%s_error.log", *symbol)
	if *kind {
		errpath = fmt.Sprintf("./log/%sfuture_error.log", *symbol)
		urlpath = fmt.Sprintf("%s%susdt@depth20@100ms", futurebasepath, *symbol)
	} else {
		errpath = fmt.Sprintf("./log/%sspot_error.log", *symbol)
		urlpath = fmt.Sprintf("%s%susdt@depth20@100ms", spotbasepath, *symbol)
	}
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
	if handler.logfile, err = os.Create(errpath); err != nil {
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
	var path string
	t := time.Now()
	year, month, day := t.Date()
	if *kind {
		path = fmt.Sprintf("./data/%s-future-%d-%d-%d.csv", *symbol, year, month, day)
	} else {
		path = fmt.Sprintf("./data/%s-spot-%d-%d-%d.csv", *symbol, year, month, day)
	}
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
	_ = conn.SetReadDeadline(time.Now().Add(time.Second * 3))
	_, message, err = conn.ReadMessage()
	handler.conn <- conn
	if err != nil {
		handler.err <- err
		handler.err <- nil
		return handler.Read()
	} else {
		data[0] = strconv.FormatInt(jsoniter.Get(message, "E").ToInt64(), 10)
		for i := 0; i < 20; i++ {
			data[2 * i + 1] = jsoniter.Get(message, "b", i, 0).ToString()
			data[2 * i + 2] = jsoniter.Get(message, "b", i, 1).ToString()
			data[2 * (i + 20) + 1] = jsoniter.Get(message, "a", i, 0).ToString()
			data[2 * (i + 20) + 2] = jsoniter.Get(message, "a", i, 1).ToString()
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
