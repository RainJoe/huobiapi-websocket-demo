package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

var (
	btcKlineTopic = "market.btcusdt.kline.1min"
	eosKlineTopic = "market.eosusdt.kline.1min"
)

//SubRequest keyword to subscribe huobi api
type SubRequest struct {
	Sub    string `json:"sub"`
	ID     string `json:"id"`
	FreqMs int    `json:"freq-ms,omitempty"`
}

//Tick kline info
type Tick struct {
	ID     int     `json:"id"`
	Open   float64 `json:"open"`
	Close  float64 `json:"close"`
	Low    float64 `json:"low"`
	High   float64 `json:"high"`
	Amount float64 `json:"amount"`
	Volume float64 `json:"vol"`
	Count  int     `json:"count"`
}

//SubResponse huobi api response
type SubResponse struct {
	Ch   string `json:"ch"`
	Ts   int    `json:"ts"`
	Tick Tick   `json:"tick"`
}

func gzipCompress(in []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	msg, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "wss", Host: "api.huobi.pro", Path: "/ws"}
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	subRequest := &SubRequest{Sub: btcKlineTopic, ID: "id1", FreqMs: 5000}
	msg, err := json.Marshal(subRequest)
	if err != nil {
		log.Println(err)
	}
	err = c.WriteMessage(websocket.TextMessage, []byte(msg))
	if err != nil {
		log.Println("write", err)
	}

	done := make(chan struct{})

	subRes := SubResponse{}

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			msg, err := gzipCompress(message)
			if err != nil {
				log.Printf("%s", err)
			}
			err = json.Unmarshal(msg, &subRes)
			if err == nil {
				log.Printf("recv: %#v", subRes)
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			err := c.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}
