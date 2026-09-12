package main

import (
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ---------- Client ----------

type Client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *Client) send(v interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.WriteJSON(v)
}

// ---------- Room ----------

type Room struct {
	mu    sync.Mutex
	code  string
	host  *Client
	guest *Client
}

var (
	rooms   = map[string]*Room{}
	roomsMu sync.Mutex
)

const codeChars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func makeCode() string {
	b := make([]byte, 4)
	for i := range b {
		b[i] = codeChars[rand.Intn(len(codeChars))]
	}
	return string(b)
}

func createRoom(host *Client) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	var code string
	for {
		code = makeCode()
		if _, exists := rooms[code]; !exists {
			break
		}
	}
	r := &Room{code: code, host: host}
	rooms[code] = r
	return r
}

func findRoom(code string) (*Room, bool) {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	r, ok := rooms[code]
	return r, ok
}

func removeRoomIfEmpty(r *Room) {
	r.mu.Lock()
	empty := r.host == nil && r.guest == nil
	r.mu.Unlock()
	if empty {
		roomsMu.Lock()
		delete(rooms, r.code)
		roomsMu.Unlock()
	}
}

// ---------- WebSocket message shapes ----------

type msgIn struct {
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
	Score   int    `json:"score,omitempty"`
	Problem string `json:"problem,omitempty"`
}

type msgOut map[string]interface{}

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	client := &Client{conn: conn}
	var room *Room
	var isHost bool

	cleanup := func() {
		if room == nil {
			return
		}
		room.mu.Lock()
		if isHost {
			room.host = nil
		} else {
			room.guest = nil
		}
		other := room.guest
		if isHost {
			other = room.guest
		} else {
			other = room.host
		}
		room.mu.Unlock()
		if other != nil {
			other.send(msgOut{"type": "opponentLeft"})
		}
		removeRoomIfEmpty(room)
	}
	defer cleanup()

	// Keep the connection alive; browsers/Render proxies can drop idle sockets.
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	go pinger(conn)

	for {
		var m msgIn
		if err := conn.ReadJSON(&m); err != nil {
			return
		}

		switch m.Type {

		case "host":
			r := createRoom(client)
			room = r
			isHost = true
			client.send(msgOut{"type": "hosted", "code": r.code})

		case "join":
			r, ok := findRoom(m.Code)
			if !ok {
				client.send(msgOut{"type": "error", "message": "Room not found. Check the code."})
				continue
			}
			r.mu.Lock()
			if r.guest != nil {
				r.mu.Unlock()
				client.send(msgOut{"type": "error", "message": "That room already has two players."})
				continue
			}
			r.guest = client
			host := r.host
			r.mu.Unlock()
			room = r
			isHost = false
			client.send(msgOut{"type": "joined"})
			if host != nil {
				host.send(msgOut{"type": "guestJoined"})
			}

		case "start":
			if room == nil || !isHost {
				continue
			}
			room.mu.Lock()
			h, g := room.host, room.guest
			room.mu.Unlock()
			startedAt := time.Now().Add(3 * time.Second).UnixMilli()
			payload := msgOut{"type": "start", "startedAt": startedAt}
			if h != nil {
				h.send(payload)
			}
			if g != nil {
				g.send(payload)
			}

		case "progress":
			if room == nil {
				continue
			}
			room.mu.Lock()
			var other *Client
			if isHost {
				other = room.guest
			} else {
				other = room.host
			}
			room.mu.Unlock()
			if other != nil {
				other.send(msgOut{"type": "opponent", "score": m.Score, "problem": m.Problem})
			}

		case "leave":
			return
		}
	}
}

func pinger(conn *websocket.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
			return
		}
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler)
	mux.Handle("/", http.FileServer(http.Dir("./static")))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("listening on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
