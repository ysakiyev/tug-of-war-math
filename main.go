package main

import (
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
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

const maxPlayers = 4

// characterOrder is also the auto-fill preference order at "start" time.
var characterOrder = []string{"plain", "cap", "bandana", "propeller", "bow", "mohawk", "glasses", "mustache"}

var validCharacters = func() map[string]bool {
	m := map[string]bool{}
	for _, c := range characterOrder {
		m[c] = true
	}
	return m
}()

var validBackgrounds = map[string]bool{
	"meadow": true, "desert": true, "snow": true, "sunset": true, "night": true,
}

// Player is one seat in a Room. A nil *Player in Room.players means that
// seat is empty; there is no separate "connected" flag; presence in the
// array is presence. This keeps the model simple since Phase 1 has no
// reconnect-to-same-seat support (see the "reconnect-by-token" idea for a
// later phase) - a dropped connection just frees its seat.
type Player struct {
	client    *Client
	seat      int
	isHost    bool
	team      string // "" | "blue" | "red"
	character string // "" | one of characterOrder
	name      string // "" until the player sets one; client falls back to "Player N"
}

type Room struct {
	mu         sync.Mutex
	code       string
	players    [maxPlayers]*Player
	started    bool
	background string
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

func createRoom(host *Client) (*Room, *Player) {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	var code string
	for {
		code = makeCode()
		if _, exists := rooms[code]; !exists {
			break
		}
	}
	p := &Player{client: host, seat: 0, isHost: true}
	r := &Room{code: code, background: "meadow"}
	r.players[0] = p
	rooms[code] = r
	return r, p
}

func findRoom(code string) (*Room, bool) {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	r, ok := rooms[code]
	return r, ok
}

func removeRoomIfEmpty(r *Room) {
	r.mu.Lock()
	empty := true
	for _, p := range r.players {
		if p != nil {
			empty = false
			break
		}
	}
	r.mu.Unlock()
	if empty {
		roomsMu.Lock()
		delete(rooms, r.code)
		roomsMu.Unlock()
	}
}

// addPlayer seats client in the first free slot of r. The bool return is
// whether the room could accept them; msg carries the reason when it can't.
func addPlayer(r *Room, client *Client) (*Player, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil, "That room's game has already started."
	}
	for i, p := range r.players {
		if p == nil {
			np := &Player{client: client, seat: i}
			r.players[i] = np
			return np, ""
		}
	}
	return nil, "That room is full."
}

// characterTakenByTeammate reports whether some other player sharing p's
// team already has character c. Caller must hold r.mu.
func characterTakenByTeammate(r *Room, p *Player, c string) bool {
	if p.team == "" {
		return false
	}
	for _, other := range r.players {
		if other != nil && other != p && other.team == p.team && other.character == c {
			return true
		}
	}
	return false
}

// autoFillPicks assigns a team (balancing counts) and a character (first
// free one for that team) to any player who never finished picking, so one
// distracted player can't block the match from starting. Caller must hold
// r.mu.
func autoFillPicks(r *Room) {
	blueCount, redCount := 0, 0
	for _, p := range r.players {
		if p == nil {
			continue
		}
		switch p.team {
		case "blue":
			blueCount++
		case "red":
			redCount++
		}
	}
	for _, p := range r.players {
		if p == nil {
			continue
		}
		if p.team == "" {
			if blueCount <= redCount {
				p.team = "blue"
				blueCount++
			} else {
				p.team = "red"
				redCount++
			}
		}
	}
	for _, p := range r.players {
		if p == nil || p.character != "" {
			continue
		}
		p.character = "plain"
		for _, c := range characterOrder {
			if !characterTakenByTeammate(r, p, c) {
				p.character = c
				break
			}
		}
	}
}

// ---------- Broadcast helpers ----------

func broadcastRoster(r *Room) {
	r.mu.Lock()
	var players []msgOut
	var clients []*Client
	for _, p := range r.players {
		if p == nil {
			continue
		}
		players = append(players, msgOut{
			"seat": p.seat, "isHost": p.isHost, "team": p.team, "character": p.character, "name": p.name,
		})
		clients = append(clients, p.client)
	}
	payload := msgOut{"type": "roster", "background": r.background, "players": players}
	r.mu.Unlock()
	for _, c := range clients {
		c.send(payload)
	}
}

func broadcastPlayerLeft(r *Room, seat int) {
	r.mu.Lock()
	var clients []*Client
	for _, p := range r.players {
		if p != nil {
			clients = append(clients, p.client)
		}
	}
	r.mu.Unlock()
	payload := msgOut{"type": "playerLeft", "seat": seat}
	for _, c := range clients {
		c.send(payload)
	}
}

// ---------- WebSocket message shapes ----------

type msgIn struct {
	Type       string `json:"type"`
	Code       string `json:"code,omitempty"`
	Score      int    `json:"score,omitempty"`
	Problem    string `json:"problem,omitempty"`
	Team       string `json:"team,omitempty"`
	Character  string `json:"character,omitempty"`
	Background string `json:"background,omitempty"`
	Name       string `json:"name,omitempty"`
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
	var me *Player

	cleanup := func() {
		if room == nil || me == nil {
			return
		}
		room.mu.Lock()
		// Only act if this connection is still the one occupying the seat -
		// a stale connection superseded by a reconnect/takeover (see "join"
		// below) must not clobber whoever took the seat over when it
		// finally unwinds.
		if room.players[me.seat] != me {
			room.mu.Unlock()
			return
		}
		if me.isHost {
			// The host leaving tears down the whole room - simplest correct
			// behavior for a hobby app; no host migration.
			var remaining []*Client
			for _, p := range room.players {
				if p != nil && p != me {
					remaining = append(remaining, p.client)
				}
			}
			for i := range room.players {
				room.players[i] = nil
			}
			room.mu.Unlock()
			for _, c := range remaining {
				c.send(msgOut{"type": "hostLeft"})
			}
			removeRoomIfEmpty(room)
			return
		}
		room.players[me.seat] = nil
		started := room.started
		room.mu.Unlock()
		if started {
			broadcastPlayerLeft(room, me.seat)
		} else {
			broadcastRoster(room)
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
			r, p := createRoom(client)
			room = r
			me = p
			client.send(msgOut{"type": "hosted", "code": r.code, "seat": p.seat})
			broadcastRoster(room)

		case "join":
			r, ok := findRoom(m.Code)
			if !ok {
				client.send(msgOut{"type": "error", "message": "Room not found. Check the code."})
				continue
			}
			p, errMsg := addPlayer(r, client)
			if errMsg != "" {
				client.send(msgOut{"type": "error", "message": errMsg})
				continue
			}
			room = r
			me = p
			client.send(msgOut{"type": "joined", "seat": p.seat, "code": r.code})
			broadcastRoster(room)

		case "setTeam":
			if room == nil || me == nil || (m.Team != "blue" && m.Team != "red") {
				continue
			}
			room.mu.Lock()
			if !room.started {
				me.team = m.Team
			}
			room.mu.Unlock()
			broadcastRoster(room)

		case "setCharacter":
			if room == nil || me == nil || !validCharacters[m.Character] {
				continue
			}
			room.mu.Lock()
			if !room.started && !characterTakenByTeammate(room, me, m.Character) {
				me.character = m.Character
			}
			room.mu.Unlock()
			broadcastRoster(room)

		case "setName":
			if room == nil || me == nil {
				continue
			}
			name := strings.TrimSpace(m.Name)
			if r := []rune(name); len(r) > 20 {
				name = string(r[:20])
			}
			room.mu.Lock()
			me.name = name
			room.mu.Unlock()
			broadcastRoster(room)

		case "setBackground":
			if room == nil || me == nil || !me.isHost || !validBackgrounds[m.Background] {
				continue
			}
			room.mu.Lock()
			if !room.started {
				room.background = m.Background
			}
			room.mu.Unlock()
			broadcastRoster(room)

		case "start":
			if room == nil || me == nil || !me.isHost {
				continue
			}
			room.mu.Lock()
			if room.started {
				room.mu.Unlock()
				continue
			}
			autoFillPicks(room)
			room.started = true
			var players []msgOut
			var clients []*Client
			for _, p := range room.players {
				if p == nil {
					continue
				}
				players = append(players, msgOut{"seat": p.seat, "team": p.team, "character": p.character, "name": p.name})
				clients = append(clients, p.client)
			}
			background := room.background
			room.mu.Unlock()

			startedAt := time.Now().Add(3 * time.Second).UnixMilli()
			payload := msgOut{"type": "start", "startedAt": startedAt, "background": background, "players": players}
			for _, c := range clients {
				c.send(payload)
			}

		case "progress":
			if room == nil || me == nil {
				continue
			}
			room.mu.Lock()
			var others []*Client
			for _, p := range room.players {
				if p != nil && p != me {
					others = append(others, p.client)
				}
			}
			room.mu.Unlock()
			payload := msgOut{"type": "opponentProgress", "seat": me.seat, "score": m.Score, "problem": m.Problem}
			for _, c := range others {
				c.send(payload)
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
