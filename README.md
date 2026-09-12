# Tug of War Math

A cartoon tug-of-war math game. Play against a CPU opponent, or host/join a room
to play a friend online over a real WebSocket connection.

## What's here

- `main.go` — a tiny Go server. It serves the game (`static/index.html`) and
  relays WebSocket messages between two players in a room. It holds no game
  state beyond "who is host / who is guest in this room" — scores and problems
  live entirely in each browser and get relayed peer-to-peer through the server.
- `static/index.html` — the whole game: canvas rendering, math problem
  generation, CPU logic, and the WebSocket client for online play.
- `go.mod` — declares the one dependency, `gorilla/websocket`.

## Deploying to Render

1. Push this folder to a GitHub repo (Render deploys from a repo, not a raw
   folder upload).
2. On [render.com](https://render.com), click **New +** → **Web Service**,
   and connect that repo.
3. Render should auto-detect Go. If it asks, set:
   - **Build Command:** `go mod tidy && go build -o server .`
   - **Start Command:** `./server`
4. Leave the environment variables alone — Render sets `PORT` automatically,
   and `main.go` already reads it (`os.Getenv("PORT")`).
5. Deploy. Render gives you a URL like `https://tug-of-war-math.onrender.com`
   — that's the link to send your friend. Opening it serves the game, and the
   game connects its WebSocket back to that same host automatically (no config
   needed on your end).

### Notes

- Render's free tier spins a service down after inactivity, so the first
  request after a quiet period can take 20–30 seconds to wake up — worth
  knowing before you're waiting on a friend to join.
- Rooms live only in server memory. If the service restarts (a redeploy, or a
  free-tier spin-down/wake-up between games), any in-progress room codes are
  gone — that's fine between games, just host a fresh room if it happens
  mid-session.
- CPU mode never touches the network — it works even if the server is asleep
  or the deploy is still spinning up.

## Running it locally first (optional but recommended)

```bash
go mod tidy
go run .
```

Then open `http://localhost:8080` in two browser tabs (or on your phone and
laptop on the same network, using your computer's local IP instead of
`localhost`) to test hosting and joining before you deploy.

## Where to go from here

- **Native iOS feel:** this is a web app today. If you want an actual App
  Store app later, the WebSocket protocol here (`host` / `join` / `start` /
  `progress` messages) can be reused as-is from a SpriteKit or Flutter+Rive
  client — you'd swap the canvas rendering for native animation but keep this
  same Go server.
- **Persistence:** if you want reconnect-after-disconnect, matchmaking, or
  leaderboards later, that's the point where you'd add a real database
  (Postgres on Render is a one-click add) instead of the in-memory room map.
