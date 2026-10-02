// Package websocket provides the WebSocket hub that manages client connections
// and broadcasts real-time fleet events to all connected browsers.
package websocket

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ── Broadcaster interface ─────────────────────────────────────────────────

// Broadcaster is the narrow interface that service-layer code uses to push
// messages to all connected WebSocket clients. Keeping this as an interface
// decouples the service from the concrete Hub and makes unit-testing trivial.
type Broadcaster interface {
	Broadcast(message []byte)
}

// ── Hub ───────────────────────────────────────────────────────────────────

const (
	// clientSendBuffer is the number of outbound messages each client can
	// queue before being considered a slow consumer and disconnected.
	clientSendBuffer = 256

	// writeTimeout is the maximum time to write a single message to a client.
	writeTimeout = 10 * time.Second
)

// Hub maintains the set of active WebSocket clients and broadcasts messages
// to all of them. All exported methods are safe for concurrent use.
type Hub struct {
	// clients holds all currently-connected clients (set semantics).
	clients map[*Client]bool

	// register delivers a new client to the hub's event loop.
	register chan *Client

	// unregister removes a client from the hub's event loop.
	unregister chan *Client

	// broadcast delivers a message payload to every registered client.
	broadcast chan []byte

	// mu guards clients for read-only accessors (e.g. ClientCount).
	mu sync.RWMutex

	log *slog.Logger
}

// NewHub constructs an initialised Hub ready to be started with Run.
func NewHub(logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client, 64),
		unregister: make(chan *Client, 64),
		broadcast:  make(chan []byte, 512),
		log:        logger,
	}
}

// ── Run ───────────────────────────────────────────────────────────────────

// Run starts the hub's event loop. It must be called in its own goroutine
// and runs until ctx is cancelled (graceful shutdown).
func (h *Hub) Run(ctx context.Context) {
	h.log.Info("websocket hub started")
	defer h.log.Info("websocket hub stopped")

	for {
		select {
		case <-ctx.Done():
			// Close all remaining client connections on shutdown.
			h.mu.Lock()
			for client := range h.clients {
				h.closeClient(client)
			}
			h.mu.Unlock()
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.log.Info("websocket client connected",
				"remote_addr", client.conn.RemoteAddr().String(),
				"total_clients", h.clientCountLocked(),
			)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				h.closeClient(client)
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.Lock()
			count := len(h.clients)
			h.log.Debug("Broadcasting to clients", "count", count)
			for client := range h.clients {
				select {
				case client.send <- message:
					// Message queued successfully.
				default:
					// Client send buffer is full — slow consumer.
					// Disconnect it to avoid blocking the hub.
					h.log.Warn("slow consumer: disconnecting client",
						"remote_addr", client.conn.RemoteAddr().String(),
					)
					h.closeClient(client)
				}
			}
			h.mu.Unlock()
		}
	}
}

// closeClient closes the client's send channel and removes it from the
// clients map. Must be called with h.mu write-locked.
func (h *Hub) closeClient(client *Client) {
	delete(h.clients, client)
	close(client.send)
}

// ── Broadcast ─────────────────────────────────────────────────────────────

// Broadcast queues message for delivery to every connected client. The call
// is non-blocking: if the hub's broadcast channel is full the message is
// silently dropped rather than blocking the caller (service layer).
func (h *Hub) Broadcast(message []byte) {
	select {
	case h.broadcast <- message:
	default:
		h.log.Warn("hub broadcast channel full – message dropped")
	}
}

// ── ClientCount ───────────────────────────────────────────────────────────

// ClientCount returns the number of currently-connected WebSocket clients.
// Safe to call from any goroutine.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clientCountLocked()
}

// clientCountLocked returns len(clients). Caller must hold at least RLock.
func (h *Hub) clientCountLocked() int {
	return len(h.clients)
}

// ── Client ────────────────────────────────────────────────────────────────

// Client represents a single WebSocket connection. It pumps outbound messages
// from the send channel to the underlying conn.
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

// NewClient registers a new client in the hub and returns the Client. Callers
// must call client.WritePump() in a goroutine to start message delivery.
func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	c := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, clientSendBuffer),
	}
	hub.register <- c
	return c
}

// WritePump drains the send channel and writes messages to the WebSocket
// connection. It runs until the channel is closed (hub disconnects the
// client) or a write error occurs.
func (c *Client) WritePump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		message, ok := <-c.send
		if !ok {
			// Hub closed the channel — send a close frame.
			_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck
		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
			c.hub.log.Warn("websocket write error",
				"remote_addr", c.conn.RemoteAddr().String(),
				"error", err,
			)
			return
		}
	}
}

// ReadPump reads from the WebSocket to detect disconnections (pings/pongs).
// The browser sends no explicit messages; we just need to drain control frames
// so the connection does not appear stale. Runs until the connection closes.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512)
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				c.hub.log.Warn("websocket read error",
					"remote_addr", c.conn.RemoteAddr().String(),
					"error", err,
				)
			}
			return
		}
	}
}

// Compile-time assertion that *Hub satisfies Broadcaster.
var _ Broadcaster = (*Hub)(nil)
