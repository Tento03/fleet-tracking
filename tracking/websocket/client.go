// Package websocket provides ServeWS — the HTTP handler that upgrades a plain
// HTTP connection to a WebSocket and registers the client with the hub.
package websocket

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

func buildUpgrader(corsOrigin string) websocket.Upgrader {
	return websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			if corsOrigin == "" || corsOrigin == "*" {
				return true
			}
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true
			}
			return strings.EqualFold(origin, corsOrigin)
		},
	}
}

// ServeWS upgrades the HTTP request to a WebSocket connection, registers the
// new client with hub, and launches the read/write pumps.
func ServeWS(hub *Hub, corsOrigin string, w http.ResponseWriter, r *http.Request) {
	upgrader := buildUpgrader(corsOrigin)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Warn("websocket upgrade failed",
			"remote_addr", r.RemoteAddr,
			"error", err,
		)
		return
	}

	client := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, clientSendBuffer),
	}
	hub.register <- client

	total := hub.ClientCount()
	slog.Info("WebSocket client connected",
		"remote_addr", r.RemoteAddr,
		"total", total,
	)

	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
		slog.Info("WebSocket client disconnected",
			"remote_addr", c.conn.RemoteAddr().String(),
			"total", c.hub.ClientCount(),
		)
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck

	c.conn.SetPongHandler(func(_ string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				slog.Warn("websocket read error",
					"remote_addr", c.conn.RemoteAddr().String(),
					"error", err,
				)
			}
			return
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				slog.Warn("websocket write error",
					"remote_addr", c.conn.RemoteAddr().String(),
					"error", err,
				)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)) //nolint:errcheck
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Warn("websocket ping error",
					"remote_addr", c.conn.RemoteAddr().String(),
					"error", err,
				)
				return
			}
		}
	}
}
