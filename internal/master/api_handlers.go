package master

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/jalala984/master-worker/internal/events"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleWebSocket upgrades an HTTP connection to WebSocket and streams events.
func handleWebSocket(bus *events.Bus, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Error("websocket upgrade failed", "error", err)
			return
		}
		defer conn.Close()

		sub := bus.Subscribe()
		defer bus.Unsubscribe(sub)

		logger.Debug("websocket client connected", "remote", r.RemoteAddr)

		// Read loop (discard client messages, detect disconnect).
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()

		// Write loop — send events to client.
		for event := range sub {
			if err := conn.WriteMessage(websocket.TextMessage, event.JSON()); err != nil {
				logger.Debug("websocket write failed", "error", err)
				return
			}
		}
	}
}
