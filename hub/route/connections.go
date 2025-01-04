package route

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func connectionRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getConnections)
	r.Delete("/", closeAllConnections)
	r.Delete("/{id}", closeConnection)
	r.Get("/watch", watchConnections)
	return r
}

func getConnections(w http.ResponseWriter, r *http.Request) {
	if !(r.Header.Get("Upgrade") == "websocket") {
		snapshot := statistic.DefaultManager.Snapshot()
		render.JSON(w, r, snapshot)
		return
	}

	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	intervalStr := r.URL.Query().Get("interval")
	interval := 1000
	if intervalStr != "" {
		t, err := strconv.Atoi(intervalStr)
		if err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		interval = t
	}

	buf := &bytes.Buffer{}
	sendSnapshot := func() error {
		buf.Reset()
		snapshot := statistic.DefaultManager.Snapshot()
		if err := json.NewEncoder(buf).Encode(snapshot); err != nil {
			return err
		}

		return wsutil.WriteMessage(conn, ws.StateServerSide, ws.OpText, buf.Bytes())
	}

	if err := sendSnapshot(); err != nil {
		return
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(interval))
	defer tick.Stop()
	for range tick.C {
		if err := sendSnapshot(); err != nil {
			break
		}
	}
}

func watchConnections(w http.ResponseWriter, r *http.Request) {
	if !(r.Header.Get("Upgrade") == "websocket") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	joinBuf := &bytes.Buffer{}
	leaveBuf := &bytes.Buffer{}

	joinSub := statistic.DefaultManager.SubscribeJoin()
	defer statistic.DefaultManager.UnsubscribeJoin(joinSub)
	leaveSub := statistic.DefaultManager.SubscribeLeave()
	defer statistic.DefaultManager.UnsubscribeLeave(leaveSub)

	joinCh := make(chan statistic.Tracker, 1024)
	leaveCh := make(chan statistic.Tracker, 1024)

	go func() {
		for tracker := range joinSub {
			select {
			case joinCh <- tracker:
			default:
			}
		}
		close(joinCh)
	}()

	go func() {
		for tracker := range leaveSub {
			select {
			case leaveCh <- tracker:
			default:
			}
		}
		close(leaveCh)
	}()

	go func() {
		for track := range joinCh {
			watchItem := ConnectionWatchItem{
				Type:       "new",
				Connection: track,
			}

			joinBuf.Reset()
			if err := json.NewEncoder(joinBuf).Encode(watchItem); err != nil {
				conn.Close()
				break
			}

			if err := wsutil.WriteMessage(conn, ws.StateServerSide, ws.OpText, joinBuf.Bytes()); err != nil {
				conn.Close()
				break
			}
		}
	}()

	go func() {
		for track := range leaveCh {
			watchItem := ConnectionWatchItem{
				Type:       "close",
				Connection: track,
			}

			leaveBuf.Reset()
			if err := json.NewEncoder(leaveBuf).Encode(watchItem); err != nil {
				conn.Close()
				break
			}

			if err := wsutil.WriteMessage(conn, ws.StateServerSide, ws.OpText, leaveBuf.Bytes()); err != nil {
				conn.Close()
				break
			}
		}
	}()

	// 阻塞等待连接关闭
	readBuf := make([]byte, 1024)
	for {
		if _, err := conn.Read(readBuf); err != nil {
			break
		}
	}
}

func closeConnection(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if c := statistic.DefaultManager.Get(id); c != nil {
		_ = c.Close()
	}
	render.NoContent(w, r)
}

func closeAllConnections(w http.ResponseWriter, r *http.Request) {
	statistic.DefaultManager.Range(func(c statistic.Tracker) bool {
		_ = c.Close()
		return true
	})
	render.NoContent(w, r)
}

type ConnectionWatchItem struct {
	Type       string            `json:"type"` // "new" or "close"
	Connection statistic.Tracker `json:"connection"`
}
