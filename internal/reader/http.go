package reader

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// bufPool reuses byte buffers for JSON response encoding.
// At 1500 QPS this avoids ~1500 buffer allocations/sec and cuts GC pressure.
var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// HTTP exposes the reader service over HTTP.
//
// Routes:
//
//	POST /query
//	     body: { "reference_ids": [...], "start_time": unix_seconds, "end_time": unix_seconds }
//	     → 200 { "events": [...], "total_events": N, "query_time_ms": N, "metrics": {...} }
//
//	GET  /events/{ref_id}[?start_time=RFC3339&end_time=RFC3339]
//	     → 200 { "ref_id", "count", "events": [...] }
//	     → 404 { "error": "no events found" }
//
//	GET  /events/{ref_id}/{event_id}
//	     → 200 AuthEvent
//	     → 404 { "error": "event not found" }
//
//	GET  /health
//	     → 200 { "status": "ok", "cache_enabled": true|false, "cache_live": N }
type HTTP struct {
	svc *Service
}

// NewHTTP returns an HTTP handler wrapping svc.
func NewHTTP(svc *Service) *HTTP {
	return &HTTP{svc: svc}
}

// Handler returns the http.Handler to pass to http.ListenAndServe.
func (h *HTTP) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.health)
	mux.HandleFunc("/query", h.query)
	mux.HandleFunc("/events/", h.events)
	return mux
}

// ─── handlers ────────────────────────────────────────────────────────────────

func (h *HTTP) health(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{"status": "ok"}
	if hc := h.svc.cache; hc != nil {
		stats := hc.Stats()
		resp["cache_enabled"] = true
		resp["cache_live_entries"] = stats.LiveEntries
		resp["cache_max_entries"] = stats.MaxEntries
	} else {
		resp["cache_enabled"] = false
	}
	writeJSON(w, http.StatusOK, resp)
}

// query handles POST /query — the endpoint the test client calls.
func (h *HTTP) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	var req struct {
		ReferenceIDs []string `json:"reference_ids"`
		StartTime    int64    `json:"start_time,omitempty"`
		EndTime      int64    `json:"end_time,omitempty"`
		Category     string   `json:"category,omitempty"`
		EventType    string   `json:"event_type,omitempty"`
		Limit        int      `json:"limit,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.ReferenceIDs) == 0 {
		writeErr(w, http.StatusBadRequest, "reference_ids is required and must not be empty")
		return
	}

	startTime := unixToRFC3339(req.StartTime)
	endTime := unixToRFC3339(req.EndTime)

	result, err := h.svc.LookupMany(r.Context(), req.ReferenceIDs, startTime, endTime)
	if err != nil {
		log.Printf("[reader:http] /query: %v", err)
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// events handles GET /events/{ref_id} and GET /events/{ref_id}/{event_id}.
func (h *HTTP) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "only GET is supported")
		return
	}
	parts := splitPath(r.URL.Path, "/events/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusBadRequest, "ref_id is required — use /events/{ref_id}")
		return
	}
	refID := parts[0]
	if len(parts) >= 2 && parts[1] != "" {
		h.getOne(w, r, refID, parts[1])
		return
	}
	h.getAll(w, r, refID)
}

func (h *HTTP) getAll(w http.ResponseWriter, r *http.Request, refID string) {
	startTime := r.URL.Query().Get("start_time")
	endTime := r.URL.Query().Get("end_time")
	if startTime != "" {
		if _, err := time.Parse(time.RFC3339, startTime); err != nil {
			writeErr(w, http.StatusBadRequest, "start_time must be RFC3339")
			return
		}
	}
	if endTime != "" {
		if _, err := time.Parse(time.RFC3339, endTime); err != nil {
			writeErr(w, http.StatusBadRequest, "end_time must be RFC3339")
			return
		}
	}
	events, err := h.svc.Lookup(r.Context(), refID, startTime, endTime)
	if err != nil {
		log.Printf("[reader:http] GET /events/%s: %v", refID, err)
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(events) == 0 {
		writeErr(w, http.StatusNotFound, "no events found for ref_id")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ref_id": refID,
		"count":  len(events),
		"events": events,
	})
}

func (h *HTTP) getOne(w http.ResponseWriter, r *http.Request, refID, eventID string) {
	ev, err := h.svc.LookupOne(r.Context(), refID, eventID)
	if err != nil {
		log.Printf("[reader:http] GET /events/%s/%s: %v", refID, eventID, err)
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if ev == nil {
		writeErr(w, http.StatusNotFound, "event not found")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(v); err != nil {
		log.Printf("[reader:http] encode: %v", err)
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func splitPath(path, prefix string) []string {
	path = strings.TrimPrefix(path, prefix)
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.SplitN(path, "/", 2)
}

func unixToRFC3339(ts int64) string {
	if ts == 0 {
		return ""
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}