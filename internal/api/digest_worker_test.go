package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestBurstRequiresFiveMinutesIdle(t *testing.T) {
	s := testServer(t)
	token, _ := login(t, s, "burst@example.com")
	_, _ = call(t, s, "PUT", "/api/settings/llm", token, Object{
		"service_mode": "custom", "base_url": "http://127.0.0.1:1", "model": "m",
		"auto_analyze": true, "local_triage": false, "batch_triage": false,
	})
	for i, text := range []string{"在吗", "看下这个", "晚点说"} {
		status, out := call(t, s, "POST", "/api/messages", token, Object{
			"source": "wechat", "account_id": "a", "conversation_id": "room@chatroom",
			"external_id": "b" + string(rune('a'+i)), "text": text,
			"sent_at": "2026-11-01T10:0" + string(rune('0'+i)) + ":00Z", "sender_id": "x",
		})
		if status != 201 {
			t.Fatalf("ingest %d %v", status, out)
		}
	}
	// 刚收到的消息还不算「一段聊天结束」。
	if bursts := s.pendingDigestBursts(testContext(), false); len(bursts) != 0 {
		t.Fatalf("fresh messages must not be digested, got %d bursts", len(bursts))
	}
	// force 用于手动汇总，不受静默时间限制。
	if bursts := s.pendingDigestBursts(testContext(), true); len(bursts) != 1 || len(bursts[0].Messages) != 3 {
		t.Fatalf("forced burst should contain 3 messages, got %+v", bursts)
	}
}

func TestDigestWorkerSkipsTrivialBurst(t *testing.T) {
	s := testServer(t)
	token, _ := login(t, s, "trivial@example.com")
	_, _ = call(t, s, "PUT", "/api/settings/llm", token, Object{
		"service_mode": "custom", "base_url": "http://127.0.0.1:1", "model": "m",
		"auto_analyze": true, "local_triage": false, "batch_triage": false,
	})
	call(t, s, "POST", "/api/messages", token, Object{
		"source": "wechat", "account_id": "a", "conversation_id": "room@chatroom",
		"external_id": "t1", "text": "嗯", "sent_at": "2026-11-01T10:00:00Z", "sender_id": "x",
	})
	bursts := s.pendingDigestBursts(testContext(), true)
	if len(bursts) != 1 {
		t.Fatalf("expected one burst, got %d", len(bursts))
	}
	if worthDigesting(bursts[0].Messages) {
		t.Fatal("single short ack should not be digested")
	}
	s.markBurstNoise(testContext(), bursts[0])
	if again := s.pendingDigestBursts(testContext(), true); len(again) != 0 {
		t.Fatalf("noise burst should not reappear, got %d", len(again))
	}
}

func TestDigestBurstStoresOriginals(t *testing.T) {
	var calls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		var body Object
		json.NewDecoder(r.Body).Decode(&body)
		text := ""
		if items := objects(body["messages"]); len(items) > 0 {
			text = str(items[len(items)-1], "content")
		}
		ids := []any{}
		for _, piece := range splitOn(text, `"id":"`) {
			end := indexByte(piece, '"')
			if end > 0 {
				ids = append(ids, piece[:end])
			}
		}
		result := Object{
			"headline":   "讨论后端改造",
			"topics":     []any{Object{"title": "后端", "summary": "决定换本地记忆库", "messages": ids}},
			"decisions":  []any{"用本地记忆库"},
			"todo_hints": []any{},
		}
		payload, _ := json.Marshal(result)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Object{
			"choices": []any{Object{"message": Object{"content": string(payload)}}},
			"usage":   Object{"total_tokens": 120},
		})
	}))
	defer upstream.Close()

	s := testServer(t)
	token, _ := login(t, s, "burst2@example.com")
	call(t, s, "PUT", "/api/settings/llm", token, Object{
		"service_mode": "custom", "base_url": upstream.URL, "model": "fixture",
		"auto_analyze": false, "local_triage": false, "batch_triage": false,
	})
	for i, text := range []string{"我在考虑缝合一下开源的记忆功能", "用本地记忆库就行", "向量读取", "我看看"} {
		call(t, s, "POST", "/api/messages", token, Object{
			"source": "wechat", "account_id": "a", "conversation_id": "room@chatroom",
			"conversation_name": "内测群", "external_id": "d" + string(rune('a'+i)),
			"text": text, "sent_at": "2026-11-01T10:0" + string(rune('0'+i)) + ":00Z", "sender_id": "x",
		})
	}
	bursts := s.pendingDigestBursts(testContext(), true)
	if len(bursts) != 1 {
		t.Fatalf("expected one burst, got %d", len(bursts))
	}
	s.digestBurst(testContext(), bursts[0])
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected exactly one model call, got %d", calls)
	}
	_, snapshot := call(t, s, "GET", "/api/snapshot", token, nil)
	digests := objects(snapshot["digests"])
	if len(digests) != 1 {
		t.Fatalf("expected one digest, got %d", len(digests))
	}
	if str(digests[0], "headline") != "讨论后端改造" {
		t.Fatalf("unexpected headline %q", str(digests[0], "headline"))
	}
	if len(list(digests[0], "message_ids")) != 4 {
		t.Fatalf("digest should reference all 4 originals, got %v", digests[0]["message_ids"])
	}
	// 已被摘要的消息不应该再次进入候选。
	if again := s.pendingDigestBursts(testContext(), true); len(again) != 0 {
		t.Fatalf("digested messages must not be re-burst, got %d", len(again))
	}
	_ = time.Now
}

func splitOn(text, sep string) []string {
	parts := []string{}
	for {
		index := indexOf(text, sep)
		if index < 0 {
			return parts
		}
		text = text[index+len(sep):]
		parts = append(parts, text)
	}
}

func indexOf(text, sub string) int {
	for i := 0; i+len(sub) <= len(text); i++ {
		if text[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func indexByte(text string, target byte) int {
	for i := 0; i < len(text); i++ {
		if text[i] == target {
			return i
		}
	}
	return -1
}
