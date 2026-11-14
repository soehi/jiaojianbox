package api

// 摘要 worker：把同一会话里「一段连续聊天」在静默之后自动汇总。
//
// 分段规则：同一会话的消息按到达时间排序，若最后一条之后 5 分钟内没有新消息，
// 这一批就视为一次聊天结束，触发一次摘要。这样摘要对应的是「一场对话」，
// 而不是任意时间窗口。

import (
	"context"
	"strings"
	"time"

	"github.com/NekoYume517/jiaojianbox/internal/store"
)

const (
	// 会话静默多久算一次聊天结束。
	digestIdleWindow = 5 * time.Minute
	// 单次摘要最多纳入多少条，避免长会话一次塞爆上下文。
	digestBurstLimit = 120
	// 全部是低价值闲聊且条数很少时不单独生成摘要。
	digestTrivialCount = 4
	digestTrivialChars = 40
)

type digestBurst struct {
	User         string
	Source       string
	Account      string
	Conversation string
	Name         string
	Messages     []Object
}

func burstKey(m Object) string {
	return strings.Join([]string{
		str(m, "user_id"), str(m, "source"), str(m, "account_id"), str(m, "conversation_id"),
	}, "\x00")
}

// pendingDigestBursts 返回「已经静默足够久、可以直接摘要」的会话批次。
func (s *Server) pendingDigestBursts(ctx context.Context, force bool) []digestBurst {
	rows := query(ctx, s.Store.DB, `SELECT user_id,payload FROM messages
		WHERE json_extract(payload,'$.digest_id') IS NULL
		  AND COALESCE(json_extract(payload,'$.digest_state'),'') = ''
		  AND COALESCE(json_extract(payload,'$.triage'),'') <> 'ignored'
		  AND COALESCE(json_extract(payload,'$.triage_state'),'') <> 'pending'
		ORDER BY rowid LIMIT 2000`)
	groups := map[string]*digestBurst{}
	order := []string{}
	for _, row := range rows {
		m := store.Payload(row)
		owner := str(row, "user_id")
		if owner == "" {
			owner = str(m, "user_id")
		}
		m["user_id"] = owner
		key := burstKey(m)
		burst := groups[key]
		if burst == nil {
			burst = &digestBurst{
				User: owner, Source: str(m, "source"), Account: str(m, "account_id"),
				Conversation: str(m, "conversation_id"), Name: str(m, "conversation_name"),
			}
			groups[key] = burst
			order = append(order, key)
		}
		burst.Messages = append(burst.Messages, m)
	}
	now := time.Now()
	ready := []digestBurst{}
	for _, key := range order {
		burst := groups[key]
		if len(burst.Messages) == 0 {
			continue
		}
		// 用「收到消息的时间」判断是否还在聊，避免历史补录的旧 sent_at 直接触发。
		newest := time.Time{}
		for _, m := range burst.Messages {
			if at, err := time.Parse(time.RFC3339Nano, str(m, "created_at")); err == nil && at.After(newest) {
				newest = at
			}
		}
		if newest.IsZero() {
			continue
		}
		if !force && now.Sub(newest) < digestIdleWindow {
			continue
		}
		messages := burst.Messages
		if len(messages) > digestBurstLimit {
			messages = messages[len(messages)-digestBurstLimit:]
		}
		ready = append(ready, digestBurst{
			User: burst.User, Source: burst.Source, Account: burst.Account,
			Conversation: burst.Conversation, Name: burst.Name, Messages: messages,
		})
	}
	return ready
}

// worthDigesting 判断这一批是否值得单独生成摘要。
// 全部是很短的闲聊、且没有任何可操作信号时，直接标记为已处理，避免刷屏。
func worthDigesting(messages []Object) bool {
	if len(messages) >= digestTrivialCount {
		return true
	}
	total := 0
	for _, m := range messages {
		total += len([]rune(str(m, "text")))
		if len(object(m["analysis"])) > 0 {
			return true
		}
		if verdict := object(m["triage_result"]); str(verdict, "verdict") == "actionable" {
			return true
		}
		if local := object(m["triage_local"]); str(local, "verdict") == "strong" {
			return true
		}
	}
	return total >= digestTrivialChars
}

// markBurstNoise 把不值得摘要的消息标记掉，避免它们反复进入候选。
func (s *Server) markBurstNoise(ctx context.Context, burst digestBurst) {
	s.transaction(ctx, func(db store.SQL) any {
		for _, m := range burst.Messages {
			exec(ctx, db, `UPDATE messages SET payload=json_set(payload,'$.digest_state','noise')
				WHERE id=? AND user_id=?`, str(m, "id"), burst.User)
		}
		return nil
	})
}

func (s *Server) digestWorker(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := attempt(func() any {
				for _, burst := range s.pendingDigestBursts(ctx, false) {
					if ctx.Err() != nil {
						return nil
					}
					if !worthDigesting(burst.Messages) {
						s.markBurstNoise(ctx, burst)
						continue
					}
					callCtx, stop := context.WithTimeout(ctx, 150*time.Second)
					s.digestBurst(callCtx, burst)
					stop()
				}
				return nil
			})
			if err != nil && ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
		}
	}
}

// digestBurst 直接对一批已分好段的消息生成摘要；失败时保留原文等待下一轮。
func (s *Server) digestBurst(ctx context.Context, burst digestBurst) {
	if len(burst.Messages) == 0 {
		return
	}
	config, key := s.resolveLLM(ctx, burst.User)
	if str(config, "base_url") == "" || str(config, "model") == "" {
		return
	}
	items := []digestItem{}
	for _, m := range burst.Messages {
		items = append(items, digestItemOf(m))
	}
	payload := Object{
		"CONVERSATION": Object{"name": fallback(burst.Name, burst.Conversation), "source": burst.Source},
		"WINDOW": Object{
			"from":  str(burst.Messages[0], "sent_at"),
			"to":    str(burst.Messages[len(burst.Messages)-1], "sent_at"),
			"count": len(items),
		},
		"MESSAGES": items,
	}
	prompt := digestPrompt
	if instruction := str(config, "instruction"); instruction != "" {
		prompt += "\n用户的关注偏好：\n" + instruction
	}
	conversation := []Object{
		{"role": "system", "content": prompt},
		{"role": "user", "content": store.JSON(payload)},
	}
	started := time.Now()
	metrics := Object{}
	success := false
	defer func() {
		safe, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		attempt(func() any {
			s.recordLLM(safe, burst.User, "digest", config, started, metrics, success)
			return nil
		})
	}()
	call := s.chat(ctx, config, key, conversation, nil, nil,
		func(part Object) { metrics = mergeMetrics(metrics, part) },
		Object{"json_mode": true, "max_tokens": 4000})
	raw := robustJSON(call.Text)
	success = true

	topics, orphan := buildDigestTopics(raw, items)
	result := Object{
		"id":                store.ID(),
		"source":            burst.Source,
		"account_id":        burst.Account,
		"conversation_id":   burst.Conversation,
		"conversation_name": fallback(burst.Name, burst.Conversation),
		"headline":          clip(fmtValue(raw["headline"]), 200),
		"topics":            topics,
		"decisions":         textList(raw["decisions"], 3, 200),
		"todo_hints":        textList(raw["todo_hints"], 3, 200),
		"message_ids":       collectIDs(items),
		"message_count":     len(items),
		"from":              str(burst.Messages[0], "sent_at"),
		"to":                str(burst.Messages[len(burst.Messages)-1], "sent_at"),
		"model":             str(config, "model"),
		"created_at":        store.Now(),
		"performance":       metrics,
	}
	_ = orphan
	s.transaction(ctx, func(db store.SQL) any {
		exec(ctx, db, `INSERT INTO digests(id,user_id,source,account_id,conversation_id,payload,created_at)
			VALUES(?,?,?,?,?,?,?)`,
			result["id"], burst.User, burst.Source, burst.Account, burst.Conversation,
			store.JSON(result), result["created_at"])
		for _, item := range items {
			exec(ctx, db, `UPDATE messages SET payload=json_set(payload,'$.digest_id',?)
				WHERE id=? AND user_id=?`, result["id"], item.ID, burst.User)
		}
		event(ctx, db, burst.User, "digest.upsert", str(result, "id"))
		return nil
	})
}
