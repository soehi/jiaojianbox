package api

import (
	"strings"
)

// buildDigestTopics 把模型返回的话题整理成可存储结构，并保证每条原文都能落到某个话题里。
func buildDigestTopics(raw Object, items []digestItem) ([]Object, []any) {
	idSet := map[string]bool{}
	for _, item := range items {
		idSet[item.ID] = true
	}
	topics := []Object{}
	seen := map[string]bool{}
	for _, value := range asList(raw["topics"], 3) {
		topic := object(value)
		title := strings.TrimSpace(clip(fmtValue(topic["title"]), 40))
		if title == "" || seen[title] {
			continue
		}
		seen[title] = true
		ids := []any{}
		for _, v := range asList(topic["messages"], 40) {
			identity := fmtValue(v)
			if idSet[identity] {
				ids = append(ids, identity)
				delete(idSet, identity)
			}
		}
		topics = append(topics, Object{
			"title":    title,
			"summary":  clip(fmtValue(topic["summary"]), 200),
			"messages": ids,
		})
	}
	if len(topics) == 0 {
		topics = append(topics, Object{
			"title":    "闲聊概览",
			"summary":  clip(fmtValue(raw["headline"]), 200),
			"messages": []any{},
		})
	}
	orphan := []any{}
	for _, item := range items {
		if idSet[item.ID] {
			orphan = append(orphan, item.ID)
			delete(idSet, item.ID)
		}
	}
	if len(orphan) > 0 {
		last := topics[len(topics)-1]
		last["messages"] = append(list(last, "messages"), orphan...)
	}
	return topics, orphan
}
