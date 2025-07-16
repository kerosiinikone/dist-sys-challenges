package main

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Body struct {
	// Provided by the RPC:
	// Reply uint        `json:":in_reply_to,omitempty"`
	Typ  string      `json:"type"`
	ID   uint        `json:"msg_id"`
	Msg  interface{} `json:"message,omitempty"`
	Msgs interface{} `json:"messages,omitempty"`
}

func main() {
	var (
		mut     sync.RWMutex
		counter uint = 0
		n            = maelstrom.NewNode()
		hashset      = map[float64]bool{}
	)
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mut.Lock()
		hashset[body.Msg.(float64)] = true
		counter = counter + 1
		currentID := counter
		mut.Unlock()

		return n.Reply(msg, Body{
			ID:  currentID,
			Typ: "broadcast_ok",
		})
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mut.RLock()
		output := make([]float64, 0, len(hashset))
		for k := range hashset {
			output = append(output, k)
		}
		mut.RUnlock()

		mut.Lock()
		counter = counter + 1
		currentID := counter
		mut.Unlock()

		return n.Reply(msg, Body{
			ID:   currentID,
			Typ:  "read_ok",
			Msgs: output,
		})
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// For now
		mut.Lock()
		counter = counter + 1
		currentID := counter
		mut.Unlock()

		return n.Reply(msg, Body{
			ID:  currentID,
			Typ: "topology_ok",
		})
	})
	if err := n.Run(); err != nil {
		panic(err)
	}
}
