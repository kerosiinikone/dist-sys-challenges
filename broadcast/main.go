package main

import (
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Body struct {
	// Provided by the RPC:
	// Reply    uint                `json:":in_reply_to,omitempty"`
	Typ      string              `json:"type"`
	ID       uint                `json:"msg_id"`
	Msg      interface{}         `json:"message,omitempty"`
	Msgs     interface{}         `json:"messages,omitempty"`
	Topology map[string][]string `json:"topology,omitempty"`
}

func main() {
	var (
		mut     sync.RWMutex
		counter uint = 0
		n            = maelstrom.NewNode()
		hashset      = map[float64]bool{}
		neighb       = make([]string, 0)
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

		mut.RLock()
		for _, r := range neighb {
			if r == msg.Src {
				continue
			}
			if err := n.RPC(r, Body{
				Typ: "broadcast",
				ID:  currentID,
				Msg: body.Msg,
			}, func(msg maelstrom.Message) error {
				var body Body
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				if body.Typ != "broadcast_ok" {
					return fmt.Errorf("wrong type")
				}
				return nil
			}); err != nil {
				// Not partition tolerant but works for this simple case
				panic(err)
			}

		}
		mut.RUnlock()

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

		mut.Lock()
		if _, ok := body.Topology[msg.Dest]; !ok {
			panic("no neighbours")
		}
		// Figure out your neighbours!
		neighb = body.Topology[msg.Dest]

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
