package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Body struct {
	Typ      string              `json:"type"`
	ID       uint                `json:"msg_id"`
	Msg      interface{}         `json:"message,omitempty"`
	Msgs     interface{}         `json:"messages,omitempty"`
	Topology map[string][]string `json:"topology,omitempty"`
}

func main() {
	// TODO: Main struct to hold the "main lock" mutex -> not globally
	var (
		// For now
		// counter uint = 0
		mut sync.RWMutex

		n       = maelstrom.NewNode()
		hashset = map[float64]bool{}
		nlog    = map[float64]bool{}
		neighb  = make([]string, 0)
	)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mut.Lock()
		hashset[body.Msg.(float64)] = true
		nlog[body.Msg.(float64)] = true
		mut.Unlock()

		// counter = counter + 1
		// currentID := counter

		// for _, r := range templist {
		// 	if r == msg.Src {
		// 		continue
		// 	}
		// 	if err := n.RPC(r, Body{
		// 		Typ: "broadcast",
		// 		ID:  body.ID + 1,
		// 		Msg: body.Msg,
		// 	}, func(msg maelstrom.Message) error {
		// 		var body Body
		// 		if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 			return err
		// 		}
		// 		if body.Typ != "broadcast_ok" {
		// 			return fmt.Errorf("wrong type")
		// 		}
		// 		return nil
		// 	}); err != nil {
		// 		continue
		// 	}
		// }

		return n.Reply(msg, Body{
			ID:  body.ID + 1,
			Typ: "broadcast_ok",
		})
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mut.RLock()
		defer mut.RUnlock()

		output := make([]float64, 0, len(hashset))
		for k := range hashset {
			output = append(output, k)
		}

		// mut.Lock()
		// counter = counter + 1
		// currentID := counter
		// mut.Unlock()

		return n.Reply(msg, Body{
			ID:   body.ID + 1,
			Typ:  "read_ok",
			Msgs: output,
		})
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if _, ok := body.Topology[msg.Dest]; !ok {
			panic("no neighbours")
		}
		// Assumes all new and old neighbours in the message
		mut.Lock()
		neighb = body.Topology[msg.Dest]
		mut.Unlock()

		// mut.Lock()
		// counter = counter + 1
		// currentID := counter
		// mut.Unlock()

		return n.Reply(msg, Body{
			ID:  body.ID + 1,
			Typ: "topology_ok",
		})
	})

	n.Handle("flush", func(msg maelstrom.Message) error {
		var (
			body Body
		)
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for _, msg := range body.Msg.([]interface{}) {
			mut.Lock()
			hashset[msg.(float64)] = true
			nlog[msg.(float64)] = true
			mut.Unlock()
		}

		return nil
	})

	go func() {
		tckr := time.NewTicker(200 * time.Millisecond)
		defer tckr.Stop()

		for range tckr.C {
			mut.Lock()
			if len(nlog) == 0 {
				mut.Unlock()
				continue
			}
			output := make([]float64, 0, len(nlog))
			for l := range nlog {
				output = append(output, l)
			}
			nlog = make(map[float64]bool)
			mut.Unlock()

			// TODO: The temp copy trick to release the lock?
			mut.RLock()
			for _, ne := range neighb {
				if err := n.Send(ne, Body{
					Typ: "flush",
					Msg: output,
				}); err != nil {
					continue
				}
			}
			mut.RUnlock()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
