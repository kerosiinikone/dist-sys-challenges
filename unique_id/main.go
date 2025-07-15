package main

import (
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func newId(n *maelstrom.Node, body map[string]any) string {
	if _, ok := body["msg_id"]; !ok {
		panic("No msg id")
	}
	return fmt.Sprintf("%s_%v", n.ID(), body["msg_id"])
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = newId(n, body)

		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		panic(err)
	}
}
