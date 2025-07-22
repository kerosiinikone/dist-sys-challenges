package main

import (
	"encoding/json"
	"log"
	"runtime"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// const SYNC_LATENCY = 45

type workerMsg struct {
	msg float64
	src string
}

type MsgBody struct {
	Typ string  `json:"type"`
	Msg float64 `json:"message,omitempty"`
}

type ReadBody struct {
	ID   int       `json:"msg_id"`
	Typ  string    `json:"type"`
	Msgs []float64 `json:"messages"`
}

type TopoBody struct {
	Topology map[string][]string `json:"topology,omitempty"`
}

type Hashset struct {
	hm      sync.RWMutex
	hashset map[float64]bool
}

type Neighb struct {
	nm     sync.RWMutex
	neighb []string
}

type NodeMut struct {
	n   *maelstrom.Node
	jch chan *workerMsg
	Hashset
	Neighb
	// nlog    map[float64]bool
}

var pool = sync.Pool{
	New: func() any {
		return new(workerMsg)
	},
}

func (n *NodeMut) broadcastOk(msg maelstrom.Message) error {
	return nil
}

func (n *NodeMut) broadcast(msg maelstrom.Message) error {
	var body MsgBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.hm.Lock()
	if _, ok := n.hashset[body.Msg]; ok {
		// Seen -> skip
		n.hm.Unlock()
		return n.n.Reply(msg, map[string]string{
			"type": "broadcast_ok",
		})
	}
	n.hashset[body.Msg] = true
	n.hm.Unlock()

	job := pool.Get().(*workerMsg)
	job.msg = body.Msg
	job.src = msg.Src
	n.jch <- job

	// n.nlog[body.Msg.(float64)] = true

	// counter = counter + 1
	// currentID := counter

	return n.n.Reply(msg, map[string]string{
		"type": "broadcast_ok",
	})
}

func (n *NodeMut) read(msg maelstrom.Message) error {
	var body ReadBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.hm.RLock()
	defer n.hm.RUnlock()
	output := make([]float64, 0, len(n.hashset))
	for k := range n.hashset {
		output = append(output, k)
	}

	// mut.Lock()
	// counter = counter + 1
	// currentID := counter
	// mut.Unlock()

	return n.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": output,
	})
}

func (n *NodeMut) topology(msg maelstrom.Message) error {
	var body TopoBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// if _, ok := body.Topology[msg.Dest]; !ok {
	// 	panic("no neighbours")
	// }

	// Assumes all new and old neighbours in the message
	n.nm.Lock()
	defer n.nm.Unlock()
	n.neighb = body.Topology[msg.Dest]

	// mut.Lock()
	// counter = counter + 1
	// currentID := counter
	// mut.Unlock()

	return n.n.Reply(msg, map[string]string{
		"type": "topology_ok",
	})
}

// func (n *NodeMut) _flush(msg maelstrom.Message) error {
// 	var (
// 		body Body
// 	)
// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
// 		return err
// 	}
// 	n.mut.Lock()
// 	defer n.mut.Unlock()
// 	for _, msg := range body.Msg.([]interface{}) {
// 		n.hashset[msg.(float64)] = true
// 		// n.nlog[msg.(float64)] = true
// 	}

// 	return nil
// }

// func (n *NodeMut) _syncLoop() {
// 	tckr := time.NewTicker(SYNC_LATENCY * time.Millisecond)
// 	defer tckr.Stop()

// 	for range tckr.C {
// 		n.mut.Lock()
// 		if len(n.nlog) == 0 {
// 			n.mut.Unlock()
// 			continue
// 		}
// 		output := make([]float64, 0, len(n.nlog))
// 		for l := range n.nlog {
// 			output = append(output, l)
// 		}
// 		n.nlog = make(map[float64]bool)
// 		temp := make([]string, len(n.neighb))
// 		copy(temp, n.neighb)
// 		n.mut.Unlock()

// 		for _, ne := range temp {
// 			if err := n.n.Send(ne, Body{
// 				Typ: "flush",
// 				Msg: output,
// 			}); err != nil {
// 				continue
// 			}
// 		}
// 	}
// }

func (n *NodeMut) worker() {
	b := MsgBody{
		Typ: "broadcast",
	}
	for msg := range n.jch {
		n.nm.RLock()
		temp := make([]string, len(n.neighb))
		copy(temp, n.neighb)
		n.nm.RUnlock()
		b.Msg = msg.msg

		for _, ne := range temp {
			if ne == msg.src {
				continue
			}
			if err := n.n.Send(ne, b); err != nil {
				continue
			}
		}
		pool.Put(msg)
	}
}

func main() {
	var (
		// For now
		// counter float64 = 0
		node = NodeMut{
			// nlog:    map[float64]bool{},
			n:   maelstrom.NewNode(),
			jch: make(chan *workerMsg, 256), // Buffer amount?
			Hashset: Hashset{
				hashset: map[float64]bool{},
				hm:      sync.RWMutex{},
			},
			Neighb: Neighb{
				neighb: make([]string, 0),
				nm:     sync.RWMutex{},
			},
		}
	)

	node.n.Handle("broadcast", node.broadcast)
	node.n.Handle("broadcast_ok", node.broadcastOk)
	node.n.Handle("read", node.read)
	node.n.Handle("topology", node.topology)
	// node.n.Handle("flush", node.flush)

	// go node.syncLoop()

	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go node.worker()
	}

	if err := node.n.Run(); err != nil {
		log.Fatal(err)
	}
}
