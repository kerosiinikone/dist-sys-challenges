package main

import (
	"encoding/json"
	"log"
	"runtime"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

// type Neighb struct {
// 	nm     sync.RWMutex
// 	neighb []string
// }

type SpanningTree struct {
	stm      sync.RWMutex
	parent   string
	children []string
}

type NodeMut struct {
	n   *maelstrom.Node
	jch chan *workerMsg
	Hashset
	SpanningTree
	// Neighb
}

var pool = sync.Pool{
	New: func() any {
		return new(workerMsg)
	},
}

func (n *NodeMut) createTopology(topology map[string][]string) {
	n.stm.Lock()
	defer n.stm.Unlock()

	if n.parent != "" {
		return
	}
	if len(n.children) > 0 {
		return
	}

	all := make([]string, 0)
	for node := range topology {
		all = append(all, node)
	}
	sort.Strings(all)
	root := all[0]

	n.parent = "root"
	if n.n.ID() == root {
		n.parent = ""
	}

	q := []string{root}
	visited := map[string]bool{root: true}
	parents := map[string]string{n.n.ID(): ""}

	for len(q) > 0 {
		curr := q[0]
		q = q[1:]

		for _, neighbor := range topology[curr] {
			if !visited[neighbor] {
				visited[neighbor] = true
				parents[neighbor] = curr
				q = append(q, neighbor)
			}
		}
	}

	ownParent := parents[n.n.ID()]
	if ownParent != "" {
		n.parent = ownParent
	}

	ownChildren := make([]string, 0)
	for node, parent := range parents {
		if parent == n.n.ID() {
			ownChildren = append(ownChildren, node)
		}
	}
	n.children = ownChildren
}

func (n *NodeMut) broadcast(msg maelstrom.Message) error {
	var body MsgBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.hm.Lock()
	n.hashset[body.Msg] = true
	n.hm.Unlock()

	job := pool.Get().(*workerMsg)
	job.msg = body.Msg
	job.src = msg.Src
	n.jch <- job

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
	// Assumes all new and old neighbours in the message
	// n.nm.Lock()
	// Construct a more efficient topology map ->
	// Remove the need for overlapping neighbours (duplicate messages)
	// Spanning trees
	// defer n.nm.Unlock()
	n.createTopology(body.Topology)

	return n.n.Reply(msg, map[string]string{
		"type": "topology_ok",
	})
}

func (n *NodeMut) flush(msg maelstrom.Message) error {
	var (
		body MsgBody
	)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.hm.Lock()
	if _, ok := n.hashset[body.Msg]; ok {
		n.hm.Unlock()
		return nil
	}
	n.hashset[body.Msg] = true
	n.hm.Unlock()

	job := pool.Get().(*workerMsg)
	job.msg = body.Msg
	job.src = msg.Src
	n.jch <- job

	return nil
}

func (n *NodeMut) worker() {
	b := MsgBody{
		Typ: "flush",
	}
	for msg := range n.jch {
		n.stm.RLock()
		temp := make([]string, len(n.children))
		copy(temp, n.children)
		n.stm.RUnlock()
		b.Msg = msg.msg

		for _, ne := range temp {
			// if ne == msg.src {
			// 	continue
			// }
			if err := n.n.Send(ne, b); err != nil {
				// Retry -> fault-tolerance?
				continue
			}
		}
		pool.Put(msg)
	}
}

func main() {
	var (
		node = NodeMut{
			n:   maelstrom.NewNode(),
			jch: make(chan *workerMsg, 256), // Buffer amount?
			Hashset: Hashset{
				hashset: map[float64]bool{},
				hm:      sync.RWMutex{},
			},
			SpanningTree: SpanningTree{
				stm:      sync.RWMutex{},
				children: make([]string, 0),
			},
		}
	)

	node.n.Handle("broadcast", node.broadcast)
	node.n.Handle("flush", node.flush)
	node.n.Handle("read", node.read)
	node.n.Handle("topology", node.topology)

	numWorkers := 2 * runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go node.worker()
	}

	if err := node.n.Run(); err != nil {
		log.Fatal(err)
	}
}
