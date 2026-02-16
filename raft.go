package main

import (
	"context"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type RaftNode struct {
	proposeC    <-chan string
	commitC     chan<- string
	node        raft.Node
	raftStorage *raft.MemoryStorage
}

func NewRaftNode(id uint64, proposeC <-chan string, commitC chan<- string) *RaftNode {
	storage := raft.NewMemoryStorage()
	config := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	peers := []raft.Peer{{ID: id}}
	node := raft.StartNode(config, peers)
	rn := &RaftNode{
		proposeC:    proposeC,
		commitC:     commitC,
		node:        node,
		raftStorage: storage,
	}
	go rn.serveChannels()
	return rn
}

func (rn *RaftNode) serveChannels() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		case data := <-rn.proposeC:
			rn.node.Propose(context.TODO(), []byte(data))
		case rd := <-rn.node.Ready():
			if !raft.IsEmptyHardState(rd.HardState) {
				rn.raftStorage.SetHardState(rd.HardState)
			}

			rn.raftStorage.Append(rd.Entries)

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
					// Send data to KVStore for application
					rn.commitC <- string(entry.Data)
				}
			}

			rn.node.Advance()
		}
	}
}
