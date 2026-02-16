package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

type RaftNode struct {
	ID uint64

	proposeC    <-chan string
	commitC     chan<- string
	node        raft.Node
	raftStorage *raft.MemoryStorage

	waldir string

	wal *wal.WAL
}

func NewRaftNode(id uint64, proposeC <-chan string, commitC chan<- string) *RaftNode {

	rn := &RaftNode{
		proposeC: proposeC,
		commitC:  commitC,
		waldir:   fmt.Sprintf("wal-%d", id),
		ID:       id,
	}
	go rn.StartRaft()
	return rn
}
func (rn *RaftNode) StartRaft() {
	rn.raftStorage = raft.NewMemoryStorage()
	oldwal := wal.Exist(rn.waldir)
	if !oldwal {
		w, err := wal.Create(zap.L(), rn.waldir, nil)
		if err != nil {
			panic(err)
		}
		w.Close()
	}
	wal, err := wal.Open(zap.L(), rn.waldir, walpb.Snapshot{})
	if err != nil {
		panic(err)
	}
	rn.wal = wal
	_, state, ets, err := rn.wal.ReadAll()
	if err != nil {
		panic(err)
	}
	rn.raftStorage.SetHardState(state)
	rn.raftStorage.Append(ets)
	config := &raft.Config{
		ID:              rn.ID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		rn.node = raft.RestartNode(config)
	} else {
		peers := []raft.Peer{{ID: rn.ID}}
		rn.node = raft.StartNode(config, peers)
	}

	// ITERATE AND REPLAY COMMITTED ENTRIES
	// This restores the state of the KVStore AND the Raft Configuration
	for _, entry := range ets {
		if entry.Index <= state.Commit {
			switch entry.Type {
			case raftpb.EntryNormal:
				if len(entry.Data) > 0 {
					rn.commitC <- string(entry.Data)
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				cc.Unmarshal(entry.Data)
				rn.node.ApplyConfChange(cc)
			}
		}
	}

	go rn.serveChannels()
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

			err := rn.wal.Save(rd.HardState, rd.Entries)
			if err != nil {
				panic(err)
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				rn.raftStorage.SetHardState(rd.HardState)
			}

			rn.raftStorage.Append(rd.Entries)

			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
					if len(entry.Data) > 0 {
						// Send data to KVStore for application
						rn.commitC <- string(entry.Data)
					}
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					rn.node.ApplyConfChange(cc)
				}
			}

			rn.node.Advance()
		}
	}
}
