package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

type RaftNode struct {
	ID int

	proposeC    <-chan string
	commitC     chan<- string
	node        raft.Node
	raftStorage *raft.MemoryStorage

	waldir string

	wal *wal.WAL

	transport *rafthttp.Transport
	peers     []string
}

func NewRaftNode(id int, peers []string, proposeC <-chan string, commitC chan<- string) *RaftNode {

	rn := &RaftNode{
		proposeC: proposeC,
		commitC:  commitC,
		waldir:   fmt.Sprintf("wal-%d", id),
		peers:    peers,
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
		ID:              uint64(rn.ID),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		rn.node = raft.RestartNode(config)
	} else {
		rpeers := make([]raft.Peer, len(rn.peers))
		for i := range rpeers {
			rpeers[i] = raft.Peer{ID: uint64(i + 1)}
		}
		rn.node = raft.StartNode(config, rpeers)
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
	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.ID),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: v2stats.NewServerStats(strconv.Itoa(rn.ID), strconv.Itoa(rn.ID)),
		LeaderStats: v2stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(int(rn.ID))),
		ErrorC:      make(chan error),
	}
	rn.transport.Start()
	for i := range rn.peers {
		if i+1 != rn.ID {
			rn.transport.AddPeer(types.ID(i+1), []string{rn.peers[i]})
		}
	}

	go rn.serveRaft()
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

			rn.transport.Send(rd.Messages)

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
func (rc *RaftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.ID-1])
	if err != nil {
		panic(err)
	}
	err = http.ListenAndServe(url.Host, rc.transport.Handler())
	if err != nil {
		panic(err)
	}
}

func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *RaftNode) IsIDRemoved(_ uint64) bool   { return false }
func (rc *RaftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
