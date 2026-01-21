package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mpaxos_hw1/db"
	node "mpaxos_hw1/proto/hw1/node"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type LogEntry struct {
	idx       int64
	status    string
	timestamp string
	seq       int64
	ballot    int32
	txn       *node.TxnMessage
	node      string
}

type TwoPCLogEntry struct {
	status      string // 2PC status (e.g., P/A)
	paxosStatus string // Paxos status (Accepted/Commit) for that 2PC record
	timestamp   string
	seq         int64
	ballot      int32
	txn         *node.TxnMessage
	node        string
	phase       node.TwoPCPhase
	beforeFrom  int64
	beforeTo    int64
	trackFrom   bool
	trackTo     bool
}

type walSnapshot struct {
	beforeFrom int64
	beforeTo   int64
	trackFrom  bool
	trackTo    bool
}

type ViewEntry struct {
	ballot int32
	node   string
	log    []*node.AcceptLog
	leader string
}

// newViewRecord captures one NewView message (sent or received) for later printing.
type newViewRecord struct {
	sender     string
	ballot     int32
	entryCount int64
	timestamp  string
}

type Node struct {
	node.UnimplementedNodeServer
	logTable           []*LogEntry
	logMap             map[int64]*LogEntry // latest paxos state per seq
	ports              []string
	seq                int64
	addr               string
	ballot             int32
	disabled           bool
	acceptedBuf        chan *node.AcceptedMsg
	pendingAccepted    map[int64]chan *node.AcceptedMsg
	lock               sync.Mutex
	db                 map[string]float64
	store              *db.Store
	dbFile             string
	leader             string
	clients            map[int64]chan *node.Acknowledgement
	promiseLogs        map[string][]*node.AcceptLog
	grpcConns          map[string]node.NodeClient
	lastReply          map[string]*node.Reply
	electionInProgress bool
	viewTable          []*ViewEntry
	lastViewWritten    int32
	viewHistory        []newViewRecord
	pendingQueue       []*queuedTxn
	awaiting           map[string]chan *node.Reply
	executeIdx         int
	twoPCLog           map[int64]*TwoPCLogEntry
	twoPCSeq           int64
	pendingLocks       map[int64]lockInfo

	hbTimer         *time.Timer
	hbTimeout       time.Duration
	hbInterval      time.Duration
	lastPrepareTime time.Time
	prepareCooldown time.Duration

	checkpointInterval   int
	lastCheckpointSeq    int64
	lastCheckpointBallot int32
	lastCheckpointDigest string
	pendingCpSeq         int64
	pendingCpDigest      string

	locks *db.LockTable

	recovering bool
	// disabledFlag   int32
	// recoveringFlag int32
	pending2PC []*node.Prepare2PCReq
}

// ClusterConfig describes one shard/cluster and its nodes.
type ClusterConfig struct {
	Name       string   `json:"name,omitempty"`
	StartID    int      `json:"start_id"`
	EndID      int      `json:"end_id"`
	Nodes      []string `json:"nodes"`
	Size       int      `json:"size,omitempty"`
	LeaderHint string   `json:"leader_hint,omitempty"`
}

// Config holds the runtime cluster layout.
type Config struct {
	Clusters       []ClusterConfig `json:"clusters"`
	TotalIDs       int             `json:"total_ids,omitempty"`
	DefaultBalance int64           `json:"default_balance,omitempty"`
}

const redistributionFile = "config/redistribution_map.json"

var (
	clusterCfg      Config
	addrToCluster   map[string]*ClusterConfig
	defaultBalance  int64 = 10
	defaultTotalIDs int   = 9000
	redistributed         = make(map[int]int) // account ID -> cluster index override
)

// defaultClusterConfig returns the legacy static layout (3 clusters x 3 nodes).
func defaultClusterConfig() Config {
	return Config{
		Clusters: []ClusterConfig{
			{StartID: 1, EndID: 3000, Nodes: []string{":5001", ":5002", ":5003"}},
			{StartID: 3001, EndID: 6000, Nodes: []string{":5004", ":5005", ":5006"}},
			{StartID: 6001, EndID: 9000, Nodes: []string{":5007", ":5008", ":5009"}},
		},
		TotalIDs:       defaultTotalIDs,
		DefaultBalance: defaultBalance,
	}
}

// loadClusterConfig loads cluster layout from CLUSTER_CONFIG (JSON) if provided,
// otherwise falls back to the default static layout.
func loadClusterConfig() {
	clusterCfg = defaultClusterConfig()
	path := os.Getenv("CLUSTER_CONFIG")
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var parsed Config
			if err := json.Unmarshal(data, &parsed); err != nil {
				log.Printf("Failed to parse CLUSTER_CONFIG %s: %v", path, err)
			} else if len(parsed.Clusters) > 0 {
				clusterCfg = parsed
			}
		} else {
			log.Printf("Failed to read CLUSTER_CONFIG %s: %v", path, err)
		}
	}
	if clusterCfg.TotalIDs <= 0 {
		clusterCfg.TotalIDs = defaultTotalIDs
	}
	if clusterCfg.DefaultBalance <= 0 {
		clusterCfg.DefaultBalance = defaultBalance
	}
	if len(clusterCfg.Clusters) == 0 {
		clusterCfg = defaultClusterConfig()
	}
	// Auto-assign ranges if missing.
	needAssign := false
	for _, c := range clusterCfg.Clusters {
		if c.StartID == 0 && c.EndID == 0 {
			needAssign = true
			break
		}
	}
	if needAssign {
		span := clusterCfg.TotalIDs / len(clusterCfg.Clusters)
		rem := clusterCfg.TotalIDs % len(clusterCfg.Clusters)
		cur := 1
		for i := range clusterCfg.Clusters {
			size := span
			if i == len(clusterCfg.Clusters)-1 {
				size += rem
			}
			clusterCfg.Clusters[i].StartID = cur
			clusterCfg.Clusters[i].EndID = cur + size - 1
			cur += size
		}
	}
	loadRedistributionMap()
	addrToCluster = make(map[string]*ClusterConfig)
	for i := range clusterCfg.Clusters {
		for _, addr := range clusterCfg.Clusters[i].Nodes {
			addrToCluster[addr] = &clusterCfg.Clusters[i]
		}
	}
}

// loadRedistributionMap loads any persisted id->cluster mapping overrides.
func loadRedistributionMap() {
	data, err := os.ReadFile(redistributionFile)
	if err != nil {
		return
	}
	var raw map[string]int
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Printf("server: failed to parse redistribution map: %v", err)
		return
	}
	tmp := make(map[int]int, len(raw))
	for k, v := range raw {
		id, err := strconv.Atoi(k)
		if err != nil || v < 0 || v >= len(clusterCfg.Clusters) {
			continue
		}
		tmp[id] = v
	}
	redistributed = tmp
	if len(redistributed) > 0 {
		log.Printf("server: loaded redistribution map for %d accounts", len(redistributed))
	}
}

// clusterForAddr returns the cluster containing the given node addr.
func clusterForAddr(addr string) *ClusterConfig {
	if c, ok := addrToCluster[addr]; ok {
		return c
	}
	return nil
}

// shardForAccount returns the cluster config for a given account id, honoring redistribution overrides.
func shardForAccount(id int) *ClusterConfig {
	if idx, ok := redistributed[id]; ok && idx >= 0 && idx < len(clusterCfg.Clusters) {
		return &clusterCfg.Clusters[idx]
	}
	for i := range clusterCfg.Clusters {
		c := &clusterCfg.Clusters[i]
		if id >= c.StartID && id <= c.EndID {
			return c
		}
	}
	return nil
}

// findShardLeader queries peers in this shard to find the current leader address.
func (n *Node) findShardLeader() string {
	peers := n.clusterPeers()
	for _, p := range peers {
		n.lock.Lock()
		client := n.grpcConns[p]
		n.lock.Unlock()
		if client == nil && p != n.addr {
			conn, err := grpc.Dial(p, grpc.WithInsecure(), grpc.WithTimeout(800*time.Millisecond))
			if err == nil {
				client = node.NewNodeClient(conn)
				n.lock.Lock()
				n.grpcConns[p] = client
				n.lock.Unlock()
			}
		}
		if p == n.addr {
			n.lock.Lock()
			isLeader := n.leader == n.addr && !n.disabled && !n.recovering
			n.lock.Unlock()
			if isLeader {
				return n.addr
			}
		}
		if client == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := client.PrintStatus(ctx, &node.PrintStatusRequest{})
		cancel()
		if err != nil || resp == nil || len(resp.Statuses) == 0 {
			continue
		}
		if resp.Statuses[0].Status == "LEADER" {
			return p
		}
	}
	return ""
}

// catchupFromLeader pulls logs from the shard leader, merges, and replays commits.
func (n *Node) catchupFromLeader() {
	leader := n.findShardLeader()
	if leader == "" {
		log.Printf("[%s] catchup: no leader found", n.addr)
		return
	}
	if leader != n.addr {
		log.Printf("[%s] catchup: fetching logs from %s", n.addr, leader)
	} else {
		log.Printf("[%s] catchup: self is leader, using local logs", n.addr)
	}

	var rows []*node.GetLogsResp_Row
	if leader != n.addr {
		client := n.grpcConns[leader]
		if client == nil {
			conn, err := grpc.Dial(leader, grpc.WithInsecure())
			if err != nil {
				log.Printf("[%s] catchup: dial leader %s failed: %v", n.addr, leader, err)
				return
			}
			defer conn.Close()
			client = node.NewNodeClient(conn)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		resp, err := client.GetLogs(ctx, &node.GetLogsReq{})
		cancel()
		if err != nil || resp == nil {
			log.Printf("[%s] catchup: GetLogs from %s failed: %v", n.addr, leader, err)
			return
		}
		rows = resp.Rows
	} else {
		// local snapshot
		n.lock.Lock()
		for _, e := range n.logTable {
			rows = append(rows, &node.GetLogsResp_Row{
				Status:    e.status,
				Seq:       e.seq,
				Ballot:    e.ballot,
				From:      e.txn.Txn.From,
				To:        e.txn.Txn.To,
				Amount:    e.txn.Txn.Amount,
				Node:      e.node,
				Timestamp: e.timestamp,
			})
		}
		n.lock.Unlock()
	}

	var acceptLogs []*node.AcceptLog
	for _, r := range rows {
		if r == nil {
			continue
		}
		acceptLogs = append(acceptLogs, &node.AcceptLog{
			Ballot:    r.Ballot,
			Seq:       r.Seq,
			Txn:       &node.TxnMessage{Txn: &node.Transaction{From: r.From, To: r.To, Amount: r.Amount}, Timestamp: r.Timestamp},
			Status:    r.Status,
			Timestamp: r.Timestamp,
			Node:      r.Node,
		})
	}

	n.lock.Lock()
	n.logTable = mergeLogs(n.logTable, acceptLogs)
	n.logMap = latestLogBySeq(n.logTable)
	n.lock.Unlock()

	// Execute any committed-but-not-executed entries learned during catchup.
	n.replayCommitted()
	n.replayCommittedFromMap()
}

type queuedTxn struct {
	msg *node.TxnMessage
	ch  chan *node.Reply
	// attempts tracks how many times we've retried this queued request.
	attempts int
}

type lockInfo struct {
	keys  []string
	txnID string
}

func NewNode(id string, peers []string) *Node {
	flag := false
	newNode := &Node{
		addr:               id,
		ports:              peers,
		logTable:           make([]*LogEntry, 0),
		grpcConns:          make(map[string]node.NodeClient),
		db:                 make(map[string]float64),
		acceptedBuf:        make(chan *node.AcceptedMsg, len(peers)*8),
		pendingAccepted:    make(map[int64]chan *node.AcceptedMsg),
		promiseLogs:        make(map[string][]*node.AcceptLog),
		ballot:             1,
		seq:                0,
		disabled:           flag,
		lastReply:          make(map[string]*node.Reply),
		leader:             "",
		electionInProgress: false,
		pendingQueue:       make([]*queuedTxn, 0, 128),
		logMap:             make(map[int64]*LogEntry),
		awaiting:           make(map[string]chan *node.Reply),
		viewTable:          make([]*ViewEntry, 0, 32),
		lastViewWritten:    0,
		viewHistory:        make([]newViewRecord, 0, 16),
		executeIdx:         0,
		twoPCLog:           make(map[int64]*TwoPCLogEntry),
		twoPCSeq:           0,
		pendingLocks:       make(map[int64]lockInfo),
		pending2PC:         make([]*node.Prepare2PCReq, 0, 16),

		hbTimeout:       time.Duration(6000+rand.Intn(2000)) * time.Millisecond,
		hbInterval:      1 * time.Second,
		prepareCooldown: 1500 * time.Millisecond,

		checkpointInterval:   20,
		lastCheckpointSeq:    0,
		lastCheckpointBallot: 0,
		lastCheckpointDigest: "",
		pendingCpSeq:         0,
		pendingCpDigest:      "",
	}
	// newNode.disabledFlag = 0
	// newNode.recoveringFlag = 0

	newNode.locks = db.NewLockTable()
	path := fmt.Sprintf("db/node%s.db", strings.TrimPrefix(id, ":"))
	// _ = newNode.loadDBFromFile()
	shardStart, shardEnd := shardRangeForNode(newNode.addr) // e.g., n1→1..3000
	st, err := newNode.store.NewStore(path, false)          // don’t prepopulate all
	if err != nil {
		log.Fatal(err)
	}
	if err := st.ResetBalances(rangeIDs(shardStart, shardEnd), clusterCfg.DefaultBalance); err != nil {
		log.Fatal(err)
	}
	newNode.store = st
	newNode.constructDefaultTxnTable()
	// newNode.saveDBToFile()
	return newNode
}

// defaultShardLeader returns the canonical leader address for this shard (first peer).
func (n *Node) defaultShardLeader() string {
	peers := n.clusterPeers()
	if len(peers) == 0 {
		return ""
	}
	return peers[0]
}

func (n *Node) resetHeartbeatTimer() {
	if n.isDisabled() {
		n.lock.Lock()
		if n.hbTimer != nil {
			if !n.hbTimer.Stop() {
				select {
				case <-n.hbTimer.C:
				default:
				}
			}
			n.hbTimer = nil
		}
		n.lock.Unlock()
		return
	}

	n.lock.Lock()
	if n.hbTimer != nil {
		if !n.hbTimer.Stop() {
			select {
			case <-n.hbTimer.C:
			default:
			}
		}
	}
	n.hbTimer = time.AfterFunc(n.hbTimeout, func() {
		if n.isDisabled() {
			return
		}
		n.lock.Lock()
		isLeader := (n.leader == n.addr)
		n.lock.Unlock()
		if isLeader {
			return
		}
		log.Printf("[%s] heartbeat timeout, starting election", n.addr)
		n.startElection()
	})
	n.lock.Unlock()
}

func (n *Node) startElection() {
	if n.isDisabled() {
		return
	}
	n.lock.Lock()

	if n.electionInProgress {
		log.Printf("[%s] skipping election: already in progress", n.addr)
		n.lock.Unlock()
		return
	}

	if time.Since(n.lastPrepareTime) < n.prepareCooldown {
		n.lock.Unlock()
		log.Printf("[%s] skipping election: in cooldown", n.addr)
		return
	}

	n.electionInProgress = true
	n.ballot++
	n.lastPrepareTime = time.Now()
	curBallot := n.ballot
	n.lock.Unlock()

	defer func() {
		n.lock.Lock()
		n.electionInProgress = false
		n.lock.Unlock()
	}()

	log.Printf("[%s] launching election, ballot %d", n.addr, n.ballot)

	type promiseResult struct {
		node string
		logs []*node.AcceptLog
		ok   bool
	}

	peers := n.clusterPeers()
	resCh := make(chan promiseResult, len(peers)-1)

	var wg sync.WaitGroup
	n.promiseLogs = make(map[string][]*node.AcceptLog)
	for _, port := range peers {
		if port == n.addr || n.isDisabled() {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.lock.Lock()
			client := n.grpcConns[peer]
			n.lock.Unlock()
			if client == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			promise, err := client.Prepare(ctx, &node.PrepareReq{Ballot: curBallot})
			if err != nil {
				log.Printf("[%s] Prepare to %s failed: %v", n.addr, peer, err)
				return
			}
			if promise != nil && promise.Ok {
				resCh <- promiseResult{node: promise.Node, logs: promise.Log, ok: true}
			}
		}(port)
	}

	selfLogs := make([]*node.AcceptLog, 0, len(n.logTable))
	n.lock.Lock()
	for _, le := range n.logTable {
		selfLogs = append(selfLogs, &node.AcceptLog{
			Ballot:    le.ballot,
			Seq:       le.seq,
			Txn:       le.txn,
			Status:    le.status,
			Timestamp: le.timestamp,
			Node:      le.node,
		})
	}
	n.lock.Unlock()

	vote := 1
	promises := map[string][]*node.AcceptLog{}
	promises[n.addr] = selfLogs
	go func() {
		wg.Wait()
		close(resCh)
	}()

	for pr := range resCh {
		if pr.ok {
			promises[pr.node] = pr.logs
			vote++
			log.Printf("%s voting for node %s . Number of votes received %d", pr.node, n.addr, vote)
		}
	}

	if vote < (len(peers)/2)+1 {
		log.Printf("[%s] election failed: got %d promises", n.addr, vote)
		return
	}

	n.lock.Lock()
	n.leader = n.addr
	n.ballot = curBallot
	mergedLog := n.buildAcceptLogFromPromises(promises)
	n.logTable = mergeLogs(n.logTable, mergedLog)
	n.logMap = rebuildLogMap(n.logTable)
	// n.disabledFlag = 0
	// n.recoveringFlag = 0
	log.Println("Election post merge logs :: Logs are")
	n.printLog()

	maxSeq := int64(0)
	for _, entry := range n.logTable {
		if entry.seq > maxSeq {
			maxSeq = entry.seq
		}
	}
	n.seq = maxSeq
	n.lock.Unlock()
	log.Printf("[%s] became leader with ballot %d", n.addr, n.ballot)

	go n.reconcileCluster(int64(curBallot))

	for _, peer := range n.clusterPeers() {
		if peer == n.addr {
			continue
		}
		go n.sendNewView(peer, n.logTable, n.ballot)
	}

	go n.sendHeartbeats()

	n.lock.Lock()
	n.electionInProgress = false
	n.lock.Unlock()

}

func (n *Node) reconcileCluster(ballot int64) {
	n.lock.Lock()
	maxSeq := int64(0)
	for _, e := range n.logTable {
		if e.seq > maxSeq {
			maxSeq = e.seq
		}
	}
	type planEntry struct {
		seq int64
		msg *node.TxnMessage
	}
	plan := make([]planEntry, 0, maxSeq)
	best := make(map[int64]*node.TxnMessage)
	for _, e := range n.logTable {
		if e.seq <= 0 {
			continue
		}
		if e.status == "Accepted" || e.status == "Commit" || e.status == "Execute" {
			best[e.seq] = e.txn
		}
	}
	for s := int64(1); s <= maxSeq; s++ {
		if v, ok := best[s]; ok {
			plan = append(plan, planEntry{seq: s, msg: v})
		}
	}
	peers := n.clusterPeers()
	self := n.addr
	n.lock.Unlock()

	for _, e := range plan {
		chosen := n.acceptRoundUntilQuorum(ballot, e.seq, e.msg, peers, self)
		if chosen != nil {
			n.broadcastCommit(e.seq, chosen)
		}
	}
}

func (n *Node) acceptRoundUntilQuorum(ballot int64, seq int64, proposal *node.TxnMessage, peers []string, self string) *node.TxnMessage {
	sameTxn := func(a, b *node.TxnMessage) bool {
		if a == nil || b == nil || a.Txn == nil || b.Txn == nil {
			return false
		}
		return a.Txn.From == b.Txn.From && a.Txn.To == b.Txn.To && a.Txn.Amount == b.Txn.Amount
	}

	if !n.isDisabled() {
		if _, err := n.Accept(context.Background(), &node.AcceptMsg{Ballot: int32(ballot), Seq: seq, Msg: proposal}); err == nil {
			n.acceptedBuf <- &node.AcceptedMsg{Node: self, Ballot: int32(ballot), Seq: seq, Msg: proposal}
		}
	}

retry:
	results := make(chan *node.AcceptedMsg, len(peers))
	var wg sync.WaitGroup

	for _, p := range peers {
		if p == self {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			client := n.peerClient(peer)
			if client == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := client.Accept(ctx, &node.AcceptMsg{Ballot: int32(ballot), Seq: seq, Msg: proposal})
			if err != nil || resp == nil {
				return
			}
			results <- resp
		}(p)
	}
	wg.Wait()
	close(results)

	var chosen *node.TxnMessage
	seen := map[string]bool{}
	for r := range results {
		if r == nil || r.Msg == nil {
			continue
		}
		if chosen == nil {
			chosen = r.Msg
		}
		if !sameTxn(r.Msg, chosen) {
			proposal = r.Msg
			if _, err := n.Accept(context.Background(), &node.AcceptMsg{Ballot: int32(ballot), Seq: seq, Msg: proposal}); err == nil {
				n.acceptedBuf <- &node.AcceptedMsg{Node: self, Ballot: int32(ballot), Seq: seq, Msg: proposal}
			}
			seen = map[string]bool{}
			chosen = nil
			goto retry
		}
		if !seen[r.Node] {
			seen[r.Node] = true
		}
	}

	n.lock.Lock()
	quorum := (len(peers) / 2) + 1
	n.lock.Unlock()

	if chosen == nil {
		chosen = proposal
	}
	agree := len(seen)
	if agree >= quorum {
		n.lock.Lock()
		if !n.isStatusAchieved(seq, "Accepted") {
			idx := 0
			for idx < len(n.logTable) && n.logTable[idx].seq < seq {
				idx++
			}
			idx = n.findInsertIndex(seq, "Accepted")
			n.appendLogEntry(idx, "Accepted", seq, chosen, n.addr, int32(ballot))
		}
		n.lock.Unlock()
		return chosen
	}
	return nil
}

func (n *Node) buildAcceptLogFromPromises(promises map[string][]*node.AcceptLog) []*node.AcceptLog {
	maxSeq := int64(0)
	best := make(map[int64]*node.AcceptLog)

	for _, logs := range promises {
		for _, al := range logs {
			if al == nil || al.Seq == 0 {
				continue
			}
			if al.Seq > maxSeq {
				maxSeq = al.Seq
			}
			existing, ok := best[al.Seq]
			if al.Status == "Noop" || al.Txn.Txn.From == "NOOP" {
				if _, ok := best[al.Seq]; !ok {
					best[al.Seq] = al
				}
				continue
			}
			existing, ok = best[al.Seq]
			if !ok || al.Ballot > existing.Ballot {
				best[al.Seq] = al
			}
		}
	}

	result := make([]*node.AcceptLog, 0, maxSeq)
	for s := int64(1); s <= maxSeq; s++ {
		if e, ok := best[s]; ok {
			result = append(result, e)
		} else {
			nowTs := strconv.FormatInt(time.Now().UnixNano(), 64)
			noOp := &node.TxnMessage{
				Txn: &node.Transaction{
					From:   "NOOP",
					To:     "NOOP",
					Amount: 0,
				},
				Timestamp: nowTs,
				Client:    "NOOP",
			}
			result = append(result, &node.AcceptLog{
				Ballot:    0,
				Seq:       s,
				Txn:       noOp,
				Status:    "Noop",
				Timestamp: nowTs,
				Node:      n.addr,
			})
		}
	}
	return result
}

func (n *Node) sendHeartbeats() {
	log.Println("Send Heartbeat")
	ticker := time.NewTicker(n.hbInterval)
	defer ticker.Stop()
	for {
		<-ticker.C
		if n.isDisabled() {
			return
		}
		n.lock.Lock()
		if n.leader != n.addr {
			n.lock.Unlock()
			return
		}
		ballot := n.ballot
		n.lock.Unlock()
		for _, port := range n.clusterPeers() {
			if port == n.addr {
				continue
			}
			go func(peer string) {
				if n.isDisabled() {
					return
				}
				server := n.peerClient(peer)
				if server == nil {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				_, _ = server.Heartbeat(ctx, &node.HeartbeatMsg{Ballot: ballot, Leader: n.addr})
			}(port)
		}
	}
}

func (n *Node) transaction(from string, to string, amt float64, ts string) bool {
	if n.store == nil {
		n.lastReply[from] = &node.Reply{Ballot: n.ballot, Client: from, Reply: false, Timestamp: ts, Node: n.addr}
		log.Println("Node", n.addr, "Store is not initialized")
		return false
	}

	if from == "NOOP" || to == "NOOP" {
		n.lastReply[from] = &node.Reply{Ballot: n.ballot, Client: from, Reply: true, Timestamp: ts, Node: n.addr}
		return true
	}

	fromInt, _ := strconv.Atoi(from)
	toInt, _ := strconv.Atoi(to)
	bal, err := n.store.GetBalance(fromInt)
	if err != nil || bal < int64(amt) {
		n.lastReply[from] = &node.Reply{Ballot: n.ballot, Client: from, Reply: false, Timestamp: ts, Node: n.addr}
		return false
	}
	if err := n.store.ApplyTransfer(fromInt, toInt, int64(amt)); err != nil {
		n.lastReply[from] = &node.Reply{Ballot: n.ballot, Client: from, Reply: false, Timestamp: ts, Node: n.addr}
		return false
	}

	log.Println("Node", n.addr, "Successfully executed transaction - ", from, to, amt)
	n.lastReply[from] = &node.Reply{Ballot: n.ballot, Client: from, Reply: true, Timestamp: ts, Node: n.addr}
	return true
}

func (n *Node) constructDefaultTxnTable() {
	for i := 'A'; i <= 'J'; i++ {
		n.db[string(i)] = float64(10)
	}
}

func (n *Node) printLog() {
	log.Printf("For node:%s\n\n", n.addr)
	seqs := make([]int64, 0, len(n.logMap))
	for s := range n.logMap {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, s := range seqs {
		entry := n.logMap[s]
		if entry == nil {
			continue
		}
		log.Printf("(%s %d %d ", entry.status, entry.ballot, entry.seq)
		if entry.txn != nil && entry.txn.Txn != nil {
			log.Printf("(%s %s %f)", entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount)
		}
		log.Printf(")")
	}
	log.Printf("\n\n")
}

func (n *Node) appendLogEntry(idx int, status string, seq int64, msg *node.TxnMessage, server string, ballot int32) {
	logEntry := &LogEntry{
		status:    status,
		timestamp: msg.Timestamp,
		seq:       seq,
		ballot:    ballot,
		txn:       msg,
	}
	if server != "" {
		logEntry.node = server
	}
	n.logTable = append(n.logTable[:idx], append([]*LogEntry{logEntry}, n.logTable[idx:]...)...)
	// update log map with latest phase/ballot
	if cur, ok := n.logMap[seq]; !ok || phaseRank(status) > phaseRank(cur.status) || (phaseRank(status) == phaseRank(cur.status) && ballot > cur.ballot) {
		n.logMap[seq] = logEntry
	}
}

func (n *Node) appendTwoPCLog(status string, paxosStatus string, seq int64, msg *node.TxnMessage, phase node.TwoPCPhase, server string, ballot int32) {
	entry := &TwoPCLogEntry{
		status:      status,
		paxosStatus: paxosStatus,
		timestamp:   msg.Timestamp,
		seq:         seq,
		ballot:      ballot,
		txn:         msg,
		node:        server,
		phase:       phase,
	}
	cur, ok := n.twoPCLog[seq]
	if !ok || phaseRank(paxosStatus) >= phaseRank(cur.paxosStatus) {
		n.twoPCLog[seq] = entry
	}
	n.printTwoPCLogEntry(entry)
}

func (n *Node) isRequestExecuted(req *node.TxnMessage) bool {
	for i := len(n.logTable) - 1; i >= 0; i-- {
		entry := n.logTable[i]
		if entry.status != "Execute" || entry.txn == nil {
			continue
		}
		if entry.txn.Timestamp == req.Timestamp && sameTxn(entry.txn, req) {
			return true
		}
	}
	return false
}

// findExistingTxnReply checks logMap for a txn with same timestamp/payload and returns a success reply if found.
func (n *Node) findExistingTxnReply(req *node.TxnMessage) *node.Reply {
	for _, e := range n.logMap {
		if e == nil || e.txn == nil || e.txn.Txn == nil {
			continue
		}
		if e.txn.Timestamp != req.Timestamp || !sameTxn(e.txn, req) {
			continue
		}
		// found a record; treat as success if already in log at Commit or higher
		if phaseRank(e.status) >= phaseRank("Commit") {
			return &node.Reply{Ballot: n.ballot, Timestamp: req.Timestamp, Client: req.Client, Reply: true, Node: n.addr}
		}
	}
	return nil
}

func (n *Node) isStatusAchieved(seqNo int64, status string) bool {
	for i := len(n.logTable) - 1; i >= 0; i-- {
		if n.logTable[i].status == status && seqNo == n.logTable[i].seq {
			return true
		}
	}
	return false
}

func (n *Node) ResetDB(ctx context.Context, req *node.ResetDBReq) (*node.ResetDBResp, error) {
	n.lock.Lock()
	// lazily open/create the store if missing
	shardStart, shardEnd := shardRangeForNode(n.addr)
	if n.store == nil {
		path := fmt.Sprintf("db/node%s.db", strings.TrimPrefix(n.addr, ":"))
		st, err := n.store.NewStore(path, true) // prepopulate 1..9000 with 10 if empty
		if err != nil {
			n.lock.Unlock()
			return &node.ResetDBResp{Ok: false, Msg: err.Error()}, nil
		}
		n.store = st
	}
	st := n.store

	// reset in-memory protocol state
	n.disabled = false
	// atomic.StoreInt32(&n.disabledFlag, 0)
	n.electionInProgress = false
	if n.hbTimer != nil {
		if !n.hbTimer.Stop() {
			select {
			case <-n.hbTimer.C:
			default:
			}
		}
		n.hbTimer = nil
	}
	n.seq = 0
	n.ballot = 0
	n.leader = ""
	n.logTable = n.logTable[:0]
	n.logMap = make(map[int64]*LogEntry)
	n.twoPCLog = make(map[int64]*TwoPCLogEntry)
	n.twoPCSeq = 0
	n.pendingAccepted = make(map[int64]chan *node.AcceptedMsg)
	n.pendingQueue = n.pendingQueue[:0]
	n.awaiting = make(map[string]chan *node.Reply)
	n.viewTable = n.viewTable[:0]
	n.viewHistory = n.viewHistory[:0]
	n.lastViewWritten = 0
	n.executeIdx = 0
	n.lastReply = make(map[string]*node.Reply)
	n.lastCheckpointSeq = 0
	n.lastCheckpointBallot = 0
	n.lastCheckpointDigest = ""
	n.pendingCpSeq = 0
	n.pendingCpDigest = ""
	n.pendingLocks = make(map[int64]lockInfo)
	n.promiseLogs = make(map[string][]*node.AcceptLog)
	n.locks = db.NewLockTable()
	n.recovering = false
	n.pending2PC = n.pending2PC[:0]
	// drop existing grpc connections and reconnect to flush state
	n.grpcConns = make(map[string]node.NodeClient)
	go n.connectPeers()
	// assign default leader within this shard (self or first peer)
	n.leader = n.defaultShardLeader()
	log.Printf("[%s] ResetDB: cleared state seq=%d ballot=%d logs=%d twoPC=%d leader=%s", n.addr, n.seq, n.ballot, len(n.logTable), len(n.twoPCLog), n.leader)
	n.lock.Unlock()

	// if this node is leader after reset, restart heartbeats
	if n.leader == n.addr {
		go n.sendHeartbeats()
	}
	n.resetHeartbeatTimer()

	// reset all balances to requested value (or default 10)
	val := req.GetValue()
	if val == 0 {
		val = clusterCfg.DefaultBalance
	}
	if err := st.ResetBalances(rangeIDs(shardStart, shardEnd), val); err != nil {
		return &node.ResetDBResp{Ok: false, Msg: err.Error()}, nil
	}
	log.Printf("[%s] ResetDB: balances reset to %d for shard %d-%d", n.addr, val, shardStart, shardEnd)
	return &node.ResetDBResp{Ok: true, Msg: "reset"}, nil
}

// clusterPeers returns only the peers that are in the same shard as this node.
// It includes this node's own addr in the list.
func (n *Node) clusterPeers() []string {
	c := clusterForAddr(n.addr)
	if c == nil {
		return []string{}
	}
	return append([]string(nil), c.Nodes...)
}

func shardPeersForAccount(id int) []string {
	if c := shardForAccount(id); c != nil {
		return append([]string(nil), c.Nodes...)
	}
	return []string{}
}

func shardRangeForNode(addr string) (int, int) {
	c := clusterForAddr(addr)
	if c == nil {
		return 1, 0 // empty
	}
	return c.StartID, c.EndID
}

// ownsAccount reports whether this node should mutate the given account id.
// A node owns an id if it falls in its configured range OR it has a modified
// marker for that id (used after redistribution/migration).
//
//	func (n *Node) ownsAccount(id int) bool {
//		if id <= 0 {
//			return false
//		}
//		start, end := shardRangeForNode(n.addr)
//		if id >= start && id <= end {
//			return true
//		}
//		n.lock.Lock()
//		st := n.store
//		n.lock.Unlock()
//		if st == nil {
//			return false
//		}
//		if ok, err := st.WasModified(id); err == nil && ok {
//			return true
//		}
//		return false
//	}
func rangeIDs(start, end int) []int {
	ids := make([]int, 0, end-start+1)
	for i := start; i <= end; i++ {
		ids = append(ids, i)
	}
	return ids
}

func (n *Node) Request(ctx context.Context, in *node.TxnMessage) (*node.Reply, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.lock.Lock()
	if rep := n.findExistingTxnReply(in); rep != nil {
		n.lock.Unlock()
		return rep, nil
	}
	n.lock.Unlock()
	n.lock.Lock()
	leader := n.leader
	n.lock.Unlock()

	if leader == "" {
		log.Printf("[%s] no leader, starting election on request", n.addr)
		n.startElection()
		time.Sleep(500 * time.Millisecond)
		n.lock.Lock()
		leader = n.leader
		n.lock.Unlock()
	}

	if leader == "" {
		return nil, fmt.Errorf("[%s] leader not elected yet", n.addr)
	}

	if leader != n.addr {
		n.lock.Lock()
		if n.isRequestExecuted(in) {
			return &node.Reply{Ballot: n.ballot, Timestamp: in.Timestamp, Client: in.Client, Reply: true, Node: n.addr}, nil
		}
		client, ok := n.grpcConns[leader]
		n.lock.Unlock()
		if !ok || client == nil {
			n.lock.Lock()
			if n.leader == leader {
				n.leader = ""
			}
			n.lock.Unlock()
			go n.startElection()
			return nil, fmt.Errorf("[%s] cannot reach leader %s; starting election", n.addr, leader)
		}

		ctxForward, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		reply, err := client.Request(ctxForward, in)
		if err != nil {
			log.Printf("[%s] forwarding to leader %s failed: %v", n.addr, leader, err)
			n.lock.Lock()
			if n.leader == leader {
				n.leader = ""
			}
			n.lock.Unlock()
			go n.startElection()
			return nil, fmt.Errorf("leader unreachable; election triggered")
		}

		return reply, nil
	}

	live := n.countLive()
	if live < (len(n.clusterPeers())/2)+1 {
		ch := make(chan *node.Reply, 1)
		n.lock.Lock()
		if _, exists := n.awaiting[in.Timestamp]; !exists {
			n.awaiting[in.Timestamp] = ch
			n.pendingQueue = append(n.pendingQueue, &queuedTxn{msg: in, ch: ch, attempts: 0})
		} else {
			ch = n.awaiting[in.Timestamp]
		}
		n.lock.Unlock()

		go n.tryDrainQueued()

		select {
		case r := <-ch:
			return r, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("queued; client canceled")
		}
	}

	n.tryDrainQueued()

	if n.isDuplicateTxn(in) {
		log.Println("Request RPC :: Duplicate transaction detected", in.Txn.From, in.Txn.To, in.Txn.Amount, in.Timestamp)
		return &node.Reply{
			Ballot:    n.ballot,
			Timestamp: in.Timestamp,
			Client:    in.Client,
			Reply:     true,
			Node:      n.addr,
		}, nil
	}

	n.lock.Lock()
	log.Println("Request RPC :: start", n.addr)

	if n.isRequestExecuted(in) {
		log.Println("Request RPC :: Request already executed", in.Txn.From, in.Txn.To, in.Txn.Amount, in.Timestamp)
		reply := &node.Reply{
			Ballot:    n.ballot,
			Timestamp: in.Timestamp,
			Client:    in.Client,
			Reply:     true,
			Node:      n.addr,
		}
		n.lock.Unlock()
		return reply, nil
	}

	if !n.disabled && n.addr != n.leader {
		log.Println("Request RPC :: node", n.addr, "is not the leader. Calling leader", n.leader)
		n.lock.Unlock()
		return nil, fmt.Errorf("Not the leader")
	}
	n.lock.Unlock()

	// fromInt, _ := strconv.Atoi(in.Txn.From)
	// toInt, _ := strconv.Atoi(in.Txn.To)
	// coordCluster := shardPeersForAccount(fromInt)
	// partCluster := shardPeersForAccount(toInt)
	// crossShard := !sameClusterPeers(coordCluster, partCluster)

	// if crossShard {
	// 	if err := n.run2PCPrepareAsCoordinator(in, partCluster); err != nil {
	// 		return nil, err
	// 	}
	// 	return &node.Reply{Ballot: n.ballot, Timestamp: in.Timestamp, Client: in.Client, Reply: true, Node: n.addr}, nil
	// }

	reply, err := n.proposeAndCommit(in)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// Request2PC explicitly handles cross-shard transactions via 2PC.
func (n *Node) Request2PC(ctx context.Context, in *node.TxnMessage) (*node.Reply, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.lock.Lock()
	if rep := n.findExistingTxnReply(in); rep != nil {
		n.lock.Unlock()
		return rep, nil
	}
	n.lock.Unlock()
	n.lock.Lock()
	leader := n.leader
	n.lock.Unlock()

	if leader == "" {
		log.Printf("[%s] Request2PC: no leader, starting election", n.addr)
		n.startElection()
		time.Sleep(500 * time.Millisecond)
		n.lock.Lock()
		leader = n.leader
		n.lock.Unlock()
	}
	if leader == "" {
		log.Printf("[%s] Request2PC: leader still empty after election", n.addr)
		return nil, fmt.Errorf("[%s] leader not elected yet", n.addr)
	}

	if leader != n.addr {
		n.lock.Lock()
		client := n.grpcConns[leader]
		n.lock.Unlock()
		if client == nil {
			log.Printf("[%s] Request2PC: missing client for leader %s", n.addr, leader)
			return nil, fmt.Errorf("leader client missing")
		}
		ctxForward, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Printf("[%s] Request2PC: forwarding to leader %s", n.addr, leader)
		return client.Request2PC(ctxForward, in)
	}

	fromInt, _ := strconv.Atoi(in.Txn.From)
	toInt, _ := strconv.Atoi(in.Txn.To)
	coordCluster := shardPeersForAccount(fromInt)
	partCluster := shardPeersForAccount(toInt)
	crossShard := !sameClusterPeers(coordCluster, partCluster)
	if !crossShard {
		log.Printf("[%s] Request2PC: same-shard txn %s->%s; delegating to Request", n.addr, in.Txn.From, in.Txn.To)
		return n.Request(ctx, in)
	}

	log.Printf("[%s] Request2PC: cross-shard txn %s->%s coordPeers=%v partPeers=%v", n.addr, in.Txn.From, in.Txn.To, coordCluster, partCluster)
	if err := n.run2PCPrepareAsCoordinator(in, partCluster); err != nil {
		log.Printf("[%s] Request2PC: prepare failed: %v", n.addr, err)
		return nil, err
	}
	log.Printf("[%s] Request2PC: prepare succeeded", n.addr)
	return &node.Reply{Ballot: n.ballot, Timestamp: in.Timestamp, Client: in.Client, Reply: true, Node: n.addr}, nil
}

func (n *Node) SetNodeState(ctx context.Context, req *node.NodeStateRequest) (*node.NodeStateReply, error) {
	n.lock.Lock()
	log.Println("For node", n.addr, "disabled?", !req.Enabled)
	// wasDisabled := n.disabled
	if req.Enabled {
		// stay disabled until recovery finishes
		n.recovering = true
		// atomic.StoreInt32(&n.recoveringFlag, 1)
		n.disabled = true
		// atomic.StoreInt32(&n.disabledFlag, 1)
	} else {
		n.disabled = true
		// atomic.StoreInt32(&n.disabledFlag, 1)
		n.recovering = false
		// atomic.StoreInt32(&n.recoveringFlag, 0)
	}

	if n.disabled {
		n.electionInProgress = false
		if n.hbTimer != nil {
			if !n.hbTimer.Stop() {
				select {
				case <-n.hbTimer.C:
				default:
				}
			}
			n.hbTimer = nil
		}
		n.pendingAccepted = make(map[int64]chan *node.AcceptedMsg)
		if n.leader == n.addr {
			n.leader = ""
		}
		n.hbTimer = nil
	}

	addr := n.addr
	nowDisabled := n.disabled
	n.lock.Unlock()

	if req.Enabled {
		// reconnect peers fresh and only then mark enabled
		go func() {
			n.lock.Lock()
			// drop old conns and reconnect
			n.grpcConns = make(map[string]node.NodeClient)
			n.lock.Unlock()
			n.connectPeers()
			time.Sleep(400 * time.Millisecond)

			// pull logs from leader and replay before enabling
			n.catchupFromLeader()

			n.lock.Lock()
			n.leader = n.defaultShardLeader()
			n.electionInProgress = false
			n.lastPrepareTime = time.Now().Add(-n.prepareCooldown)
			n.disabled = false
			// atomic.StoreInt32(&n.disabledFlag, 0)
			n.recovering = false
			// atomic.StoreInt32(&n.recoveringFlag, 0)
			n.lock.Unlock()

			n.resetHeartbeatTimer()
			n.lock.Lock()
			isLeader := n.leader == n.addr
			n.lock.Unlock()
			if isLeader {
				go n.sendHeartbeats()
			}
		}()
	}

	state := "enabled"
	if nowDisabled || n.recovering {
		state = "disabled"
	}
	return &node.NodeStateReply{Success: true, Msg: fmt.Sprintf("%s %s", addr, state)}, nil
}

func (n *Node) PrintStatus(ctx context.Context, req *node.PrintStatusRequest) (*node.PrintStatusResponse, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	role := "FOLLOWER"
	if n.disabled {
		role = "DISABLED"
	} else if n.leader == n.addr {
		role = "LEADER"
	}

	ns := &node.PrintStatusResponse_NodeStatus{
		NodeAddr:    n.addr,
		Status:      role,
		Ballot:      n.ballot,
		Transaction: "",
	}
	return &node.PrintStatusResponse{
		Statuses: []*node.PrintStatusResponse_NodeStatus{ns},
		Message:  "ok",
	}, nil
}

func (n *Node) Accept(ctx context.Context, in *node.AcceptMsg) (*node.AcceptedMsg, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.resetHeartbeatTimer()
	n.lock.Lock()
	defer n.lock.Unlock()

	for _, e := range n.logTable {
		if e.seq == in.Seq && e.ballot == in.Ballot && e.status == "Accepted" {
			if !sameTxn(e.txn, in.Msg) {
				return &node.AcceptedMsg{
					Node:   n.addr,
					Ballot: in.Ballot,
					Seq:    in.Seq,
					Msg:    e.txn,
				}, nil
			}
			return &node.AcceptedMsg{
				Node:   n.addr,
				Ballot: in.Ballot,
				Seq:    in.Seq,
				Msg:    in.Msg,
			}, nil
		}
	}

	if in.Ballot >= n.ballot {
		n.ballot = in.Ballot
		i := 0
		for i < len(n.logTable) {
			if in.Seq < n.logTable[i].seq {
				break
			}
			i++
		}

		if n.hasPhaseAtOrAbove(in.Seq, "Commit") {
			return &node.AcceptedMsg{
				Node:   n.addr,
				Ballot: in.Ballot,
				Seq:    in.Seq,
				Msg:    in.Msg,
			}, nil
		}
		idx := n.findInsertIndex(in.Seq, "Accepted")
		n.appendLogEntry(idx, "Accepted", in.Seq, in.Msg, n.addr, in.Ballot)
		return &node.AcceptedMsg{
			Node:   n.addr,
			Ballot: in.Ballot,
			Seq:    in.Seq,
			Msg:    in.Msg,
		}, nil
	}
	return nil, fmt.Errorf("reject: stale ballot")
}

// Prepare2PC handles the participant's prepare RPC for cross-shard transactions.
func (n *Node) Prepare2PC(ctx context.Context, req *node.Prepare2PCReq) (*node.Prepare2PCResp, error) {
	if n.isDisabled() {
		log.Printf("[%s] Prepare2PC: disabled", n.addr)
		return &node.Prepare2PCResp{Ok: false, Reason: "disabled", Node: n.addr, Seq: 0}, nil
	}
	if req == nil || req.Request == nil || req.Request.Txn == nil {
		log.Printf("[%s] Prepare2PC: invalid request %+v", n.addr, req)
		return &node.Prepare2PCResp{Ok: false, Reason: "invalid request", Node: n.addr, Seq: 0}, nil
	}

	// participant locks the receiver record; do not debit/credit yet
	txn := req.Request
	txnID := fmt.Sprintf("%s-%s-%s", txn.Txn.From, txn.Txn.To, txn.Timestamp)
	key := txn.Txn.To
	if !n.locks.TryLock([]string{key}, txnID) {
		log.Printf("[%s] Prepare2PC: lock held on %s for txn %s", n.addr, key, txnID)
		// abort immediately; do not enqueue for retry
		return &node.Prepare2PCResp{Ok: false, Reason: "lock held (abort)", Node: n.addr, Seq: 0}, nil
	}
	log.Printf("[%s] Prepare2PC: locked %s for txn %s", n.addr, key, txnID)

	// replicate prepare via Accept2PC across this shard (Paxos accept+commit semantics)
	n.lock.Lock()
	n.seq++
	n.twoPCSeq = n.seq
	seq := n.twoPCSeq
	ballot := n.ballot
	if n.pendingLocks == nil {
		n.pendingLocks = make(map[int64]lockInfo)
	}
	n.pendingLocks[seq] = lockInfo{keys: []string{key}, txnID: txnID}
	n.lock.Unlock()

	accept := &node.Accept2PCMsg{Ballot: ballot, Seq: seq, Msg: txn, Phase: node.TwoPCPhase_PREPARE_PHASE}
	if err := n.broadcastAccept2PC(accept, true, "P"); err != nil {
		log.Printf("[%s] Prepare2PC: broadcastAccept2PC failed: %v", n.addr, err)
		n.locks.Unlock([]string{key}, txnID)
		n.appendTwoPCLog("A", "Abort", seq, txn, node.TwoPCPhase_PREPARE_PHASE, n.addr, ballot)
		n.broadcastAbort2PC(&node.Abort2PCMsg{Ballot: ballot, Seq: seq, Msg: txn, Phase: node.TwoPCPhase_PREPARE_PHASE, Reason: err.Error()})
		return &node.Prepare2PCResp{Ok: false, Reason: err.Error(), Node: n.addr, Seq: seq}, nil
	}

	// quorum reached; log and execute locally
	n.lock.Lock()
	n.appendTwoPCLog("P", "Commit", seq, txn, node.TwoPCPhase_PREPARE_PHASE, n.addr, ballot)
	n.lock.Unlock()
	log.Printf("[%s] Prepare2PC: quorum accepted seq=%d ballot=%d", n.addr, seq, ballot)

	// execute the receiver-side effect (credit) if applicable; keep lock held until explicit commit/abort logic
	// if err := n.executeCrossShardWithWal(seq, txn); err != nil {
	// 	log.Printf("[%s] Prepare2PC: executeCrossShard failed: %v", n.addr, err)
	// 	n.broadcastAbort2PC(&node.Abort2PCMsg{Ballot: ballot, Seq: seq, Msg: txn, Phase: node.TwoPCPhase_PREPARE_PHASE, Reason: err.Error()})
	// 	return &node.Prepare2PCResp{Ok: false, Reason: err.Error(), Node: n.addr, Seq: seq}, nil
	// }
	log.Printf("[%s] Prepare2PC: executeCrossShard succeeded", n.addr)
	n.logTxnBalances(txn)
	return &node.Prepare2PCResp{Ok: true, Node: n.addr, Seq: seq}, nil
}

// Accept2PC replicates 2PC decisions within a shard without touching the normal Paxos log.
func (n *Node) Accept2PC(ctx context.Context, in *node.Accept2PCMsg) (*node.Accept2PCResp, error) {
	if n.isDisabled() {
		log.Printf("[%s] Accept2PC: disabled", n.addr)
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	log.Printf("[%s] Accept2PC: ballot=%d seq=%d phase=%s txn=%s->%s amt=%.2f", n.addr, in.Ballot, in.Seq, in.Phase.String(), in.Msg.Txn.From, in.Msg.Txn.To, in.Msg.Txn.Amount)
	n.resetHeartbeatTimer()
	n.lock.Lock()
	defer n.lock.Unlock()

	for _, e := range n.twoPCLog {
		if e.seq == in.Seq && e.ballot == in.Ballot && e.phase == in.Phase && e.paxosStatus == "Accepted" {
			if !sameTxn(e.txn, in.Msg) {
				return &node.Accept2PCResp{
					Node:   n.addr,
					Ballot: in.Ballot,
					Seq:    in.Seq,
					Msg:    e.txn,
					Phase:  in.Phase,
				}, nil
			}
			return &node.Accept2PCResp{
				Node:   n.addr,
				Ballot: in.Ballot,
				Seq:    in.Seq,
				Msg:    in.Msg,
				Phase:  in.Phase,
			}, nil
		}
	}

	if in.Ballot >= n.ballot {
		n.ballot = in.Ballot
		statusLabel := "C"
		if in.Phase == node.TwoPCPhase_PREPARE_PHASE {
			statusLabel = "P"
		}
		n.appendTwoPCLog(statusLabel, "Accepted", in.Seq, in.Msg, in.Phase, n.addr, in.Ballot)
		log.Printf("[%s] Accept2PC: accepted seq=%d ballot=%d phase=%s", n.addr, in.Seq, in.Ballot, in.Phase.String())
		return &node.Accept2PCResp{
			Node:   n.addr,
			Ballot: in.Ballot,
			Seq:    in.Seq,
			Msg:    in.Msg,
			Phase:  in.Phase,
		}, nil
	}
	return nil, fmt.Errorf("reject: stale ballot")
}

// CommitPhase2PC runs the commit Paxos phase (no execution) with retry on quorum failure.
func (n *Node) CommitPhase2PC(ctx context.Context, req *node.CommitPhase2PCReq) (*node.CommitPhase2PCResp, error) {
	if n.isDisabled() {
		return &node.CommitPhase2PCResp{Ok: false, Reason: "disabled", Node: n.addr}, nil
	}
	if req == nil || req.Msg == nil || req.Msg.Txn == nil {
		return &node.CommitPhase2PCResp{Ok: false, Reason: "invalid request", Node: n.addr}, nil
	}

	seq := req.Seq
	ballot := req.Ballot
	phase := node.TwoPCPhase_COMMIT_PHASE
	if seq == 0 {
		return &node.CommitPhase2PCResp{Ok: false, Reason: "missing seq for commit phase", Node: n.addr}, nil
	}
	n.lock.Lock()
	if ballot == 0 {
		ballot = n.ballot
	}
	n.lock.Unlock()

	msg := &node.Accept2PCMsg{Ballot: ballot, Seq: seq, Msg: req.Msg, Phase: phase}
	for attempt := 1; ; attempt++ {
		if err := n.broadcastAccept2PC(msg, true, "C"); err == nil {
			log.Printf("[%s] CommitPhase2PC: commit quorum achieved seq=%d", n.addr, seq)
			// n.appendTwoPCLog("C", "CommitPhase", seq, req.Msg, phase, n.addr, ballot)
			return &node.CommitPhase2PCResp{Ok: true, Node: n.addr}, nil
		}
		log.Printf("[%s] CommitPhase2PC: retrying attempt %d for seq=%d", n.addr, attempt, seq)
		select {
		case <-ctx.Done():
			return &node.CommitPhase2PCResp{Ok: false, Reason: "commit retries canceled", Node: n.addr}, nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Commit2PC executes the replicated 2PC record without marking main Paxos log executed.
func (n *Node) Commit2PC(ctx context.Context, in *node.Commit2PCMsg) (*node.Acknowledgement, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	if in == nil || in.Msg == nil || in.Msg.Txn == nil {
		return nil, fmt.Errorf("invalid commit2pc message")
	}
	log.Printf("[%s] Commit2PC: seq=%d ballot=%d phase=%s txn=%s->%s amt=%.2f", n.addr, in.Seq, in.Ballot, in.Phase.String(), in.Msg.Txn.From, in.Msg.Txn.To, in.Msg.Txn.Amount)

	// Idempotence: if we already have a Commit entry for this txn+phase, skip re-execution.
	// if seq, dup := n.findDuplicate2PCTxn(in.Msg); dup {
	// 	n.lock.Lock()
	// 	if e, ok := n.twoPCLog[seq]; ok && e != nil && e.paxosStatus == "Commit" {
	// 		// If we've already committed this txn for this phase, do nothing.
	// 		if e.phase == in.Phase {
	// 			n.lock.Unlock()
	// 			return &node.Acknowledgement{Status: "Committed", Node: n.addr}, nil
	// 		}
	// 		// If commit-phase already recorded, any repeat prepare should also no-op.
	// 		if in.Phase == node.TwoPCPhase_PREPARE_PHASE {
	// 			n.lock.Unlock()
	// 			return &node.Acknowledgement{Status: "Committed", Node: n.addr}, nil
	// 		}
	// 	}
	// 	n.lock.Unlock()
	// }

	statusLabel := "C"
	if in.Phase == node.TwoPCPhase_PREPARE_PHASE {
		statusLabel = "P"
		// prepare phase: one attempt, abort on failure
		if err := n.executeCrossShardWithWal(in.Seq, in.Msg); err != nil {
			log.Printf("[%s] Commit2PC: prepare-phase execute failed: %v", n.addr, err)
			n.broadcastAbort2PC(&node.Abort2PCMsg{Ballot: in.Ballot, Seq: in.Seq, Msg: in.Msg, Phase: in.Phase, Reason: err.Error()})
			return &node.Acknowledgement{Status: "ERR", Node: n.addr}, nil
		}
	}
	log.Printf("[%s] Commit2PC: Phase: %s", n.addr, in.Phase.String())
	n.appendTwoPCLog(statusLabel, "Commit", in.Seq, in.Msg, in.Phase, n.addr, in.Ballot)
	if statusLabel == "C" {
		n.lock.Lock()
		// record Commit and Execute in main Paxos log map for visibility
		cIdx := n.findInsertIndex(in.Seq, "Commit")
		n.appendLogEntry(cIdx, "Commit", in.Seq, in.Msg, n.addr, in.Ballot)
		eIdx := n.findInsertIndex(in.Seq, "Execute")
		n.appendLogEntry(eIdx, "Execute", in.Seq, in.Msg, n.addr, in.Ballot)
		n.printLog()
		n.lock.Unlock()
		// release locks for this 2PC seq if held
		n.lock.Lock()
		if li, ok := n.pendingLocks[in.Seq]; ok {
			n.locks.Unlock(li.keys, li.txnID)
			delete(n.pendingLocks, in.Seq)
		}
		n.lock.Unlock()
		// After cross-shard execution, advance any pending intra-shard commits that can now run.
		n.replayCommitted()
		n.replayCommittedFromMap()
		// No main Paxos append for 2PC commits; avoid double execution.
		go n.tryDrain2PCPending()
	}
	n.logTxnBalances(in.Msg)
	return &node.Acknowledgement{Status: "Committed2PC", Node: n.addr}, nil
}

// Abort2PC undoes any prepare-side effects and logs abort.
func (n *Node) Abort2PC(ctx context.Context, in *node.Abort2PCMsg) (*node.Acknowledgement, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	if in == nil || in.Msg == nil || in.Msg.Txn == nil {
		return nil, fmt.Errorf("invalid abort2pc message")
	}
	log.Printf("[%s] Abort2PC: seq=%d ballot=%d phase=%s reason=%s", n.addr, in.Seq, in.Ballot, in.Phase.String(), in.Reason)
	n.undoWithWal(in.Seq, in.Msg)
	if err := n.undoCrossShard(in.Msg); err != nil {
		log.Printf("[%s] Abort2PC: undoCrossShard failed: %v", n.addr, err)
	}
	n.appendTwoPCLog("A", "Abort", in.Seq, in.Msg, in.Phase, n.addr, in.Ballot)
	n.lock.Lock()
	if li, ok := n.pendingLocks[in.Seq]; ok {
		n.locks.Unlock(li.keys, li.txnID)
		delete(n.pendingLocks, in.Seq)
	}
	n.lock.Unlock()
	go n.tryDrain2PCPending()
	n.logTxnBalances(in.Msg)
	return &node.Acknowledgement{Status: "Aborted2PC", Node: n.addr}, nil
}

// broadcastAccept2PC sends Accept2PC to all peers (including self) and waits for quorum.
func (n *Node) broadcastAccept2PC(msg *node.Accept2PCMsg, doCommit bool, statusLabel string) error {
	peers := n.clusterPeers()
	quorum := (len(peers) / 2) + 1
	log.Printf("[%s] broadcastAccept2PC: seq=%d ballot=%d phase=%s peers=%v quorum=%d", n.addr, msg.Seq, msg.Ballot, msg.Phase.String(), peers, quorum)

	seen := map[string]bool{}
	respCh := make(chan *node.Accept2PCResp, len(peers))

	// local accept
	if resp, err := n.Accept2PC(context.Background(), msg); err == nil {
		respCh <- resp
	}

	for _, peer := range peers {
		if peer == n.addr {
			continue
		}
		go func(p string) {
			for {
				client := n.peerClient(p)
				if client == nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := client.Accept2PC(ctx2, msg)
				cancel()
				if err != nil || resp == nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				respCh <- resp
				return
			}
		}(peer)
	}

	timeout := time.After(10 * time.Second)
	for {
		select {
		case r := <-respCh:
			if r == nil {
				continue
			}
			if !seen[r.Node] {
				seen[r.Node] = true
			}
			if len(seen) >= quorum {
				log.Printf("[%s] broadcastAccept2PC: quorum reached (%d/%d) for phase %s", n.addr, len(seen), quorum, msg.Phase.String())
				// mark commit stage in 2PC log for bookkeeping and broadcast commit
				n.lock.Lock()
				n.appendTwoPCLog(statusLabel, "Commit", msg.Seq, msg.Msg, msg.Phase, n.addr, msg.Ballot)
				n.lock.Unlock()
				if doCommit {
					n.broadcastCommit2PC(msg, statusLabel)
				}
				return nil
			}
		case <-timeout:
			log.Printf("[%s] broadcastAccept2PC: timeout waiting for quorum (%d/%d)", n.addr, len(seen), quorum)
			if statusLabel != "C" { // commit phase should retry without aborting
				n.appendTwoPCLog("A", "Abort", msg.Seq, msg.Msg, msg.Phase, n.addr, msg.Ballot)
				n.broadcastAbort2PC(&node.Abort2PCMsg{Ballot: msg.Ballot, Seq: msg.Seq, Msg: msg.Msg, Phase: msg.Phase, Reason: "timeout"})
			}
			return fmt.Errorf("timeout waiting for 2PC accepts")
		}
	}
}

// broadcastCommit2PC sends Commit2PC to all shard peers to apply the prepared change.
func (n *Node) broadcastCommit2PC(msg *node.Accept2PCMsg, statusLabel string) {
	phase := msg.Phase
	if statusLabel == "C" {
		phase = node.TwoPCPhase_COMMIT_PHASE
	}
	for _, p := range n.clusterPeers() {
		peer := p
		go func() {
			if peer == n.addr {
				_, _ = n.Commit2PC(context.Background(), &node.Commit2PCMsg{
					Ballot: msg.Ballot, Seq: msg.Seq, Msg: msg.Msg, Phase: phase,
				})
				return
			}
			client := n.peerClient(peer)
			if client == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = client.Commit2PC(ctx, &node.Commit2PCMsg{
				Ballot: msg.Ballot, Seq: msg.Seq, Msg: msg.Msg, Phase: phase,
			})
		}()
	}
}

// broadcastAbort2PC sends Abort2PC to all shard peers to undo and log abort.
func (n *Node) broadcastAbort2PC(msg *node.Abort2PCMsg) {
	for _, p := range n.clusterPeers() {
		peer := p
		go func() {
			if peer == n.addr {
				_, _ = n.Abort2PC(context.Background(), msg)
				return
			}
			n.lock.Lock()
			client := n.grpcConns[peer]
			n.lock.Unlock()
			if client == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = client.Abort2PC(ctx, msg)
		}()
	}
}

// executeCrossShard applies the receiver-side effect for a cross-shard transfer when possible.
func (n *Node) executeCrossShard(txn *node.TxnMessage) error {
	if n.store == nil {
		return fmt.Errorf("store not initialized")
	}
	from, _ := strconv.Atoi(txn.Txn.From)
	to, _ := strconv.Atoi(txn.Txn.To)
	start, end := shardRangeForNode(n.addr)
	fromInShard := from >= start && from <= end
	toInShard := to >= start && to <= end

	switch {
	case fromInShard && !toInShard:
		// coordinator shard: only debit sender
		return n.store.SubtractBalance(from, int64(txn.Txn.Amount))
	case !fromInShard && toInShard:
		// participant shard: only credit receiver
		return n.store.AddBalance(to, int64(txn.Txn.Amount))
	case fromInShard && toInShard:
		// both local
		return n.store.ApplyTransfer(from, to, int64(txn.Txn.Amount))
	default:
		// neither local; nothing to do
		return nil
	}
}

// undoCrossShard reverses executeCrossShard effects for an abort.
func (n *Node) undoCrossShard(txn *node.TxnMessage) error {
	if n.store == nil {
		return fmt.Errorf("store not initialized")
	}
	from, _ := strconv.Atoi(txn.Txn.From)
	to, _ := strconv.Atoi(txn.Txn.To)
	start, end := shardRangeForNode(n.addr)
	fromInShard := from >= start && from <= end
	toInShard := to >= start && to <= end

	switch {
	case fromInShard && !toInShard:
		// undo coordinator debit
		return n.store.AddBalance(from, int64(txn.Txn.Amount))
	case !fromInShard && toInShard:
		// undo participant credit
		return n.store.SubtractBalance(to, int64(txn.Txn.Amount))
	case fromInShard && toInShard:
		// undo local transfer
		return n.store.RevertTransfer(from, to, int64(txn.Txn.Amount))
	default:
		return nil
	}
}

// executeCrossShardWithWal records before-state and then applies changes; wal stored in twoPCWal keyed by seq.
func (n *Node) executeCrossShardWithWal(seq int64, txn *node.TxnMessage) error {
	n.lock.Lock()
	before := walSnapshot{}
	fromID, _ := strconv.Atoi(txn.Txn.From)
	toID, _ := strconv.Atoi(txn.Txn.To)
	start, end := shardRangeForNode(n.addr)
	fromInShard := fromID >= start && fromID <= end
	toInShard := toID >= start && toID <= end
	if fromInShard {
		if bal, err := n.store.GetBalance(fromID); err == nil {
			before.beforeFrom = bal
			before.trackFrom = true
		}
	}
	if toInShard {
		if bal, err := n.store.GetBalance(toID); err == nil {
			before.beforeTo = bal
			before.trackTo = true
		}
	}
	n.lock.Unlock()

	if err := n.store.SaveWAL(seq, fromID, toID, before.beforeFrom, before.beforeTo, before.trackFrom, before.trackTo); err != nil {
		return err
	}
	log.Printf("[%s] WAL saved seq=%d fromID=%d trackFrom=%v beforeFrom=%d toID=%d trackTo=%v beforeTo=%d", n.addr, seq, fromID, before.trackFrom, before.beforeFrom, toID, before.trackTo, before.beforeTo)

	if err := n.executeCrossShard(txn); err != nil {
		return err
	}
	log.Printf("[%s] executeCrossShardWithWal applied seq=%d", n.addr, seq)
	return nil
}

// undoWithWal reverts state using stored WAL snapshot if available.
func (n *Node) undoWithWal(seq int64, txn *node.TxnMessage) {
	if n.store == nil || txn == nil || txn.Txn == nil {
		_ = n.undoCrossShard(txn)
		return
	}
	snap, ok, err := n.store.LoadWAL(seq)
	if err != nil {
		log.Printf("[%s] undoWithWal: loadWAL error: %v", n.addr, err)
	}
	if !ok {
		_ = n.undoCrossShard(txn)
		return
	}
	log.Printf("[%s] undoWithWal: restoring seq=%d fromID=%d trackFrom=%v beforeFrom=%d toID=%d trackTo=%v beforeTo=%d", n.addr, seq, snap.FromID, snap.TrackFrom, snap.BeforeFrom, snap.ToID, snap.TrackTo, snap.BeforeTo)
	defer n.store.DeleteWAL(seq)
	fromID, _ := strconv.Atoi(txn.Txn.From)
	toID, _ := strconv.Atoi(txn.Txn.To)

	if snap.TrackFrom {
		_ = n.store.SetBalance(fromID, snap.BeforeFrom)
	}
	if snap.TrackTo {
		_ = n.store.SetBalance(toID, snap.BeforeTo)
	}
	log.Printf("[%s] undoWithWal: restored balances for seq=%d", n.addr, seq)
}

// logTxnBalances writes the current balances of the txn endpoints to the log file.
func (n *Node) logTxnBalances(txn *node.TxnMessage) {
	if txn == nil || txn.Txn == nil || n.store == nil {
		return
	}
	fromID, _ := strconv.Atoi(txn.Txn.From)
	toID, _ := strconv.Atoi(txn.Txn.To)
	fromBal, err1 := n.store.GetBalance(fromID)
	toBal, err2 := n.store.GetBalance(toID)
	log.Printf("[%s] Balances after txn %s->%s ts=%s : from=%d (err=%v) to=%d (err=%v)",
		n.addr, txn.Txn.From, txn.Txn.To, txn.Timestamp, fromBal, err1, toBal, err2)
}

// printTwoPCLogEntry emits a single 2PC log entry to the node log for debugging.
func (n *Node) printTwoPCLogEntry(e *TwoPCLogEntry) {
	if e == nil || e.txn == nil || e.txn.Txn == nil {
		return
	}
	log.Printf("[%s] 2PC log: status=%s paxos=%s phase=%s seq=%d ballot=%d txn=%s->%s amt=%.2f ts=%s",
		n.addr, e.status, e.paxosStatus, e.phase.String(), e.seq, e.ballot, e.txn.Txn.From, e.txn.Txn.To, e.txn.Txn.Amount, e.timestamp)
}

// run2PCPrepareAsCoordinator runs Prepare2PC on participant leader and locally.
func (n *Node) run2PCPrepareAsCoordinator(txn *node.TxnMessage, participantPeers []string) error {
	// allocate seq for coordinator's view of this 2PC
	n.lock.Lock()
	// n.seq++
	n.twoPCSeq = n.seq + 1
	seq := n.seq + 1
	ballot := n.ballot
	n.lock.Unlock()

	// check sender balance locally; abort early if insufficient
	if n.store != nil {
		fromID, _ := strconv.Atoi(txn.Txn.From)
		if bal, err := n.store.GetBalance(fromID); err == nil && bal < int64(txn.Txn.Amount) {
			log.Printf("[%s] run2PCPrepareAsCoordinator: insufficient funds (%d < %.0f), aborting", n.addr, bal, txn.Txn.Amount)
			n.appendTwoPCLog("A", "Abort", seq, txn, node.TwoPCPhase_PREPARE_PHASE, n.addr, ballot)
			idx := n.findInsertIndex(seq, "Abort")
			n.appendLogEntry(idx, "Abort", seq, txn, n.addr, ballot)
			n.printLog()
			return fmt.Errorf("insufficient funds")
		}
	}

	// participant prepare for receiver shard
	prepSeq, err := callPrepare2PCRPC(participantPeers, txn)
	if err != nil {
		log.Printf("[%s] run2PCPrepareAsCoordinator: participant prepare failed: %v", n.addr, err)
		return fmt.Errorf("participant prepare failed: %w", err)
	}

	// debit sender on coordinator shard via 2PC replication (prepare phase)
	localSeq, err := n.Prepare2PC(context.Background(), &node.Prepare2PCReq{Request: txn})
	if err != nil {
		log.Printf("[%s] run2PCPrepareAsCoordinator: local prepare failed: %v", n.addr, err)
		return fmt.Errorf("coordinator prepare failed: %w", err)
	}

	log.Printf("[%s] run2PCPrepareAsCoordinator: prepare succeeded; starting commit phase", n.addr)

	// trigger commit phase Paxos on participant and coordinator clusters (no execution)
	if err := callCommitPhase2PCRPC(participantPeers, txn, prepSeq, 0, node.TwoPCPhase_COMMIT_PHASE); err != nil {
		log.Printf("[%s] run2PCPrepareAsCoordinator: participant commit phase failed: %v", n.addr, err)
		return fmt.Errorf("participant commit failed: %w", err)
	}
	if _, err := n.CommitPhase2PC(context.Background(), &node.CommitPhase2PCReq{
		Msg:    txn,
		Seq:    localSeq.Seq,
		Ballot: 0,
		Phase:  node.TwoPCPhase_COMMIT_PHASE,
	}); err != nil {
		log.Printf("[%s] run2PCPrepareAsCoordinator: local commit phase failed: %v", n.addr, err)
		return fmt.Errorf("coordinator commit failed: %w", err)
	}
	log.Printf("[%s] run2PCPrepareAsCoordinator: commit phase succeeded, seq: %d, prepSeq: %d", n.addr, localSeq.Seq, prepSeq)
	// mark as executed in local paxos log for this seq
	// n.lock.Lock()
	// execIdx := n.findInsertIndex(localSeq.Seq, "Execute")
	// n.appendLogEntry(execIdx, "Execute", localSeq.Seq, txn, n.addr, n.ballot)
	// n.lock.Unlock()
	return nil
}

func findLeader(peers []string) string {
	for _, addr := range peers {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := node.NewNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := client.PrintStatus(ctx, &node.PrintStatusRequest{})
		cancel()
		conn.Close()
		if err == nil && len(resp.Statuses) > 0 && resp.Statuses[0].Status == "LEADER" {
			return addr
		}
	}
	return ""
}

func callPrepare2PCRPC(addrs []string, msg *node.TxnMessage) (int64, error) {
	var lastErr error
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			lastErr = err
			continue
		}
		cli := node.NewNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := cli.Prepare2PC(ctx, &node.Prepare2PCReq{Request: msg})
		cancel()
		conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("nil prepare2pc resp")
			continue
		}
		if !resp.Ok {
			lastErr = fmt.Errorf(resp.Reason)
			continue
		}
		return resp.Seq, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("prepare2pc failed")
	}
	return 0, lastErr
}

func callCommitPhase2PCRPC(addrs []string, msg *node.TxnMessage, seq int64, ballot int32, _ node.TwoPCPhase) error {
	var lastErr error
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			lastErr = err
			continue
		}
		cli := node.NewNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := cli.CommitPhase2PC(ctx, &node.CommitPhase2PCReq{
			Msg:    msg,
			Seq:    seq,
			Ballot: ballot,
			Phase:  node.TwoPCPhase_COMMIT_PHASE,
		})
		cancel()
		conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("nil commitphase2pc resp")
			continue
		}
		if !resp.Ok {
			lastErr = fmt.Errorf(resp.Reason)
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("commit phase failed")
	}
	return lastErr
}

func (n *Node) Commit(ctx context.Context, in *node.CommitReq) (*node.Acknowledgement, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.resetHeartbeatTimer()
	n.lock.Lock()
	defer n.lock.Unlock()

	insert := 0
	for insert < len(n.logTable) && n.logTable[insert].seq <= in.Seq {
		insert++
	}
	if !n.isStatusAchieved(in.Seq, "Commit") {
		insert := n.findInsertIndex(in.Seq, "Commit")
		n.appendLogEntry(insert, "Commit", in.Seq, in.Msg, "", in.Ballot)
		n.printLog()
	}

	for {
		next := n.executeIdx + 1
		entry := n.getLogEntry(int64(next))
		// skip checkpoint-only slots so they don't block execution
		if entry == nil {
			if n.isStatusAchieved(int64(next), "Checkpoint") {
				n.executeIdx = next
				continue
			}
			break
		}
		if !n.isStatusAchieved(int64(next), "Commit") || n.isStatusAchieved(int64(next), "Execute") {
			break
		}

		n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)

		i := 0
		for i < len(n.logTable) && n.logTable[i].seq <= entry.seq {
			i++
		}
		if !n.isStatusAchieved(entry.seq, "Execute") {
			i := n.findInsertIndex(in.Seq, "Execute")
			n.appendLogEntry(i, "Execute", entry.seq, entry.txn, "", in.Ballot)
			n.printLog()
		}
		// release any intra-shard locks held for this seq
		if li, ok := n.pendingLocks[entry.seq]; ok {
			n.locks.Unlock(li.keys, li.txnID)
			delete(n.pendingLocks, entry.seq)
			go n.tryDrainQueued()
			go n.tryDrain2PCPending()
		}

		prevExec := n.executeIdx
		n.executeIdx = next

		if n.checkpointInterval > 0 {
			start := ((prevExec / n.checkpointInterval) + 1) * n.checkpointInterval
			for m := start; m <= n.executeIdx; m += n.checkpointInterval {
				if m <= 0 {
					continue
				}
				d := n.computeDigestFromDBLocked()
				cpMsg := &node.TxnMessage{
					Timestamp: entry.txn.Timestamp,
					Client:    "CHECKPOINT",
					Txn:       &node.Transaction{From: d, To: "", Amount: 0},
				}
				j := 0
				for j < len(n.logTable) && n.logTable[j].seq <= int64(m) {
					j++
				}
				if !n.isStatusAchieved(int64(m), "Checkpoint") {
					j := n.findInsertIndex(int64(m), "Checkpoint")
					n.appendLogEntry(j, "Checkpoint", int64(m), cpMsg, n.addr, n.ballot)
				}

				if n.leader == n.addr {
					seq := int64(m)
					digest := d
					ballot := n.ballot
					ts := entry.txn.Timestamp
					go func() {
						n.lock.Lock()
						n.applyCheckpointLocked(seq, digest, ballot, ts)
						n.lock.Unlock()
						n.broadcastCheckpoint(seq)
					}()
				}

				n.applyCheckpointLocked(int64(m), d, n.ballot, entry.txn.Timestamp)
			}
		}
	}

	if in.Msg != nil {
		if ch, ok := n.awaiting[in.Msg.Timestamp]; ok {
			if r := n.lastReply[in.Msg.Txn.From]; r != nil {
				select {
				case ch <- r:
				default:
				}
				delete(n.awaiting, in.Msg.Timestamp)
			}
		}
	}

	return &node.Acknowledgement{Status: "Committed", Node: n.addr}, nil
}

func (n *Node) Heartbeat(ctx context.Context, hb *node.HeartbeatMsg) (*node.Acknowledgement, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.lock.Lock()
	if hb.Ballot < n.ballot {
		log.Printf("[%s] Ignoring heartbeat from %s due to smaller ballot %d < %d", n.addr, hb.Leader, hb.Ballot, n.ballot)
		n.lock.Unlock()
		return &node.Acknowledgement{Status: "STALE", Node: n.addr}, nil
	}

	n.leader = hb.Leader
	n.ballot = hb.Ballot
	n.lock.Unlock()
	n.resetHeartbeatTimer()
	return &node.Acknowledgement{Status: "ALIVE", Node: n.addr}, nil
}

func (n *Node) Prepare(ctx context.Context, req *node.PrepareReq) (*node.PromiseMsg, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.lock.Lock()

	log.Printf("[%s] got Prepare, ballot %d (current %d)", n.addr, req.Ballot, n.ballot)
	if req.Ballot < n.ballot {
		n.lock.Unlock()
		return &node.PromiseMsg{Ballot: n.ballot, Ok: false, Node: n.addr}, nil
	}
	n.ballot = req.Ballot
	n.leader = ""
	n.lock.Unlock()
	n.resetHeartbeatTimer()

	n.lock.Lock()
	var logs = []*node.AcceptLog{}

	for _, log := range n.logTable {
		logs = append(logs, &node.AcceptLog{
			Ballot:    log.ballot,
			Seq:       log.seq,
			Txn:       log.txn,
			Status:    log.status,
			Timestamp: log.timestamp,
			Node:      log.node,
		})
	}
	n.lock.Unlock()

	return &node.PromiseMsg{Ballot: req.Ballot, Ok: true, Log: logs, Node: n.addr, CpSeq: n.lastCheckpointSeq, CpDigest: n.lastCheckpointDigest}, nil
}

func (n *Node) NewView(ctx context.Context, req *node.NewViewRequest) (*node.NewViewReply, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.lock.Lock()
	defer n.lock.Unlock()

	log.Printf("[%s] RECEIVED NEW-VIEW from %s (ballot %d)", n.addr, req.Sender, req.Ballot)
	n.addNewViewRecordLocked(req.Sender, req.Ballot, len(req.Entries))

	if req.Ballot <= n.ballot {
		return &node.NewViewReply{Success: false}, nil
	}
	n.logTable = mergeLogs(n.logTable, req.Entries)
	n.logMap = rebuildLogMap(n.logTable)
	n.applyMergedLogStateLocked()

	n.viewTable = append(n.viewTable, &ViewEntry{
		ballot: n.ballot,
		node:   n.addr,
	})
	log.Println("New View RPC :: Logs are")
	n.printLog()

	for _, view := range n.viewTable {
		log.Printf("(%d, %s, %+v)\n", view.ballot, view.node, view.log)
	}
	maxSeq := int64(0)
	for _, entry := range n.logTable {
		if entry.seq > maxSeq {
			maxSeq = entry.seq
		}
	}
	if maxSeq > n.seq {
		n.seq = maxSeq
	}
	n.leader = req.Sender
	n.ballot = req.Ballot

	for _, entry := range n.logTable {
		if _, ok := n.pendingAccepted[entry.seq]; !ok {
			n.pendingAccepted[entry.seq] = make(chan *node.AcceptedMsg, len(n.ports))
		}
	}

	snapBallot := n.ballot
	snap := make([]*LogEntry, len(n.logTable))
	copy(snap, n.logTable)
	go func(ballot int32, logs []*LogEntry) {
		for _, entry := range logs {
			if entry.status != "Execute" && entry.status != "Commit" && entry.status == "Accepted" {
				if _, ok := n.pendingAccepted[entry.seq]; !ok {
					n.pendingAccepted[entry.seq] = make(chan *node.AcceptedMsg, len(n.ports))
				}
			}
		}
	}(snapBallot, snap)

	return &node.NewViewReply{Success: true}, nil
}

func (n *Node) GetViewEntries(ctx context.Context, _ *node.GetViewEntriesReq) (*node.GetViewEntriesResp, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	out := make([]*node.GetViewEntriesResp_View, 0, len(n.viewTable))
	for _, v := range n.viewTable {
		out = append(out, &node.GetViewEntriesResp_View{
			Ballot:    int64(v.ballot),
			Leader:    v.node,
			Sender:    v.node,
			Timestamp: strconv.FormatInt(time.Now().UnixNano(), 10),
		})
	}
	return &node.GetViewEntriesResp{Views: out}, nil
}

// PrintView returns all NewView messages exchanged since the last ResetDB (test-case start).
func (n *Node) PrintView(ctx context.Context, _ *node.PrintViewRequest) (*node.PrintViewResponse, error) {
	n.lock.Lock()
	defer n.lock.Unlock()
	resp := &node.PrintViewResponse{}
	for _, v := range n.viewHistory {
		resp.NewViews = append(resp.NewViews, &node.PrintViewResponse_NewViewInfo{
			Sender:     v.sender,
			Ballot:     v.ballot,
			EntryCount: v.entryCount,
			Timestamp:  v.timestamp,
		})
	}
	if len(resp.NewViews) == 0 {
		resp.Message = "no new-view messages recorded"
	}
	return resp, nil
}

func (n *Node) GetLogs(ctx context.Context, _ *node.GetLogsReq) (*node.GetLogsResp, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	logMap := latestLogBySeq(n.logTable)
	rows := make([]*node.GetLogsResp_Row, 0, len(logMap))
	seqs := make([]int64, 0, len(logMap))
	for k := range logMap {
		seqs = append(seqs, k)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, s := range seqs {
		e := logMap[s]
		if e == nil || e.txn == nil || e.txn.Txn == nil {
			continue
		}
		rows = append(rows, &node.GetLogsResp_Row{
			Status:    e.status,
			Seq:       e.seq,
			Ballot:    e.ballot,
			From:      e.txn.Txn.From,
			To:        e.txn.Txn.To,
			Amount:    e.txn.Txn.Amount,
			Node:      e.node,
			Timestamp: e.timestamp,
		})
	}
	return &node.GetLogsResp{Rows: rows}, nil
}

func (n *Node) GetTxnStatus(ctx context.Context, in *node.GetTxnStatusReq) (*node.GetTxnStatusResp, error) {
	seq := int64(in.Seq)
	order := func(s string) int {
		switch s {
		case "Execute":
			return 4
		case "Commit":
			return 3
		case "Accepted":
			return 2
		default:
			return 1
		}
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	var best *LogEntry
	for _, e := range n.logTable {
		if e == nil || e.txn == nil {
			continue
		}
		if int64(e.seq) != seq {
			continue
		}
		if best == nil || order(e.status) > order(best.status) {
			best = e
		}
	}
	if best == nil {
		return &node.GetTxnStatusResp{Status: "UNKNOWN", Seq: 0, Ballot: 0}, nil
	}
	return &node.GetTxnStatusResp{Status: best.status, Seq: best.seq, Ballot: best.ballot}, nil
}

func (n *Node) GetDB(ctx context.Context, _ *node.GetDBReq) (*node.GetDBResp, error) {
	n.lock.Lock()
	st := n.store
	n.lock.Unlock()
	if st == nil {
		return &node.GetDBResp{Balances: map[string]float64{}}, nil
	}

	bals, err := st.ListModifiedBalances()
	if err != nil {
		return nil, err
	}

	return &node.GetDBResp{Balances: bals}, nil

}

func (n *Node) GetBalance(ctx context.Context, in *node.GetBalanceReq) (*node.GetBalanceResp, error) {
	if n.isDisabled() {
		return &node.GetBalanceResp{Ok: false, Msg: "disabled", Node: n.addr}, nil
	}
	n.lock.Lock()
	st := n.store
	n.lock.Unlock()
	if st == nil {
		return &node.GetBalanceResp{Ok: false, Msg: "store not initialized", Node: n.addr}, nil
	}
	bal, err := st.GetBalance(int(in.Id))
	if err != nil {
		return &node.GetBalanceResp{Ok: false, Msg: err.Error(), Node: n.addr}, nil
	}
	return &node.GetBalanceResp{Ok: true, Balance: float64(bal), Node: n.addr}, nil
}

// SetBalance writes an explicit balance (used for migration/resharding).
func (n *Node) SetBalance(ctx context.Context, in *node.SetBalanceReq) (*node.SetBalanceResp, error) {
	if n.isDisabled() {
		return &node.SetBalanceResp{Ok: false, Msg: "disabled", Node: n.addr}, nil
	}
	if in == nil {
		return &node.SetBalanceResp{Ok: false, Msg: "invalid request", Node: n.addr}, nil
	}
	id := int(in.Id)
	n.lock.Lock()
	st := n.store
	n.lock.Unlock()
	if st == nil {
		return &node.SetBalanceResp{Ok: false, Msg: "store not initialized", Node: n.addr}, nil
	}
	if err := st.SetBalance(id, int64(in.Value)); err != nil {
		return &node.SetBalanceResp{Ok: false, Msg: err.Error(), Node: n.addr}, nil
	}
	// mark modified when value > 0; clear modified when zeroing
	if in.Value == 0 {
		_ = st.ClearModified(id)
	} else {
		_ = st.MarkModified(id)
	}
	log.Printf("[%s] SetBalance id=%d val=%.0f", n.addr, id, in.Value)
	return &node.SetBalanceResp{Ok: true, Node: n.addr}, nil
}

func (n *Node) Checkpoint(ctx context.Context, in *node.CheckpointMsg) (*node.Acknowledgement, error) {
	if n.isDisabled() {
		return nil, fmt.Errorf("[%s] node is disabled", n.addr)
	}
	n.resetHeartbeatTimer()

	n.lock.Lock()
	defer n.lock.Unlock()

	n.applyCheckpointLocked(in.Seq, in.Digest, in.Ballot, in.Timestamp)

	return &node.Acknowledgement{Status: "CHECKPOINTED", Node: n.addr}, nil
}

func (n *Node) computeDigestFromDBLocked() string {
	ids := make([]string, 0, len(n.db))
	for k := range n.db {
		ids = append(ids, k)
	}
	sort.Strings(ids)

	h := sha256.New()
	for _, id := range ids {
		fmt.Fprintf(h, "%s:%0.2f|", id, n.db[id])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (n *Node) applyCheckpointLocked(seq int64, digest string, ballot int32, ts string) {
	cpMsg := &node.TxnMessage{
		Timestamp: ts,
		Client:    "CHECKPOINT",
		Txn: &node.Transaction{
			From:   digest,
			To:     "",
			Amount: 0,
		},
	}
	if !n.isStatusAchieved(seq, "Checkpoint") {
		idx := 0
		for idx < len(n.logTable) && n.logTable[idx].seq <= seq {
			idx++
		}
		idx = n.findInsertIndex(seq, "Checkpoint")
		n.appendLogEntry(idx, "Checkpoint", seq, cpMsg, n.addr, ballot)
	}

	n.lastCheckpointSeq = seq
	n.lastCheckpointBallot = ballot
	n.lastCheckpointDigest = digest
}

func (n *Node) broadcastCheckpoint(seq int64) {
	n.lock.Lock()
	d := n.computeDigestFromDBLocked()
	b := n.ballot
	ts := time.Now().Format(time.RFC3339Nano)
	n.lock.Unlock()

	req := &node.CheckpointMsg{Ballot: b, Seq: seq, Digest: d, Timestamp: ts}

	for _, p := range n.ports {
		go func(peer string) {
			c := n.grpcConns[peer]
			if c == nil {
				return
			}
			_, _ = c.Checkpoint(context.Background(), req)
		}(p)
	}
}

func (n *Node) tryDrainQueued() {
	if n.isDisabled() {
		return
	}

	n.lock.Lock()
	isLeader := (n.leader == n.addr)
	n.lock.Unlock()
	if !isLeader {
		return
	}

	quorum := (len(n.clusterPeers()) / 2) + 1
	if n.countLive() < quorum {
		return
	}

	for {
		var qt *queuedTxn

		n.lock.Lock()
		if len(n.pendingQueue) == 0 {
			n.lock.Unlock()
			return
		}
		qt = n.pendingQueue[0]
		n.pendingQueue = n.pendingQueue[1:]
		n.lock.Unlock()

		reply, err := n.proposeAndCommit(qt.msg)
		if err != nil {
			qt.attempts++
			if qt.attempts >= 5 {
				// give up on this txn and return failure to waiting client
				if qt.ch != nil {
					fail := &node.Reply{Ballot: n.ballot, Timestamp: qt.msg.Timestamp, Client: qt.msg.Client, Reply: false, Node: n.addr}
					select {
					case qt.ch <- fail:
					default:
					}
				}
				// proceed to next queued txn (if any)
				continue
			}
			n.lock.Lock()
			n.pendingQueue = append(n.pendingQueue, qt)
			n.lock.Unlock()
			return
		}

		if qt.ch != nil {
			select {
			case qt.ch <- reply:
			default:
			}
		}
	}
}

func (n *Node) proposeAndCommit(in *node.TxnMessage) (*node.Reply, error) {
	peers := n.clusterPeers()

	if rep := n.findExistingTxnReply(in); rep != nil {
		return rep, nil
	}

	// intra-shard lock acquisition on from/to; if locked, enqueue for retry
	txnID := fmt.Sprintf("%s-%s-%s", in.Txn.From, in.Txn.To, in.Timestamp)
	isLocal := func(id string) bool {
		v, err := strconv.Atoi(id)
		if err != nil {
			return false
		}
		start, end := shardRangeForNode(n.addr)
		return v >= start && v <= end
	}
	needsLock := isLocal(in.Txn.From) && isLocal(in.Txn.To) && in.Txn.From != "NOOP" && in.Txn.To != "NOOP"
	keys := []string{in.Txn.From, in.Txn.To}
	if needsLock && !n.locks.TryLock(keys, txnID) {
		log.Printf("[%s] proposeAndCommit: locks held for %v, ask client to retry", n.addr, keys)
		return &node.Reply{Ballot: n.ballot, Timestamp: in.Timestamp, Client: in.Client, Reply: false, Node: n.addr}, fmt.Errorf("lock held")
	}

	n.lock.Lock()
	n.seq++
	seq := n.seq
	curBallot := n.ballot
	acceptChan := make(chan *node.AcceptedMsg, len(n.ports))
	n.pendingAccepted[seq] = acceptChan
	if needsLock {
		n.pendingLocks[seq] = lockInfo{keys: keys, txnID: txnID}
	}
	n.lock.Unlock()

	if needsLock {
		defer func(localSeq int64) {
			n.lock.Lock()
			if li, ok := n.pendingLocks[localSeq]; ok {
				n.locks.Unlock(li.keys, li.txnID)
				delete(n.pendingLocks, localSeq)
			}
			n.lock.Unlock()
			go n.tryDrainQueued()
		}(seq)
	}

	if _, err := n.Accept(context.Background(), &node.AcceptMsg{Ballot: curBallot, Seq: seq, Msg: in}); err == nil {
		n.acceptedBuf <- &node.AcceptedMsg{Node: n.addr, Ballot: curBallot, Seq: seq, Msg: in}
	}

	for _, peer := range peers {
		if peer == n.addr {
			continue
		}
		go func(p string) {
			for {
				n.lock.Lock()
				client := n.grpcConns[p]
				n.lock.Unlock()
				if client == nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := client.Accept(ctx2, &node.AcceptMsg{Ballot: curBallot, Seq: seq, Msg: in})
				cancel()
				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				n.acceptedBuf <- resp
				break
			}
		}(peer)
	}

	quorum := (len(peers) / 2) + 1
	seen := map[string]bool{}
	timeout := time.After(10 * time.Second)
	chosen := in

	for {
		select {
		case msg := <-acceptChan:
			if msg.Seq != seq {
				continue
			}
			if !sameTxn(msg.Msg, chosen) {
				chosen = msg.Msg
				for _, peer := range n.ports {
					if peer == n.addr {
						continue
					}
					go func(p string) {
						n.lock.Lock()
						client := n.grpcConns[p]
						n.lock.Unlock()
						if client == nil {
							return
						}
						ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						resp, err := client.Accept(ctx2, &node.AcceptMsg{Ballot: curBallot, Seq: seq, Msg: chosen})
						cancel()
						if err == nil && resp != nil {
							n.acceptedBuf <- resp
						}
					}(peer)
				}
				if _, err := n.Accept(context.Background(), &node.AcceptMsg{Ballot: curBallot, Seq: seq, Msg: chosen}); err == nil {
					n.acceptedBuf <- &node.AcceptedMsg{Node: n.addr, Ballot: curBallot, Seq: seq, Msg: chosen}
				}
				seen = map[string]bool{}
				continue
			}
			if !seen[msg.Node] {
				seen[msg.Node] = true
			}
			if len(seen) >= quorum {
				n.lock.Lock()
				delete(n.pendingAccepted, seq)
				insertIdx := 0
				for insertIdx < len(n.logTable) && n.logTable[insertIdx].seq < seq {
					insertIdx++
				}
				if !n.isStatusAchieved(seq, "Accepted") {
					insertIdx := n.findInsertIndex(seq, "Accepted")
					n.appendLogEntry(insertIdx, "Accepted", seq, chosen, n.addr, curBallot)
				}
				n.lock.Unlock()
				n.broadcastCommit(seq, chosen)

				n.lock.Lock()
				reply := n.lastReply[chosen.Txn.From]
				if ch, ok := n.awaiting[chosen.Timestamp]; ok && reply != nil {
					select {
					case ch <- reply:
					default:
					}
					delete(n.awaiting, chosen.Timestamp)
				}
				n.lock.Unlock()

				if reply == nil {
					return &node.Reply{Ballot: n.ballot, Timestamp: in.Timestamp, Client: in.Client, Reply: true, Node: n.addr}, nil
				}
				return reply, nil
			}
		case <-timeout:
			n.lock.Lock()
			delete(n.pendingAccepted, seq)
			n.lock.Unlock()
			return nil, fmt.Errorf("timeout waiting for quorum")
		}
	}
}

func sameTxn(a, b *node.TxnMessage) bool {
	if a == nil || b == nil || a.Txn == nil || b.Txn == nil {
		return false
	}
	return a.Txn.From == b.Txn.From && a.Txn.To == b.Txn.To && a.Txn.Amount == b.Txn.Amount
}

func sameClusterPeers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, x := range a {
		set[x] = true
	}
	for _, y := range b {
		if !set[y] {
			return false
		}
	}
	return true
}

func (n *Node) countLive() int {
	live := 0
	if !n.isDisabled() {
		live++
	}

	n.lock.Lock()
	peers := append([]string(nil), n.clusterPeers()...)
	conns := make(map[string]node.NodeClient, len(n.grpcConns))
	for k, v := range n.grpcConns {
		conns[k] = v
	}
	n.lock.Unlock()

	for _, p := range peers {
		if p == n.addr {
			continue
		}
		c := conns[p]
		if c == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		resp, err := c.PrintStatus(ctx, &node.PrintStatusRequest{})
		cancel()
		if err != nil || resp == nil || len(resp.Statuses) == 0 {
			continue
		}
		if resp.Statuses[0].Status != "DISABLED" {
			live++
		}
	}
	return live
}

func mergeLogs(existing []*LogEntry, newLogs []*node.AcceptLog) []*LogEntry {
	best := make(map[int64]*node.AcceptLog)
	maxSeq := int64(0)

	better := func(cur *node.AcceptLog, candBallot int32, candStatus string) bool {
		if cur == nil {
			return true
		}
		rc := phaseRank(cur.Status)
		rn := phaseRank(candStatus)
		if rn > rc {
			return true
		}
		if rn < rc {
			return false
		}
		return candBallot > cur.Ballot
	}

	// use logMap (latest per seq) for merging existing state
	existingMap := latestLogBySeq(existing)
	for _, e := range existingMap {
		if e == nil {
			continue
		}
		if e.seq > maxSeq {
			maxSeq = e.seq
		}
		cur := best[e.seq]
		if better(cur, e.ballot, e.status) {
			best[e.seq] = &node.AcceptLog{
				Ballot:    e.ballot,
				Seq:       e.seq,
				Txn:       e.txn,
				Status:    e.status,
				Timestamp: e.timestamp,
				Node:      e.node,
			}
		}
	}

	for _, l := range newLogs {
		if l == nil || l.Txn == nil || l.Txn.Txn == nil {
			continue
		}
		if l.Seq > maxSeq {
			maxSeq = l.Seq
		}
		cur := best[l.Seq]
		if better(cur, l.Ballot, l.Status) {
			best[l.Seq] = l
		}
	}

	result := make([]*LogEntry, 0, maxSeq)
	for seq := int64(1); seq <= maxSeq; seq++ {
		if entry, ok := best[seq]; ok {
			result = append(result, &LogEntry{
				idx:       seq - 1,
				status:    entry.Status,
				timestamp: entry.Timestamp,
				seq:       entry.Seq,
				ballot:    entry.Ballot,
				txn:       entry.Txn,
				node:      entry.Node,
			})
		} else {
			nowTs := strconv.FormatInt(time.Now().UnixNano(), 10)
			noOp := &node.TxnMessage{
				Txn:       &node.Transaction{From: "NOOP", To: "NOOP", Amount: 0},
				Timestamp: nowTs, Client: "NOOP",
			}
			result = append(result, &LogEntry{
				idx:       seq - 1,
				status:    "Noop",
				timestamp: nowTs,
				seq:       seq,
				ballot:    0,
				txn:       noOp,
				node:      "NOOP",
			})
		}
	}
	return result
}

// latestLogBySeq returns the highest-phase log entry per sequence number.
func latestLogBySeq(entries []*LogEntry) map[int64]*LogEntry {
	bySeq := make(map[int64]*LogEntry, len(entries))
	for _, e := range entries {
		if e == nil {
			continue
		}
		cur, ok := bySeq[e.seq]
		if !ok || phaseRank(e.status) > phaseRank(cur.status) || (phaseRank(e.status) == phaseRank(cur.status) && e.ballot > cur.ballot) {
			bySeq[e.seq] = e
		}
	}
	return bySeq
}

// rebuildLogMap constructs a seq->entry map from the provided log slice.
func rebuildLogMap(entries []*LogEntry) map[int64]*LogEntry {
	return latestLogBySeq(entries)
}

// applyMergedLogStateLocked ensures merged logs are promoted to highest state, executed, and gaps filled with NOOP.
// Caller must hold n.lock.
func (n *Node) applyMergedLogStateLocked() {
	logMap := latestLogBySeq(n.logTable)
	maxSeq := int64(0)
	for s := range logMap {
		if s > maxSeq {
			maxSeq = s
		}
	}
	if maxSeq < n.seq {
		maxSeq = n.seq
	}

	for seq := int64(1); seq <= maxSeq; seq++ {
		entry := logMap[seq]
		if entry == nil {
			ts := strconv.FormatInt(time.Now().UnixNano(), 10)
			noOp := &node.TxnMessage{
				Txn:       &node.Transaction{From: "NOOP", To: "NOOP", Amount: 0},
				Timestamp: ts,
				Client:    "NOOP",
			}
			idx := n.findInsertIndex(seq, "Execute")
			n.appendLogEntry(idx, "Execute", seq, noOp, n.addr, 0)
			continue
		}

		switch entry.status {
		case "Accepted":
			if !n.isStatusAchieved(seq, "Commit") {
				idx := n.findInsertIndex(seq, "Commit")
				n.appendLogEntry(idx, "Commit", seq, entry.txn, n.addr, entry.ballot)
			}
			if !n.isStatusAchieved(seq, "Execute") {
				idx := n.findInsertIndex(seq, "Execute")
				n.appendLogEntry(idx, "Execute", seq, entry.txn, n.addr, entry.ballot)
				n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)
			}
		case "Commit":
			if !n.isStatusAchieved(seq, "Execute") {
				idx := n.findInsertIndex(seq, "Execute")
				n.appendLogEntry(idx, "Execute", seq, entry.txn, n.addr, entry.ballot)
				n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)
			}
		case "Execute":
			// The leader may have already promoted this slot to Execute, but a
			// recovering follower with a fresh store still needs to apply it.
			// if n.executeIdx < int(seq) {
			// 	if !n.isStatusAchieved(seq, "Execute") {
			// 		idx := n.findInsertIndex(seq, "Execute")
			// 		n.appendLogEntry(idx, "Execute", seq, entry.txn, n.addr, entry.ballot)
			// 	}
			// 	if entry.txn != nil && entry.txn.Txn != nil && entry.txn.Txn.From != "NOOP" {
			// 		n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)
			// 	}
			// }
		case "Checkpoint":
			// already executed
		default:
			if !n.isStatusAchieved(seq, "Execute") {
				idx := n.findInsertIndex(seq, "Execute")
				n.appendLogEntry(idx, "Execute", seq, entry.txn, n.addr, entry.ballot)
				if entry.txn != nil && entry.txn.Txn != nil && entry.txn.Txn.From != "NOOP" {
					n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)
				}
			}
		}
	}
	n.logMap = rebuildLogMap(n.logTable)
	for {
		next := n.executeIdx + 1
		if n.isStatusAchieved(int64(next), "Execute") || n.isStatusAchieved(int64(next), "Checkpoint") {
			n.executeIdx = next
		} else {
			break
		}
	}
}

func (n *Node) isDuplicateTxn(txn *node.TxnMessage) bool {
	for _, entry := range n.logTable {
		if entry.txn != nil && entry.txn.Txn != nil &&
			entry.txn.Txn.From == txn.Txn.From &&
			entry.txn.Txn.To == txn.Txn.To &&
			entry.txn.Txn.Amount == txn.Txn.Amount &&
			entry.txn.Timestamp == txn.Timestamp {
			return true
		}
	}
	return false
}

// findDuplicate2PCTxn checks the 2PC log for a matching txn (by endpoints, amount, timestamp).
// Returns the existing seq if found.
// func (n *Node) findDuplicate2PCTxn(txn *node.TxnMessage) (int64, bool) {
// 	if txn == nil || txn.Txn == nil {
// 		return 0, false
// 	}
// 	n.lock.Lock()
// 	defer n.lock.Unlock()
// 	for seq, e := range n.twoPCLog {
// 		if e == nil || e.txn == nil || e.txn.Txn == nil {
// 			continue
// 		}
// 		if e.txn.Txn.From == txn.Txn.From &&
// 			e.txn.Txn.To == txn.Txn.To &&
// 			e.txn.Txn.Amount == txn.Txn.Amount &&
// 			e.txn.Timestamp == txn.Timestamp {
// 			return seq, true
// 		}
// 	}
// 	return 0, false
// }

func (n *Node) sendNewView(follower string, entries []*LogEntry, ballot int32) {
	conn, err := grpc.Dial(follower, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to %s: %v", follower, err)
		return
	}
	defer conn.Close()

	client := node.NewNodeClient(conn)
	req := &node.NewViewRequest{
		Sender:  n.addr,
		Ballot:  ballot,
		Entries: convertToAcceptLog(entries),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.NewView(ctx, req)
	if err != nil {
		log.Printf("NewView RPC to %s failed: %v", follower, err)
		return
	}

	n.lock.Lock()
	n.addNewViewRecordLocked(n.addr, ballot, len(entries))
	n.lock.Unlock()
}

func convertToAcceptLog(entries []*LogEntry) []*node.AcceptLog {
	result := make([]*node.AcceptLog, len(entries))
	for i, e := range entries {
		result[i] = &node.AcceptLog{
			Seq:       e.seq,
			Ballot:    e.ballot,
			Txn:       e.txn,
			Status:    e.status,
			Timestamp: e.timestamp,
			Node:      e.node,
		}
	}
	return result
}

func (n *Node) broadcastCommit(seq int64, msg *node.TxnMessage) {
	n.lock.Lock()
	b := n.ballot
	n.lock.Unlock()
	for _, peer := range n.ports {
		go func(peer string) {
			if peer == n.addr {
				_, err := n.Commit(context.Background(), &node.CommitReq{Ballot: b, Seq: seq, Msg: msg})
				if err != nil {
					log.Printf("Local Commit failed: %v", err)
				}
				return
			}

			n.lock.Lock()
			client, ok := n.grpcConns[peer]
			n.lock.Unlock()

			if !ok || client == nil {
				log.Printf("broadcastCommit :: no client for peer %s, skipping commit", peer)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := client.Commit(ctx, &node.CommitReq{Ballot: b, Seq: seq, Msg: msg})
			if err != nil {
				log.Printf("Commit to %s failed: %v", peer, err)
			}
		}(peer)
	}
}

// broadcastCommitSync sends Commit to all peers (including self) and waits for responses or timeouts.
// This ensures commit/execute is applied cluster-wide before replying to the client.
func (n *Node) broadcastCommitSync(seq int64, msg *node.TxnMessage) {
	n.lock.Lock()
	b := n.ballot
	peers := append([]string(nil), n.ports...)
	n.lock.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if p == n.addr {
				if _, err := n.Commit(ctx, &node.CommitReq{Ballot: b, Seq: seq, Msg: msg}); err != nil {
					log.Printf("Local Commit sync failed: %v", err)
				}
				return
			}
			n.lock.Lock()
			client := n.grpcConns[p]
			n.lock.Unlock()
			if client == nil {
				log.Printf("broadcastCommitSync :: no client for peer %s, skipping commit", p)
				return
			}
			if _, err := client.Commit(ctx, &node.CommitReq{Ballot: b, Seq: seq, Msg: msg}); err != nil {
				log.Printf("Commit to %s failed (sync): %v", p, err)
			}
		}(peer)
	}
	wg.Wait()
}

// getLogEntry returns the commit entry for seq using the latest-state map, falling back to logTable.
// Callers must hold n.lock.
func (n *Node) getLogEntry(seq int64) *LogEntry {
	if entry := n.logMap[seq]; entry != nil && (entry.status == "Commit" || entry.status == "Execute") {
		return entry
	}
	for i := len(n.logTable) - 1; i >= 0; i-- {
		e := n.logTable[i]
		if e.seq == seq && (e.status == "Commit" || e.status == "Execute") {
			n.logMap[seq] = e // refresh map for future calls
			return e
		}
	}
	return nil
}

func (n *Node) isDisabled() bool {
	if n == nil {
		return true
	}
	// Read flags without taking the main lock to avoid self-deadlock on re-entrant checks.
	// if atomic.LoadInt32(&n.disabledFlag) == 1 || atomic.LoadInt32(&n.recoveringFlag) == 1 {
	// 	return true
	// }
	// Fallback to locked read for consistency.
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.disabled || n.recovering
}

// replayCommitted advances execution for any already-committed entries in the log.
func (n *Node) replayCommitted() {
	n.lock.Lock()
	defer n.lock.Unlock()

	for {
		next := n.executeIdx + 1
		entry := n.getLogEntry(int64(next))
		if entry == nil {
			break
		}
		if !n.isStatusAchieved(int64(next), "Commit") || n.isStatusAchieved(int64(next), "Execute") {
			break
		}

		n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)

		i := n.findInsertIndex(entry.seq, "Execute")
		if !n.isStatusAchieved(entry.seq, "Execute") {
			n.appendLogEntry(i, "Execute", entry.seq, entry.txn, "", entry.ballot)
		}
		if li, ok := n.pendingLocks[entry.seq]; ok {
			n.locks.Unlock(li.keys, li.txnID)
			delete(n.pendingLocks, entry.seq)
			go n.tryDrainQueued()
		}
		n.executeIdx = next
	}
}

// replayCommittedFromMap scans logMap to execute any committed entries in order.
// It helps when logMap has commit entries that logTable iteration might miss.
func (n *Node) replayCommittedFromMap() {
	n.lock.Lock()
	defer n.lock.Unlock()

	seqs := make([]int64, 0, len(n.logMap))
	for s := range n.logMap {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	for _, s := range seqs {
		if s != int64(n.executeIdx+1) {
			// maintain in-order execution
			continue
		}
		entry := n.logMap[s]
		if entry == nil || (entry.status != "Commit" && entry.status != "Execute") {
			break
		}
		if n.isStatusAchieved(s, "Execute") {
			n.executeIdx++
			continue
		}
		// Even if the log already carries an Execute entry, the local store may
		// not reflect it (fresh state after reset/catchup). Reapply when our
		// executeIdx shows we have not executed this slot yet.
		n.transaction(entry.txn.Txn.From, entry.txn.Txn.To, entry.txn.Txn.Amount, entry.txn.Timestamp)
		idx := n.findInsertIndex(entry.seq, "Execute")
		n.appendLogEntry(idx, "Execute", entry.seq, entry.txn, entry.node, entry.ballot)
		if li, ok := n.pendingLocks[entry.seq]; ok {
			n.locks.Unlock(li.keys, li.txnID)
			delete(n.pendingLocks, entry.seq)
			go n.tryDrain2PCPending()
		}
		n.executeIdx++
	}
}

// tryDrain2PCPending retries queued Prepare2PC requests when locks become free.
func (n *Node) tryDrain2PCPending() {
	n.lock.Lock()
	pending := n.pending2PC
	n.pending2PC = nil
	n.lock.Unlock()
	if len(pending) == 0 {
		return
	}
	for _, req := range pending {
		if req == nil {
			continue
		}
		if resp, _ := n.Prepare2PC(context.Background(), req); resp != nil && resp.Ok {
			continue
		}
		// still blocked; requeue
		n.lock.Lock()
		n.pending2PC = append(n.pending2PC, req)
		n.lock.Unlock()
	}
}

func (n *Node) connectPeers() {
	for _, port := range n.clusterPeers() {
		if port == n.addr {
			continue
		}
		go func(p string) {
			for {
				conn, err := grpc.Dial(p, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
				if err == nil {
					n.lock.Lock()
					n.grpcConns[p] = node.NewNodeClient(conn)
					log.Println("Connected with node:", p)
					n.lock.Unlock()
					return
				}
				log.Printf("connectPeers: retrying %s in 1s...", p)
				time.Sleep(1 * time.Second)
			}
		}(port)
	}
}

// peerClient returns an existing grpc client for peer or dials a new one.
func (n *Node) peerClient(peer string) node.NodeClient {
	n.lock.Lock()
	if c := n.grpcConns[peer]; c != nil {
		n.lock.Unlock()
		return c
	}
	n.lock.Unlock()

	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
	if err != nil {
		log.Printf("[%s] peerClient dial %s failed: %v", n.addr, peer, err)
		return nil
	}
	c := node.NewNodeClient(conn)
	n.lock.Lock()
	n.grpcConns[peer] = c
	n.lock.Unlock()
	return c
}

func (n *Node) startAcceptedHandler() {
	for msg := range n.acceptedBuf {
		n.lock.Lock()
		ch, ok := n.pendingAccepted[msg.Seq]
		n.lock.Unlock()
		if ok {
			select {
			case ch <- msg:
			default:
				log.Printf("dropping ACCEPTED msg for seq=%d", msg.Seq)
			}
		}
	}
}

func phaseRank(s string) int {
	switch s {
	case "Accepted":
		return 1
	case "Commit":
		return 2
	case "Execute":
		return 3
	case "Checkpoint":
		return 4
	default:
		return 0
	}
}

func (n *Node) hasPhaseAtOrAbove(seq int64, phase string) bool {
	want := phaseRank(phase)
	for _, e := range n.logTable {
		if e.seq == seq && phaseRank(e.status) >= want {
			return true
		}
	}
	return false
}

func (n *Node) findInsertIndex(seq int64, status string) int {
	r := phaseRank(status)
	i := 0
	for i < len(n.logTable) {
		e := n.logTable[i]
		if e.seq > seq {
			break
		}
		if e.seq == seq && phaseRank(e.status) > r {
			break
		}
		i++
	}
	return i
}

func (n *Node) addNewViewRecordLocked(sender string, ballot int32, count int) {
	ts := time.Now().Format(time.RFC3339Nano)
	n.viewHistory = append(n.viewHistory, newViewRecord{
		sender:     sender,
		ballot:     ballot,
		entryCount: int64(count),
		timestamp:  ts,
	})
}

func main() {
	bench := flag.Bool("bench", false, "run in quiet benchmark mode (no per-node logs)")
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("Usage: server [-bench] :<port>")
	}
	id := flag.Arg(0)

	loadClusterConfig()
	cl := clusterForAddr(id)
	if cl == nil {
		log.Fatalf("Unknown node id %s in cluster config", id)
	}
	peers := append([]string(nil), cl.Nodes...)

	if *bench {
		log.SetOutput(io.Discard)
	} else {
		if err := os.MkdirAll("logs", 0o755); err != nil {
			log.Fatalf("create logs dir: %v", err)
		}
		logFile := fmt.Sprintf("logs/log%s.log", strings.TrimPrefix(id, ":"))
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			log.Fatalf("open log file %s: %v", logFile, err)
		}
		log.SetOutput(f)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		defer f.Close()
	}

	log.Println("Creating new node at port", id)
	n := NewNode(id, peers)
	log.Println("Created new node")
	n.connectPeers()
	log.Println("Connected with peers")
	go n.startAcceptedHandler()

	lis, err := net.Listen("tcp", id)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	node.RegisterNodeServer(s, n)
	log.Println("Node running on", id)
	s.Serve(lis)
}
