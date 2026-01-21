package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	node "mpaxos_hw1/proto/hw1/node"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc"
)

type Transaction struct {
	from       string
	to         string
	amt        float64
	ts         string
	readOnly   bool
	readQuorum int
	op         string // "FAIL" | "RECOVER" or empty
	target     string // node address like :5001
}

type BacklogItem struct {
	seg segment
}

var backlog []BacklogItem
var backlogTxns []*Transaction
var perf perfTracker

var historyMu sync.Mutex
var txnHistory []Transaction

// Cluster configuration (dynamic).
type ClusterConfig struct {
	StartID int      `json:"start_id"`
	EndID   int      `json:"end_id"`
	Nodes   []string `json:"nodes"`
}

type Config struct {
	Clusters       []ClusterConfig `json:"clusters"`
	TotalIDs       int             `json:"total_ids,omitempty"`
	DefaultBalance int64           `json:"default_balance,omitempty"`
}

var (
	clusterCfg      Config
	ports           []string
	benchModeGlobal bool
)

// defaultClusterConfig returns the legacy static layout.
func defaultClusterConfig() Config {
	return Config{
		Clusters: []ClusterConfig{
			{StartID: 1, EndID: 3000, Nodes: []string{":5001", ":5002", ":5003"}},
			{StartID: 3001, EndID: 6000, Nodes: []string{":5004", ":5005", ":5006"}},
			{StartID: 6001, EndID: 9000, Nodes: []string{":5007", ":5008", ":5009"}},
		},
		TotalIDs:       9000,
		DefaultBalance: 10,
	}
}

// loadClusterConfig populates clusterCfg/ports from CLUSTER_CONFIG or defaults.
func loadClusterConfig() {
	clusterCfg = defaultClusterConfig()
	path := os.Getenv("CLUSTER_CONFIG")
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var parsed Config
			if err := json.Unmarshal(data, &parsed); err == nil && len(parsed.Clusters) > 0 {
				clusterCfg = parsed
			} else if err != nil {
				log.Printf("client: failed to parse CLUSTER_CONFIG %s: %v", path, err)
			}
		} else {
			log.Printf("client: failed to read CLUSTER_CONFIG %s: %v", path, err)
		}
	}
	if clusterCfg.TotalIDs == 0 {
		clusterCfg.TotalIDs = 9000
	}
	// auto-assign ranges if missing
	needAssign := false
	for _, c := range clusterCfg.Clusters {
		if c.StartID == 0 && c.EndID == 0 {
			needAssign = true
			break
		}
	}
	if needAssign && len(clusterCfg.Clusters) > 0 {
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
	// flatten nodes
	seen := make(map[string]bool)
	var flat []string
	for _, c := range clusterCfg.Clusters {
		for _, addr := range c.Nodes {
			if !seen[addr] {
				seen[addr] = true
				flat = append(flat, addr)
			}
		}
	}
	ports = flat
}

// Hyperedge captures co-accessed accounts within a transaction for resharding.
type Hyperedge struct {
	ID     string
	Nodes  []int
	Weight int
}

// redistributed maps account ID -> cluster index (and nodes) after reshard.
var redistributed = make(map[int]int)

const redistributionFile = "config/redistribution_map.json"

// loadRedistributionMap loads the persisted id->cluster mapping if present.
func loadRedistributionMap() {
	data, err := os.ReadFile(redistributionFile)
	if err != nil {
		return
	}
	var raw map[string]int
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Printf("client: failed to parse redistribution map: %v", err)
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
		log.Printf("client: loaded redistribution map for %d accounts", len(redistributed))
	}
}

// saveRedistributionMap persists the current redistribution mapping to disk.
func saveRedistributionMap() {
	raw := make(map[string]int, len(redistributed))
	for id, shard := range redistributed {
		raw[strconv.Itoa(id)] = shard
	}
	if err := os.MkdirAll(filepath.Dir(redistributionFile), 0o755); err != nil {
		log.Printf("client: unable to create config dir for redistribution map: %v", err)
		return
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		log.Printf("client: failed to marshal redistribution map: %v", err)
		return
	}
	if err := os.WriteFile(redistributionFile, data, 0o644); err != nil {
		log.Printf("client: failed to write redistribution map: %v", err)
	}
}

const maxBatchSize = 20

type segment struct {
	txns    []*Transaction
	lf      bool
	fail    []string
	recover []string
}

type job struct {
	txn *Transaction
	wg  *sync.WaitGroup
}

type Client struct {
	id             string
	tsxs           []*Transaction
	timeout        time.Duration
	leader         string
	conn           *grpc.ClientConn
	client         node.NodeClient
	lastReplyPerTS map[string]*node.Reply
	mu             sync.Mutex
	nodes          []string
	reqCh          chan job
}

type TestCase struct {
	setNo        int
	transactions []*Transaction
	liveNodes    []string
}

type benchConfig struct {
	roPct    float64
	crossPct float64
	skew     float64
	duration time.Duration
	totalOps int64
	workers  int
	amount   float64
	startID  int
	endID    int
	seed     int64
}

type perfTracker struct {
	mu        sync.Mutex
	start     time.Time
	latencies []time.Duration
	count     int64
}

func (p *perfTracker) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.start = time.Now()
	p.latencies = nil
	p.count = 0
}

func (p *perfTracker) Add(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.start.IsZero() {
		p.start = time.Now()
	}
	p.latencies = append(p.latencies, d)
	p.count++
}

func (p *perfTracker) Performance() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.count == 0 || p.start.IsZero() {
		fmt.Println("Performance: no transactions recorded")
		return
	}
	elapsed := time.Since(p.start)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}
	var sum time.Duration
	min := time.Duration(math.MaxInt64)
	max := time.Duration(0)
	for _, d := range p.latencies {
		sum += d
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	avg := time.Duration(0)
	if p.count > 0 {
		avg = time.Duration(int64(sum) / p.count)
	}
	tput := float64(p.count) / elapsed.Seconds()
	fmt.Printf("Performance: count=%d throughput=%.2f txns/sec avg-lat=%s min=%s max=%s window=%s\n",
		p.count, tput, avg, min, max, elapsed)
}

// recordTxnHistory stores a transaction for resharding analysis.
func recordTxnHistory(t Transaction) {
	if t.to == "" { // skip read-only
		return
	}
	historyMu.Lock()
	txnHistory = append(txnHistory, t)
	// cap history to last 500 entries to bound cost
	if len(txnHistory) > 500 && len(txnHistory) > 0 {
		txnHistory = txnHistory[len(txnHistory)-500:]
	}
	historyMu.Unlock()
}

// func markSuccess(ts string) {
// 	successMu.Lock()
// 	successTS[ts] = true
// 	successMu.Unlock()
// }

// func addToBacklog(txn *Transaction) {
// 	if txn == nil || txn.ts == "" {
// 		return
// 	}
// 	successMu.Lock()
// 	if successTS[txn.ts] {
// 		successMu.Unlock()
// 		return
// 	}
// 	successMu.Unlock()
// 	if backlogSeen[txn.ts] {
// 		return
// 	}
// 	backlogSeen[txn.ts] = true
// 	backlogTxns = append(backlogTxns, txn)
// }

// ensureClient returns an existing client for the given id or lazily creates one.
func ensureClient(clients map[string]*Client, id string) *Client {
	if c, ok := clients[id]; ok && c != nil {
		return c
	}
	intID := mustAtoi(id)
	cluster := clusterForAccount(intID)
	leader := findLeader(cluster)
	if leader == "" && len(cluster) > 0 {
		leader = cluster[0]
	}
	if leader == "" {
		leader = ":5001"
	}
	timeout := time.Duration(rand.Intn(5000)+1000) * time.Millisecond
	c := NewClient(id, leader, timeout)
	c.startWorker()
	clients[id] = c
	return c
}

func shardIDForAccount(id int) string {
	if idx, ok := redistributed[id]; ok {
		return fmt.Sprintf("s%d", idx+1)
	}
	for idx, c := range clusterCfg.Clusters {
		if id >= c.StartID && id <= c.EndID {
			return fmt.Sprintf("s%d", idx+1)
		}
	}
	return "unknown"
}

func clusterNodesByShardID(shard string) []string {
	if strings.HasPrefix(shard, "s") && len(shard) > 1 {
		idx, err := strconv.Atoi(shard[1:])
		if err == nil && idx >= 1 && idx <= len(clusterCfg.Clusters) {
			return append([]string(nil), clusterCfg.Clusters[idx-1].Nodes...)
		}
	}
	return nil
}

var tsMutex sync.Mutex
var lastTS int64

func nextTs() string {
	tsMutex.Lock()
	defer tsMutex.Unlock()
	now := time.Now().UnixNano()
	if now <= lastTS {
		now = lastTS + 1
	}
	lastTS = now
	return strconv.FormatInt(now, 10)
}

func init() {
	loadClusterConfig()
	loadRedistributionMap()
}

func randSkewedID(r *rand.Rand, start, end int, skew float64) int {
	if skew <= 0 {
		return r.Intn(end-start+1) + start
	}
	s := 1.0 + skew*4.0 // map 0..1 -> 1..5
	zipf := rand.NewZipf(r, s, 1, uint64(end-start))
	return start + int(zipf.Uint64())
}

func clusterForAccount(id int) []string {
	if idx, ok := redistributed[id]; ok && idx >= 0 && idx < len(clusterCfg.Clusters) {
		return append([]string(nil), clusterCfg.Clusters[idx].Nodes...)
	}
	for _, c := range clusterCfg.Clusters {
		if id >= c.StartID && id <= c.EndID {
			return append([]string(nil), c.Nodes...)
		}
	}
	return append([]string(nil), ports...)
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func NewClient(idx string, addr string, timeout time.Duration) *Client {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Error while creating client: %v", err)
	}

	client := node.NewNodeClient(conn)

	return &Client{
		id:             idx,
		tsxs:           make([]*Transaction, 0),
		timeout:        timeout,
		leader:         addr,
		conn:           conn,
		client:         client,
		lastReplyPerTS: make(map[string]*node.Reply),
		nodes:          append([]string(nil), ports...),
		reqCh:          make(chan job, 100),
	}
}

func normalizeAddr(s string) string {
	if strings.HasPrefix(s, "localhost:") {
		return s[len("localhost"):]
	}
	return s
}

func (c *Client) reconnect(newLeader string) error {
	if c.conn != nil {
		c.conn.Close()
	}

	newLeader = normalizeAddr(newLeader)
	conn, err := grpc.Dial(newLeader, grpc.WithInsecure())
	if err != nil {
		return err
	}

	c.conn = conn
	c.client = node.NewNodeClient(conn)
	c.leader = newLeader
	log.Printf("[Client %s] Reconnected to new leader at %s", c.id, newLeader)
	return nil
}

func (c *Client) startWorker() {
	go func() {
		for j := range c.reqCh {
			c.sendRequest(context.Background(), j.txn.from, j.txn.to, j.txn.amt, j.txn.ts, j.txn.readQuorum)
			if j.wg != nil {
				j.wg.Done()
			}
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		}
	}()
}

func (c *Client) enqueueRequest(txn *Transaction, wg *sync.WaitGroup) {
	c.reqCh <- job{txn: txn, wg: wg}
}

func (c *Client) sendRequest(ctx context.Context, from string, to string, amt float64, ts string, readQuorum int) {
	start := time.Now()
	// ensure we are talking to the leader of the sender's shard
	fromInt, _ := strconv.Atoi(from)
	fromCluster := clusterForAccount(fromInt)
	leaderForShard := findLeader(fromCluster)
	if leaderForShard == "" && len(fromCluster) > 0 {
		leaderForShard = fromCluster[0]
	}
	if leaderForShard != "" && leaderForShard != c.leader {
		_ = c.reconnect(leaderForShard)
	}

	req := &node.TxnMessage{
		Txn: &node.Transaction{
			From:   from,
			To:     to,
			Amount: amt,
		},
		Timestamp: ts,
		Client:    from,
	}

	toInt, _ := strconv.Atoi(to)
	toCluster := clusterForAccount(toInt)
	crossShard := !sameCluster(fromCluster, toCluster)
	// track for resharding analysis
	recordTxnHistory(Transaction{from: from, to: to, amt: amt, ts: ts})

	// read-only transaction
	if to == "" {
		c.sendRead(ctx, from, readQuorum)
		return
	}

	// Cross-shard: run 2PC prepare on participant (receiver shard) and coordinator (sender shard)
	if crossShard {
		coordLeader := findLeader(fromCluster)
		if coordLeader == "" {
			coordLeader = fromCluster[0]
		}
		if coordLeader != "" && coordLeader != c.leader {
			_ = c.reconnect(coordLeader)
		}
		if c.client == nil {
			log.Printf("[Client %s] no client to send Request2PC", from)
			return
		}

		maxAttempts := 5
		if benchModeGlobal {
			maxAttempts = 3
		}
		attempt := 1
		for attempt <= maxAttempts {
			// ensure we have a live leader; wait briefly if needed
			if coordLeader == "" {
				coordLeader = waitForShardLeader(fromCluster, 2*time.Second)
				if coordLeader == "" && len(fromCluster) > 0 {
					coordLeader = fromCluster[0]
				}
				if coordLeader != "" && coordLeader != c.leader {
					_ = c.reconnect(coordLeader)
				}
			}
			reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
			log.Printf("[Client %s] Sending Request2PC I,=%s: %s -> %s (%.2f) [Leader=%s]",
				from, ts, from, to, amt, c.leader)
			reply, err := c.client.Request2PC(reqCtx, req)
			cancel()
			if err == nil {
				c.mu.Lock()
				c.lastReplyPerTS[ts] = reply
				c.mu.Unlock()
				if reply != nil && reply.Reply {
					recordTxnHistory(Transaction{from: from, to: to, amt: amt, ts: ts})
				}
				// if reply.Reply {
				// 	markSuccess(ts)
				// }
				perf.Add(time.Since(start))
				log.Printf("[Client %s] Reply2PC I,=%s: success=%v", from, ts, reply.Reply)
				// if !reply.Reply {
				// 	addToBacklog(&Transaction{from: from, to: to, amt: amt, ts: ts})
				// }
				return
			}
			if strings.HasPrefix(err.Error(), "rpc error: code = Unknown desc = redirect:") {
				newLeader := strings.TrimSpace(strings.TrimPrefix(err.Error(), "rpc error: code = Unknown desc = redirect:"))
				newLeader = normalizeAddr(newLeader)
				_ = c.reconnect(newLeader)
				attempt++
				continue
			}
			// if leader unreachable, try broadcasting to shard peers (similar to intra-shard)
			log.Printf("[Client %s] Request2PC error: %v; broadcasting to shard peers", from, err)
			replyCh := make(chan *node.Reply, 1)
			broadcastCtx, cancelBroadcast := context.WithCancel(context.Background())

			for _, addr := range fromCluster {
				if addr == coordLeader {
					continue
				}
				go func(nodeAddr string) {
					conn, derr := grpc.Dial(nodeAddr, grpc.WithInsecure())
					if derr != nil {
						log.Printf("[Client %s] Failed to connect to %s: %v", from, nodeAddr, derr)
						return
					}
					defer conn.Close()

					cli := node.NewNodeClient(conn)
					reqCtx2, cancel2 := context.WithTimeout(broadcastCtx, c.timeout)
					defer cancel2()
					r, derr := cli.Request2PC(reqCtx2, req)
					if derr == nil && r != nil {
						select {
						case replyCh <- r:
						default:
						}
					} else if derr != nil {
						log.Printf("[Client %s] Request2PC to %s failed: %v", from, nodeAddr, derr)
					}
				}(addr)
			}

			select {
			case r := <-replyCh:
				cancelBroadcast()
				c.mu.Lock()
				c.lastReplyPerTS[ts] = r
				c.leader = normalizeAddr(r.Node)
				c.mu.Unlock()
				if r != nil && r.Reply {
					recordTxnHistory(Transaction{from: from, to: to, amt: amt, ts: ts})
				}
				// if r.Reply {
				// 	markSuccess(ts)
				// }
				perf.Add(time.Since(start))
				log.Printf("[Client %s] Reply2PC via broadcast ts=%s: success=%v, node=%s", from, ts, r.Reply, r.Node)
				// if !r.Reply {
				// 	addToBacklog(&Transaction{from: from, to: to, amt: amt, ts: ts})
				// }
				return
			case <-time.After(5 * time.Second):
				cancelBroadcast()
				log.Printf("[Client %s] Broadcast timed out for 2PC ts=%s, retrying...", from, ts)
				c.waitAndReconnect(fromCluster)
			}
			attempt++
			time.Sleep(300 * time.Millisecond)
		}
		if benchModeGlobal {
			log.Printf("[Client %s] Request2PC failed after retries; skipping in bench mode", from)
			return
		}
		log.Printf("[Client %s] Request2PC failed after retries", from)
		// enqueue for retry after cluster stabilizes
		backlogTxns = append(backlogTxns, &Transaction{from: from, to: to, amt: amt, ts: ts})
		return
	}

	maxAttempts := 3
	attempt := 1
	for attempt <= maxAttempts {
		reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
		log.Printf("[Client %s] Sending request τ=%s: %s → %s (%.2f) [Leader=%s]",
			from, ts, from, to, amt, c.leader)

		reply, err := c.client.Request(reqCtx, req)
		cancel()

		if err == nil {
			c.mu.Lock()
			c.lastReplyPerTS[ts] = reply
			c.mu.Unlock()
			if reply != nil && reply.Reply {
				recordTxnHistory(Transaction{from: from, to: to, amt: amt, ts: ts})
			}
			// if reply.Reply {
			// 	markSuccess(ts)
			// }
			perf.Add(time.Since(start))
			log.Printf("[Client %s] Reply τ=%s: success=%v", from, ts, reply.Reply)
			// if !reply.Reply {
			// 	addToBacklog(&Transaction{from: from, to: to, amt: amt, ts: ts})
			// }
			return
		}

		if strings.HasPrefix(err.Error(), "rpc error: code = Unknown desc = redirect:") {
			newLeader := strings.TrimSpace(strings.TrimPrefix(err.Error(), "rpc error: code = Unknown desc = redirect:"))
			newLeader = normalizeAddr(newLeader)
			log.Printf("[Client %s]  Redirected to new leader %s", from, newLeader)
			if err := c.reconnect(newLeader); err != nil {
				log.Printf("[Client %s] Reconnect failed: %v", from, err)
				time.Sleep(time.Second)
			}
			attempt++
			continue
		}

		log.Printf("[Client %s]s Timeout or error, broadcasting to shard nodes", from)
		replyCh := make(chan *node.Reply, 1)
		broadcastCtx, cancelBroadcast := context.WithCancel(context.Background())

		targets := clusterForAccount(fromInt)
		for _, addr := range targets {
			go func(nodeAddr string) {
				conn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
				if err != nil {
					log.Printf("[Client %s] Failed to connect to %s: %v", from, nodeAddr, err)
					return
				}
				defer conn.Close()

				client := node.NewNodeClient(conn)
				ctx2, cancel2 := context.WithTimeout(broadcastCtx, 3*time.Second)
				defer cancel2()
				r, err := client.Request(ctx2, req)
				if err == nil {
					select {
					case replyCh <- r:
					default:
					}
				} else {
					log.Printf("[Client %s] Request to %s failed: %v", from, nodeAddr, err)
				}
			}(addr)
		}

		select {
		case r := <-replyCh:
			cancelBroadcast()
			c.mu.Lock()
			c.lastReplyPerTS[ts] = r
			c.leader = normalizeAddr(r.Node)
			c.mu.Unlock()
			// if r.Reply {
			// 	markSuccess(ts)
			// }
			perf.Add(time.Since(start))
			log.Printf("[Client %s] Reply τ=%s via broadcast: success=%v, node=%s", from, ts, r.Reply, r.Node)
			// if !r.Reply {
			// 	addToBacklog(&Transaction{from: from, to: to, amt: amt, ts: ts})
			// }
			return
		case <-time.After(5 * time.Second):
			cancelBroadcast()
			log.Printf("[Client %s] Broadcast timed out for τ=%s, retrying...", from, ts)
			c.waitAndReconnect(fromCluster)
		}

		attempt++
		time.Sleep(c.timeout)
	}
	if benchModeGlobal {
		log.Printf("[Client %s] Request failed after retries; skipping in bench mode", from)
		return
	}
	// enqueue failed intra-shard txn for later retry
	backlogTxns = append(backlogTxns, &Transaction{from: from, to: to, amt: amt, ts: ts})

}

func splitByLF(txns []*Transaction) []segment {
	segs := []segment{{}}
	for _, t := range txns {
		switch {
		case t == nil:
			continue
		case t.op == "FAIL":
			segs[len(segs)-1].fail = append(segs[len(segs)-1].fail, t.target)
			segs = append(segs, segment{})
		case t.op == "RECOVER":
			segs[len(segs)-1].recover = append(segs[len(segs)-1].recover, t.target)
			segs = append(segs, segment{})
		case strings.EqualFold(t.from, "LF"):
			segs[len(segs)-1].lf = true
			segs = append(segs, segment{})
		default:
			segs[len(segs)-1].txns = append(segs[len(segs)-1].txns, t)
		}
	}
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		if len(last.txns) == 0 && !last.lf && len(last.fail) == 0 && len(last.recover) == 0 {
			segs = segs[:len(segs)-1]
		}
	}
	return segs
}

func pushSetToBacklog(segs []segment) {
	for _, s := range segs {
		backlog = append(backlog, BacklogItem{seg: s})
	}
}

func processSegments(clients map[string]*Client, segs []segment, live []string) {
	liveSet := make(map[string]bool, len(live))
	for _, n := range live {
		liveSet[n] = true
	}

	for _, s := range segs {
		for i := 0; i < len(s.txns); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(s.txns) {
				end = len(s.txns)
			}
			chunk := s.txns[i:end]

			var wg sync.WaitGroup
			for _, t := range chunk {
				if t != nil && t.op == "" && t.from != "LF" {
					if !txnHasQuorum(liveSet, t) {
						log.Printf("[process] insufficient quorum for txn %+v, pushing to backlog", t)
						backlogTxns = append(backlogTxns, t)
						continue
					}
				}
				c := ensureClient(clients, t.from)
				if c == nil {
					log.Printf("[process] no client available for txn %+v, skipping", t)
					continue
				}
				wg.Add(1)
				c.enqueueRequest(t, &wg)
			}
			wg.Wait()
		}

		if len(s.fail) > 0 {
			time.Sleep(300 * time.Millisecond)
		}
		for _, target := range s.fail {
			log.Printf("[process] Failing node %s", target)
			if err := setNodeStateRPC(target, false); err != nil {
				log.Printf("[process] Failed to disable %s: %v", target, err)
			}
			liveSet[target] = false
			processBacklogTxns(clients, liveSlice(liveSet))
			time.Sleep(750 * time.Millisecond) // allow leader election
		}
		if len(s.recover) > 0 {
			time.Sleep(300 * time.Millisecond)
		}
		for _, target := range s.recover {
			log.Printf("[process] Recovering node %s", target)
			if err := setNodeStateRPC(target, true); err != nil {
				log.Printf("[process] Failed to enable %s: %v", target, err)
			}
			liveSet[target] = true
			processBacklogTxns(clients, liveSlice(liveSet))
			time.Sleep(500 * time.Millisecond) // allow leader election
		}

		if s.lf {
			anyClient := clients["1"]
			leader := anyClient.currentLeaderAddr()
			log.Printf("Simulating leader failure at %s (LF)", leader)
			if err := failNode(leader); err != nil {
				log.Printf("Failed to disable leader %s: %v", leader, err)
			}
			liveSet[leader] = false
			processBacklogTxns(clients, liveSlice(liveSet))
			time.Sleep(750 * time.Millisecond)
		}
	}
	// final pass for any backlog txns with current live set
	processBacklogTxns(clients, liveSlice(liveSet))
}

// computeReshard computes a partition of accounts based on recent txnHistory.
// It uses a greedy load balancer minimizing cross-shard edges (hyperedge cost ~1 per txn).
// The number of shards matches the configured clusters.
func computeReshard() [][]int {
	historyMu.Lock()
	hist := append([]Transaction(nil), txnHistory...)
	historyMu.Unlock()

	_ = buildHypergraph(hist) // build hypergraph for future partitioning/inspection

	// accumulate frequency per account and co-access pairs to favor co-location
	type nodeScore struct {
		id   int
		freq int
	}
	freq := map[int]int{}
	co := map[[2]int]int{}
	for _, t := range hist {
		f, _ := strconv.Atoi(t.from)
		to, _ := strconv.Atoi(t.to)
		freq[f]++
		freq[to]++
		key := [2]int{minInt(f, to), maxInt(f, to)}
		co[key]++
	}
	// sort accounts by freq desc
	accounts := make([]nodeScore, 0, len(freq))
	for id, c := range freq {
		accounts = append(accounts, nodeScore{id: id, freq: c})
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].freq > accounts[j].freq })

	k := len(clusterCfg.Clusters)
	if k == 0 {
		k = 3
	}
	shards := make([][]int, k)
	shardLoad := make([]int, k)

	// helper to compute incremental cost of placing id into shard s
	cost := func(id int, shardIdx int) int {
		c := 0
		for other, _ := range freq {
			if other == id {
				continue
			}
			key := [2]int{minInt(id, other), maxInt(id, other)}
			weight := co[key]
			if weight == 0 {
				continue
			}
			// find shard of other (if already placed)
			placed := -1
			for si := 0; si < k; si++ {
				for _, v := range shards[si] {
					if v == other {
						placed = si
						break
					}
				}
				if placed != -1 {
					break
				}
			}
			if placed != -1 && placed != shardIdx {
				c += weight // crossing cost
			}
		}
		return c
	}

	// greedy placement: iterate accounts by freq, place where added cost+load balance is minimal
	for _, a := range accounts {
		bestShard := 0
		bestScore := int(^uint(0) >> 1) // max int
		for s := 0; s < k; s++ {
			score := cost(a.id, s) + shardLoad[s]
			if score < bestScore {
				bestScore = score
				bestShard = s
			}
		}
		shards[bestShard] = append(shards[bestShard], a.id)
		shardLoad[bestShard] += a.freq
	}

	// update redistributed mapping to reflect new shard assignment
	newMap := make(map[int]int)
	for shardIdx, ids := range shards {
		for _, id := range ids {
			newMap[id] = shardIdx
		}
	}
	redistributed = newMap
	saveRedistributionMap()
	log.Printf("[reshard] redistributed %d accounts across %d shards", len(redistributed), len(shards))

	return shards
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// buildHypergraph constructs hyperedges from the transaction history (one edge per txn).
// Each edge connects the participating account IDs.
func buildHypergraph(hist []Transaction) []Hyperedge {
	edges := make([]Hyperedge, 0, len(hist))
	for i, t := range hist {
		if t.to == "" {
			continue // skip read-only
		}
		fromID, err1 := strconv.Atoi(t.from)
		toID, err2 := strconv.Atoi(t.to)
		if err1 != nil || err2 != nil {
			continue
		}
		nodes := []int{fromID, toID}
		edges = append(edges, Hyperedge{
			ID:     fmt.Sprintf("txn-%d-%s", i, t.ts),
			Nodes:  nodes,
			Weight: 1,
		})
	}
	if len(edges) > 0 {
		log.Printf("[reshard] built hypergraph with %d edges from history", len(edges))
	} else {
		log.Printf("[reshard] hypergraph empty (no txns recorded)")
	}
	return edges
}

// pickClient returns a client keyed by the given id (usually the sender account string).
// If not present, it falls back to client "1" to avoid nil dereference.
func pickClient(clients map[string]*Client, id string) *Client {
	if c, ok := clients[id]; ok && c != nil {
		return c
	}
	return ensureClient(clients, id)
}

func drainBacklog(clients map[string]*Client) {
	if len(backlog) == 0 {
		return
	}

	var segs []segment
	for _, it := range backlog {
		segs = append(segs, it.seg)
	}
	log.Printf("Draining backlog: %d segments", len(segs))
	processSegments(clients, segs, ports)
	backlog = backlog[:0]
}

func backlogSegments() []segment {
	var segs []segment
	for _, it := range backlog {
		segs = append(segs, it.seg)
	}
	return segs
}

func liveSlice(liveSet map[string]bool) []string {
	out := make([]string, 0, len(liveSet))
	for n, ok := range liveSet {
		if ok {
			out = append(out, n)
		}
	}
	return out
}

// waitForShardLeader polls until a leader is found in the cluster or timeout elapses.
func waitForShardLeader(cluster []string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := findLeader(cluster); l != "" {
			return l
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

// waitAndReconnect waits for a leader in the shard and reconnects this client to it.
func (c *Client) waitAndReconnect(cluster []string) {
	l := waitForShardLeader(cluster, 2*time.Second)
	if l == "" && len(cluster) > 0 {
		l = cluster[0]
	}
	if l != "" && l != c.leader {
		_ = c.reconnect(l)
	}
}

// txnHasQuorum checks quorum for shards touched by txn using liveSet map.
func txnHasQuorum(liveSet map[string]bool, t *Transaction) bool {
	if t == nil {
		return false
	}
	needed := make(map[string]bool)
	fromID := mustAtoi(t.from)
	needed[shardIDForAccount(fromID)] = true
	if t.to != "" {
		toID := mustAtoi(t.to)
		needed[shardIDForAccount(toID)] = true
	}
	for shard := range needed {
		members := clusterNodesByShardID(shard)
		if len(members) == 0 {
			continue
		}
		live := 0
		for _, m := range members {
			if liveSet[m] {
				live++
			}
		}
		if live < (len(members)/2)+1 {
			return false
		}
	}
	return true
}

// processBacklogTxns re-evaluates backlogTxns with current live nodes, executes if quorum, else skips.
func processBacklogTxns(clients map[string]*Client, live []string) {
	if len(backlogTxns) == 0 {
		return
	}
	liveSet := make(map[string]bool, len(live))
	for _, n := range live {
		liveSet[n] = true
	}

	// retry for a few rounds so eligible transactions keep getting attempted until success
	for round := 0; round < 3 && len(backlogTxns) > 0; round++ {
		var wg sync.WaitGroup
		remaining := make([]*Transaction, 0, len(backlogTxns))
		for _, t := range backlogTxns {
			if t == nil {
				continue
			}
			if !txnHasQuorum(liveSet, t) {
				log.Printf("[backlog] skipping txn %+v due to insufficient quorum", t)
				remaining = append(remaining, t)
				continue
			}
			c := ensureClient(clients, t.from)
			if c == nil {
				log.Printf("[backlog] no client for txn %+v, skipping", t)
				remaining = append(remaining, t)
				continue
			}
			c.mu.Lock()
			seen := c.lastReplyPerTS[t.ts]
			c.mu.Unlock()
			if seen != nil && seen.Reply {
				// already succeeded
				continue
			}
			wg.Add(1)
			c.enqueueRequest(t, &wg)
			// keep in backlog to retry next round until success observed
			remaining = append(remaining, t)
		}
		wg.Wait()
		backlogTxns = remaining
	}
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func runBenchmark(cfg benchConfig) {
	log.Printf("[bench] starting benchmark: roPct=%.2f crossPct=%.2f skew=%.2f duration=%s count=%d workers=%d amount=%.2f seed=%d",
		cfg.roPct, cfg.crossPct, cfg.skew, cfg.duration, cfg.totalOps, cfg.workers, cfg.amount, cfg.seed)
	var deadline time.Time
	if cfg.totalOps <= 0 {
		deadline = time.Now().Add(cfg.duration)
	}

	// create workers
	clients := make([]*Client, cfg.workers)
	for i := 0; i < cfg.workers; i++ {
		id := fmt.Sprintf("bench-%d", i+1)
		clients[i] = NewClient(id, ":5001", 3*time.Second)
	}

	var ops, roOps, rwOps int64

	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		c := clients[i]
		wg.Add(1)
		go func(idx int, cli *Client) {
			defer wg.Done()
			localRand := rand.New(rand.NewSource(cfg.seed + int64(idx)))
			for {
				if cfg.totalOps > 0 {
					cur := atomic.AddInt64(&ops, 1)
					if cur > cfg.totalOps {
						break
					}
				} else if !deadline.IsZero() && time.Now().After(deadline) {
					break
				}

				ts := nextTs()
				if localRand.Float64() < cfg.roPct {
					// read-only
					id := pickAccount(localRand, cfg.skew)
					cli.sendRead(context.Background(), strconv.Itoa(id), 0)
					atomic.AddInt64(&roOps, 1)
				} else {
					from, to := pickTxnAccounts(localRand, cfg.crossPct, cfg.skew)
					cli.sendRequest(context.Background(), strconv.Itoa(from), strconv.Itoa(to), cfg.amount, ts, 0)
					atomic.AddInt64(&rwOps, 1)
				}
				if cfg.totalOps <= 0 {
					atomic.AddInt64(&ops, 1)
				}
			}
		}(i, c)
	}

	wg.Wait()
	log.Printf("[bench] completed: total=%d rw=%d ro=%d (duration=%s)", ops, rwOps, roOps, cfg.duration)
	for _, c := range clients {
		if c != nil && c.conn != nil {
			c.conn.Close()
		}
	}
}

func pickAccount(r *rand.Rand, skew float64) int {
	shard := r.Intn(3)
	switch shard {
	case 0:
		return randSkewedID(r, 1, 3000, skew)
	case 1:
		return randSkewedID(r, 3001, 6000, skew)
	default:
		return randSkewedID(r, 6001, 9000, skew)
	}
}

func pickTxnAccounts(r *rand.Rand, crossPct, skew float64) (int, int) {
	cross := r.Float64() < crossPct
	if cross {
		// pick different shards
		fromShard := r.Intn(3)
		toShard := (fromShard + 1 + r.Intn(2)) % 3
		var from, to int
		switch fromShard {
		case 0:
			from = randSkewedID(r, 1, 3000, skew)
		case 1:
			from = randSkewedID(r, 3001, 6000, skew)
		default:
			from = randSkewedID(r, 6001, 9000, skew)
		}
		switch toShard {
		case 0:
			to = randSkewedID(r, 1, 3000, skew)
		case 1:
			to = randSkewedID(r, 3001, 6000, skew)
		default:
			to = randSkewedID(r, 6001, 9000, skew)
		}
		return from, to
	}

	// intra-shard
	shard := r.Intn(3)
	switch shard {
	case 0:
		return randSkewedID(r, 1, 3000, skew), randSkewedID(r, 1, 3000, skew)
	case 1:
		return randSkewedID(r, 3001, 6000, skew), randSkewedID(r, 3001, 6000, skew)
	default:
		return randSkewedID(r, 6001, 9000, skew), randSkewedID(r, 6001, 9000, skew)
	}
}

func applyLiveNodes(live []string) {
	liveSet := make(map[string]bool, len(live))
	for _, x := range live {
		liveSet[x] = true
	}

	log.Printf("Live nodes for the test set :: %+v\n", liveSet)
	for _, addr := range ports {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("[Client] cannot connect to %s to set state: %v", addr, err)
			continue
		}
		client := node.NewNodeClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.SetNodeState(ctx, &node.NodeStateRequest{Enabled: liveSet[addr]})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[Client] SetNodeState(%s -> %v) failed: %v", addr, liveSet[addr], err)
		} else {
			log.Printf("[Client] %s => %s", addr, map[bool]string{true: "ENABLED", false: "DISABLED"}[liveSet[addr]])
		}
	}

	time.Sleep(300 * time.Millisecond)
}

func resetCluster(nodes []string, val int64) {
	var wg sync.WaitGroup
	for _, addr := range nodes {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, err := grpc.Dial(a, grpc.WithInsecure())
			if err != nil {
				log.Printf("[resetCluster] dial %s failed: %v", a, err)
				return
			}
			defer conn.Close()
			cli := node.NewNodeClient(conn)
			resp, err := cli.ResetDB(ctx, &node.ResetDBReq{Value: val})
			if err != nil || resp == nil || !resp.Ok {
				log.Printf("[resetCluster] ResetDB(%s) failed: err=%v resp=%+v", a, err, resp)
			}
		}(addr)
	}
	wg.Wait()
}

// assignmentFromRedistribution builds a shard->ids slice using the current redistributed map.
func assignmentFromRedistribution() [][]int {
	k := len(clusterCfg.Clusters)
	if k == 0 {
		return nil
	}
	shards := make([][]int, k)
	for id, idx := range redistributed {
		if idx >= 0 && idx < k {
			shards[idx] = append(shards[idx], id)
		}
	}
	return shards
}

func letterToIntStr(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" || utf8.RuneCountInString(s) != 1 {
		return 0, fmt.Errorf("expected exactly one letter, got %q", s)
	}
	r, _ := utf8.DecodeRuneInString(s)
	r = unicode.ToUpper(r)
	if r < 'A' || r > 'Z' {
		return 0, fmt.Errorf("not A..Z: %q", r)
	}
	return int(r-'A') + 1, nil
}

func parseNodeAddressToken(raw string) (string, error) {
	tok := strings.TrimSpace(raw)
	tok = strings.Trim(tok, "() ")
	tok = strings.TrimPrefix(strings.ToLower(tok), "n")

	if strings.HasPrefix(tok, ":") {
		return tok, nil
	}
	if strings.HasPrefix(tok, "500") && len(tok) == 4 {
		return ":" + tok, nil
	}

	num, err := strconv.Atoi(tok)
	if err != nil {
		return "", fmt.Errorf("invalid node token %q", raw)
	}
	if num >= 1 && num <= 9 {
		return fmt.Sprintf(":500%d", num), nil
	}
	if num >= 5000 && num <= 5999 {
		return fmt.Sprintf(":%d", num), nil
	}
	return "", fmt.Errorf("invalid node index %q", raw)
}

// parseAccountID parses either a single letter A..Z or a numeric account string.
func parseAccountID(token string) (int, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, fmt.Errorf("empty account token")
	}
	// all digits -> numeric id
	digitOnly := true
	for _, r := range token {
		if !unicode.IsDigit(r) {
			digitOnly = false
			break
		}
	}
	if digitOnly {
		return strconv.Atoi(token)
	}
	return letterToIntStr(token)
}

func parseCSV(filename string) ([]*TestCase, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %v", err)
	}

	testCases := []*TestCase{}
	var current *TestCase

	for _, row := range rows[1:] {
		setStr := strings.TrimSpace(row[0])
		txnStr := strings.TrimSpace(row[1])
		liveStr := strings.TrimSpace(row[2])

		if setStr != "" {
			setNo, _ := strconv.Atoi(setStr)
			if current != nil {
				testCases = append(testCases, current)
			}
			current = &TestCase{
				setNo:        setNo,
				transactions: []*Transaction{},
				liveNodes:    []string{},
			}
		}

		if txnStr != "" {
			trimmed := strings.TrimSpace(txnStr)
			upper := strings.ToUpper(trimmed)
			switch {
			case strings.EqualFold(trimmed, "LF"):
				current.transactions = append(current.transactions, &Transaction{from: "LF"})
			case strings.HasPrefix(upper, "F(") && strings.HasSuffix(upper, ")"):
				inner := strings.TrimSpace(trimmed[2 : len(trimmed)-1])
				target, err := parseNodeAddressToken(inner)
				if err != nil {
					log.Printf("[parseCSV] invalid fail target %q: %v", trimmed, err)
					break
				}
				current.transactions = append(current.transactions, &Transaction{op: "FAIL", target: target})
			case strings.HasPrefix(upper, "R(") && strings.HasSuffix(upper, ")"):
				inner := strings.TrimSpace(trimmed[2 : len(trimmed)-1])
				target, err := parseNodeAddressToken(inner)
				if err != nil {
					log.Printf("[parseCSV] invalid recover target %q: %v", trimmed, err)
					break
				}
				current.transactions = append(current.transactions, &Transaction{op: "RECOVER", target: target})
			default:
				txnStrClean := strings.Trim(trimmed, "() ")
				parts := strings.Split(txnStrClean, ",")
				if len(parts) == 3 {
					from, _ := parseAccountID(strings.TrimSpace(parts[0]))
					to, _ := parseAccountID(strings.TrimSpace(parts[1]))
					amt, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
					ts := nextTs()
					current.transactions = append(current.transactions, &Transaction{
						from: strconv.Itoa(from), to: strconv.Itoa(to), amt: amt, ts: ts,
					})
				} else if len(parts) == 1 && strings.TrimSpace(parts[0]) != "" {
					id, _ := parseAccountID(strings.TrimSpace(parts[0]))
					ts := nextTs()
					current.transactions = append(current.transactions, &Transaction{from: strconv.Itoa(id), readOnly: true, ts: ts})
				} else if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" {
					id, _ := parseAccountID(strings.TrimSpace(parts[0]))
					qStr := strings.TrimSpace(parts[1])
					q := 0
					if strings.EqualFold(qStr, "quorum") {
						q = -1 // sentinel for majority quorum
					} else {
						q, _ = strconv.Atoi(qStr)
					}
					ts := nextTs()
					current.transactions = append(current.transactions, &Transaction{
						from:       strconv.Itoa(id),
						readOnly:   true,
						readQuorum: q,
						ts:         ts,
					})
				}
			}
		}

		if liveStr != "" {
			liveStr = strings.Trim(liveStr, "[] ")
			nodes := strings.Split(liveStr, ",")
			for i := range nodes {
				nodes[i] = strings.TrimSpace(nodes[i])
				if !strings.HasPrefix(nodes[i], ":") && strings.HasPrefix(nodes[i], "n") {
					num, _ := strconv.Atoi(strings.TrimPrefix(nodes[i], "n"))
					nodes[i] = fmt.Sprintf(":%d", 5000+num)
				}
			}
			current.liveNodes = nodes
		}
	}

	if current != nil {
		testCases = append(testCases, current)
	}

	return testCases, nil
}

func queryStatus(addr string) (string, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	c := node.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	resp, err := c.PrintStatus(ctx, &node.PrintStatusRequest{})
	if err != nil {
		return "", err
	}
	if len(resp.Statuses) == 0 {
		return "", fmt.Errorf("no status from %s", addr)
	}
	return resp.Statuses[0].Status, nil
}

func findLeader(cluster []string) string {
	for _, addr := range cluster {
		if status, err := queryStatus(addr); err == nil && status == "LEADER" {
			log.Println("Leader found from cluster", cluster, "is", addr)
			return addr
		}
	}
	return ""
}

func sameCluster(a, b []string) bool {
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

func (c *Client) sendRead(ctx context.Context, id string, quorum int) {
	start := time.Now()
	acctID := mustAtoi(id)
	cluster := dedupeStrings(clusterForAccount(acctID))
	if len(cluster) == 0 {
		log.Printf("[Client %s] read failed: no cluster for id %s", c.id, id)
		return
	}

	effectiveQuorum := quorum
	if effectiveQuorum < 0 {
		effectiveQuorum = (len(cluster) / 2) + 1
	}

	// Default path: read from leader when quorum not requested.
	if effectiveQuorum <= 0 {
		leader := findLeader(cluster)
		if leader == "" && len(cluster) > 0 {
			leader = cluster[0]
		}
		if leader == "" {
			log.Printf("[Client %s] read failed: no leader for id %s", c.id, id)
			return
		}
		if leader != "" && leader != c.leader {
			_ = c.reconnect(leader)
		}
		conn, err := grpc.Dial(leader, grpc.WithInsecure())
		if err != nil {
			log.Printf("[Client %s] read connect failed: %v", c.id, err)
			return
		}
		defer conn.Close()
		cli := node.NewNodeClient(conn)
		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		resp, err := cli.GetBalance(reqCtx, &node.GetBalanceReq{Id: int64(acctID)})
		if err != nil || resp == nil || !resp.Ok {
			log.Printf("[Client %s] read failed for %s: %v resp=%+v", c.id, id, err, resp)
			return
		}
		perf.Add(time.Since(start))
		log.Printf("[Client %s] Read balance id=%s bal=%.2f (node %s)", c.id, id, resp.Balance, resp.Node)
		return
	}

	if effectiveQuorum > len(cluster) {
		effectiveQuorum = len(cluster)
	}

	type readResp struct {
		bal  int64
		node string
		err  error
	}
	respCh := make(chan readResp, len(cluster))
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, addr := range cluster {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				respCh <- readResp{node: addr, err: err}
				return
			}
			defer conn.Close()
			cli := node.NewNodeClient(conn)
			resp, err := cli.GetBalance(reqCtx, &node.GetBalanceReq{Id: int64(acctID)})
			if err != nil || resp == nil || !resp.Ok {
				if err == nil && resp != nil && !resp.Ok {
					err = fmt.Errorf(resp.Msg)
				}
				respCh <- readResp{node: addr, err: err}
				return
			}
			respCh <- readResp{bal: int64(math.Round(resp.Balance)), node: resp.Node}
		}(addr)
	}
	go func() {
		wg.Wait()
		close(respCh)
	}()

	counts := make(map[int64][]string)
	bestBal := int64(0)
	bestCount := 0
	for {
		select {
		case <-reqCtx.Done():
			if bestCount > 0 {
				log.Printf("[Client %s] quorum read timeout id=%s (best %d/%d for bal=%.0f)", c.id, id, bestCount, quorum, float64(bestBal))
			} else {
				log.Printf("[Client %s] quorum read timeout id=%s (q=%d)", c.id, id, quorum)
			}
			return
		case r, ok := <-respCh:
			if !ok {
				log.Printf("[Client %s] quorum read not reached for id=%s (best %d/%d)", c.id, id, bestCount, quorum)
				return
			}
			if r.err != nil {
				if !benchModeGlobal {
					log.Printf("[Client %s] read from %s failed: %v", c.id, r.node, r.err)
				}
				continue
			}
			counts[r.bal] = append(counts[r.bal], r.node)
			if len(counts[r.bal]) > bestCount {
				bestBal = r.bal
				bestCount = len(counts[r.bal])
			}
			if len(counts[r.bal]) >= quorum {
				cancel()
				perf.Add(time.Since(start))
				log.Printf("[Client %s] Read quorum id=%s bal=%.0f via %v (q=%d)", c.id, id, float64(r.bal), counts[r.bal], quorum)
				return
			}
		}
	}
}

func mustAtoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

// hasShardQuorum checks that each shard touched by the segments has majority live nodes.
func hasShardQuorum(live []string, segs []segment) bool {
	liveSet := make(map[string]bool, len(live))
	for _, n := range live {
		liveSet[n] = true
	}
	// collect shards involved
	needed := make(map[string]bool)
	for _, seg := range segs {
		for _, t := range seg.txns {
			if t == nil {
				continue
			}
			if t.from == "LF" {
				continue
			}
			if t.readOnly {
				id := mustAtoi(t.from)
				needed[shardIDForAccount(id)] = true
				continue
			}
			fromID := mustAtoi(t.from)
			toID := mustAtoi(t.to)
			needed[shardIDForAccount(fromID)] = true
			needed[shardIDForAccount(toID)] = true
		}
	}
	for shard := range needed {
		members := clusterNodesByShardID(shard)
		if len(members) == 0 {
			continue
		}
		live := 0
		for _, m := range members {
			if liveSet[m] {
				live++
			}
		}
		if live < (len(members)/2)+1 {
			return false
		}
	}
	return true
}

func (c *Client) currentLeaderAddr() string {
	for _, addr := range c.nodes {
		status, err := queryStatus(addr)
		if err == nil && status == "LEADER" {
			return addr
		}
	}
	return c.leader
}

func failNode(addr string) error {
	return setNodeStateRPC(addr, false)
}

func setNodeStateRPC(addr string, enabled bool) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := node.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = cli.SetNodeState(ctx, &node.NodeStateRequest{Enabled: enabled})
	return err
}

func (c *Client) SetNodeState(port string, enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		conn, err := grpc.Dial(port, grpc.WithInsecure())
		if err != nil {
			log.Printf("[Client %s] Cannot connect to leader to set state: %v", c.id, err)
			return
		}
		c.conn = conn
		c.client = node.NewNodeClient(conn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.client.SetNodeState(ctx, &node.NodeStateRequest{Enabled: enabled})
	if err != nil {
		log.Printf("[Client %s] Failed to set node state: %v", c.id, err)
	}
}

func prettyHeader(title string) {
	fmt.Println("\n==============================")
	fmt.Println(title)
	fmt.Println("==============================")
}

func printViews(addr string) {
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	defer conn.Close()
	c := node.NewNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.PrintView(ctx, &node.PrintViewRequest{})
	if err != nil {
		fmt.Printf("PrintView(%s) error: %v\n", addr, err)
		return
	}

	prettyHeader(fmt.Sprintf("New-view messages @ %s", addr))
	if len(resp.NewViews) == 0 {
		if resp.GetMessage() != "" {
			fmt.Println(resp.GetMessage())
		} else {
			fmt.Println("(no new-view messages recorded)")
		}
		return
	}

	for i, v := range resp.NewViews {
		fmt.Printf("[%d] sender=%s ballot=%d entries=%d ts=%s\n",
			i+1, v.Sender, v.Ballot, v.EntryCount, v.Timestamp)
	}
}

func printLogs(addr string) {
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	defer conn.Close()
	c := node.NewNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.GetLogs(ctx, &node.GetLogsReq{})
	if err != nil {
		fmt.Printf("GetLogs(%s) error: %v\n", addr, err)
		return
	}

	prettyHeader(fmt.Sprintf("Logs @ %s", addr))
	for _, r := range resp.Rows {
		fmt.Printf("(%s %d %d (%s %s %.6f)) ts=%s node=%s\n",
			r.Status, r.Ballot, r.Seq, r.From, r.To, r.Amount, r.Timestamp, r.Node)
	}
}

func printTxnStatus(addr string, seq int64) {
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	defer conn.Close()
	c := node.NewNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.GetTxnStatus(ctx, &node.GetTxnStatusReq{Seq: seq})
	if err != nil {
		fmt.Printf("GetTxnStatus(%s, seq=%d) error: %v\n", addr, seq, err)
		return
	}
	prettyHeader(fmt.Sprintf("Txn status @ %s", addr))
	fmt.Printf("status=%s  seq=%d  ballot=%d\n", resp.Status, resp.Seq, resp.Ballot)
}

func printDB(addr string) {
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	defer conn.Close()
	c := node.NewNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.GetDB(ctx, &node.GetDBReq{})
	if err != nil {
		fmt.Printf("GetDB(%s) error: %v\n", addr, err)
		return
	}
	prettyHeader(fmt.Sprintf("DB @ %s", addr))
	for k, v := range resp.Balances {
		fmt.Printf("%s => %.2f\n", k, v)
	}
}

func printAllDB(nodes []string) {
	for _, addr := range nodes {
		printDB(addr)
	}
}

// currentClusterIndex returns the configured cluster index for an account id.
func currentClusterIndex(id int) int {
	for idx, c := range clusterCfg.Clusters {
		if id >= c.StartID && id <= c.EndID {
			return idx
		}
	}
	return -1
}

func getBalanceRPC(addr string, id int) (float64, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	c := node.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.GetBalance(ctx, &node.GetBalanceReq{Id: int64(id)})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fmt.Errorf(resp.Msg)
	}
	return resp.Balance, nil
}

func setBalanceRPC(addr string, id int, val float64) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := node.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.SetBalance(ctx, &node.SetBalanceReq{Id: int64(id), Value: val})
	if err != nil {
		return err
	}
	if resp != nil && !resp.Ok {
		return fmt.Errorf(resp.Msg)
	}
	return nil
}

func setBalanceAll(addrs []string, id int, val float64) {
	for _, addr := range addrs {
		if err := setBalanceRPC(addr, id, val); err != nil {
			log.Printf("[reshard] set balance id %d to %s failed: %v", id, addr, err)
		}
	}
}

// migrateAccounts moves balances from their current cluster to the reassigned cluster.
// It reads from the source leader and writes to all nodes in the destination cluster,
// then zeros the source cluster.
func migrateAccounts(shards [][]int) {
	for dstIdx, ids := range shards {
		if dstIdx >= len(clusterCfg.Clusters) {
			continue
		}
		dstCluster := clusterCfg.Clusters[dstIdx].Nodes
		for _, id := range ids {
			srcIdx := currentClusterIndex(id)
			if srcIdx == -1 || srcIdx == dstIdx {
				continue // already in place or unknown
			}
			srcCluster := clusterCfg.Clusters[srcIdx].Nodes
			srcLeader := findLeader(srcCluster)
			if srcLeader == "" && len(srcCluster) > 0 {
				srcLeader = srcCluster[0]
			}
			if srcLeader == "" || len(dstCluster) == 0 {
				log.Printf("[reshard] skip id %d: leader missing (src=%s dstCluster=%v)", id, srcLeader, dstCluster)
				continue
			}
			bal, err := getBalanceRPC(srcLeader, id)
			if err != nil {
				log.Printf("[reshard] get balance id %d from %s failed: %v", id, srcLeader, err)
				continue
			}
			setBalanceAll(dstCluster, id, bal)
			// clear source cluster to avoid duplication
			setBalanceAll(srcCluster, id, 0)
			log.Printf("[reshard] moved id %d bal %.0f from cluster %d to %d", id, bal, srcIdx+1, dstIdx+1)
		}
	}
}

// doReshard computes a suggested 3-way partition and outputs moves as (id, cSrc, cDst).
func doReshard() {
	fmt.Println("Reshard: computing suggested partition from recent txn history...")
	shards := computeReshard()
	// derive current shard assignment from id ranges
	shardOf := func(id int) string {
		idx := currentClusterIndex(id)
		if idx >= 0 {
			return fmt.Sprintf("c%d", idx+1)
		}
		return "unknown"
	}
	// build target mapping
	targetShard := make(map[int]string)
	for i, group := range shards {
		dest := fmt.Sprintf("c%d", i+1)
		for _, id := range group {
			targetShard[id] = dest
		}
	}
	var moves []string
	for id, dest := range targetShard {
		src := shardOf(id)
		if src != "unknown" && src != dest {
			moves = append(moves, fmt.Sprintf("(%d, %s, %s)", id, src, dest))
		}
	}
	if len(moves) == 0 {
		fmt.Println("Reshard: no moves suggested (already balanced/co-located)")
		return
	}
	sort.Strings(moves)
	fmt.Println("Reshard moves:")
	for _, m := range moves {
		fmt.Println(" ", m)
	}
	// apply migration
	migrateAccounts(shards)
}

func portToNodeName(addr string) string {
	addr = strings.TrimSpace(addr)
	addr = strings.TrimPrefix(addr, "localhost")
	addr = strings.TrimPrefix(addr, ":")
	if len(addr) == 4 && strings.HasPrefix(addr, "500") {
		return "n" + addr[len(addr)-1:]
	}
	return addr
}

// PrintBalance queries all nodes in the shard of id and prints balances.
func PrintBalance(id string) {
	acct := mustAtoi(id)
	cluster := clusterForAccount(acct)
	results := []string{}
	for _, addr := range cluster {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			results = append(results, fmt.Sprintf("%s : err", portToNodeName(addr)))
			continue
		}
		cli := node.NewNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := cli.GetBalance(ctx, &node.GetBalanceReq{Id: int64(acct)})
		cancel()
		conn.Close()
		if err != nil || resp == nil || !resp.Ok {
			results = append(results, fmt.Sprintf("%s : err", portToNodeName(addr)))
			continue
		}
		results = append(results, fmt.Sprintf("%s : %.0f", portToNodeName(addr), resp.Balance))
	}
	fmt.Printf("PrintBalance(%s) => %s\n", id, strings.Join(results, ", "))
}

// waitForClusterReady polls until all nodes respond to PrintStatus and each shard reports a leader.
func waitForClusterReady(nodes []string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allUp := true
		for _, addr := range nodes {
			if _, err := queryStatus(addr); err != nil {
				allUp = false
				break
			}
		}
		if allUp {
			// ensure each shard has a leader
			for _, c := range clusterCfg.Clusters {
				if findLeader(c.Nodes) == "" {
					allUp = false
					break
				}
			}
		}
		if allUp {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("[waitForClusterReady] timeout waiting for all nodes/leaders to be ready")
}

func syncClientsToClusterLeader(clients map[string]*Client) {
	// sync per-shard instead of one global leader
	for _, cl := range clusterCfg.Clusters {
		cluster := cl.Nodes
		leader := findLeader(cluster)
		if leader == "" {
			continue
		}
		for _, c := range clients {
			if c == nil {
				continue
			}
			idNum, err := strconv.Atoi(c.id)
			if err != nil {
				continue
			}
			if sameCluster(clusterForAccount(idNum), cluster) && c.leader != leader {
				if err := c.reconnect(leader); err != nil {
					log.Printf("[Client %s] failed to reconnect to %s: %v", c.id, leader, err)
				}
				// update all peers in this shard to the discovered leader
				for _, addr := range cluster {
					liveSet := map[string]bool{addr: true}
					_ = setNodeStateRPC(addr, liveSet[addr])
				}
			}
		}
	}
}

func main() {
	benchMode := flag.Bool("bench", false, "run synthetic benchmark workload")
	roPct := flag.Float64("bench-ro-pct", 0.5, "fraction of read-only transactions (0..1)")
	crossPct := flag.Float64("bench-cross-pct", 0.5, "fraction of cross-shard txns (0..1)")
	skew := flag.Float64("bench-skew", 0.0, "skew (0 uniform, up to 1 for hotkeys)")
	durationSec := flag.Int("bench-duration", 30, "benchmark duration in seconds")
	count := flag.Int64("bench-count", 0, "total number of benchmark transactions (if >0, overrides duration)")
	workers := flag.Int("bench-workers", 4, "number of concurrent benchmark workers")
	amount := flag.Float64("bench-amount", 1.0, "transfer amount for RW txns")
	seed := flag.Int64("bench-seed", time.Now().UnixNano(), "RNG seed")
	flag.Parse()

	if !*benchMode && len(flag.Args()) < 1 {
		fmt.Println("Usage: go run client.go <input.csv>  OR with -bench flags")
		return
	}

	if *benchMode {
		log.SetOutput(io.Discard) // silence per-txn logs during benchmark
		benchModeGlobal = true
		cfg := benchConfig{
			roPct:    clamp01(*roPct),
			crossPct: clamp01(*crossPct),
			skew:     clamp01(*skew),
			duration: time.Duration(*durationSec) * time.Second,
			totalOps: *count,
			workers:  *workers,
			amount:   *amount,
			startID: func() int {
				if len(clusterCfg.Clusters) > 0 {
					return clusterCfg.Clusters[0].StartID
				} else {
					return 1
				}
			}(),
			endID: func() int {
				if len(clusterCfg.Clusters) > 0 {
					return clusterCfg.Clusters[len(clusterCfg.Clusters)-1].EndID
				} else {
					return 9000
				}
			}(),
			seed: *seed,
		}
		perf.Reset()
		runBenchmark(cfg)
		perf.Performance()
		return
	}

	benchModeGlobal = false
	testCases, err := parseCSV(flag.Args()[0])
	if err != nil {
		log.Fatalf("Error reading CSV: %v", err)
	}

	clients := make(map[string]*Client)

	reader := bufio.NewReader(os.Stdin)

	for _, tc := range testCases {
		fmt.Printf("\n==============================\n")
		fmt.Printf("Test Case #%d | Live Nodes: %v\n", tc.setNo, tc.liveNodes)
		fmt.Printf("==============================\n")
		fmt.Print("Press ENTER to process this transaction set...")
		_, _ = reader.ReadString('\n')
		// clear any leftovers from previous sets
		backlog = backlog[:0]
		backlogTxns = backlogTxns[:0]
		perf.Reset()
		// backlogSeen = make(map[string]bool)
		// successMu.Lock()
		// successTS = make(map[string]bool)
		// successMu.Unlock()
		historyMu.Lock()
		txnHistory = nil
		historyMu.Unlock()
		resetCluster(ports, 10)
		// Re-apply redistributed mapping after reset so accounts stay on their new shards.
		if len(redistributed) > 0 {
			assign := assignmentFromRedistribution()
			migrateAccounts(assign)
		}
		applyLiveNodes(tc.liveNodes)
		waitForClusterReady(ports, 5*time.Second)

		segs := splitByLF(tc.transactions)
		// append a demo read-only txn after existing txns in this set
		if len(segs) == 0 {
			segs = []segment{{}}
		}
		readTs := nextTs()
		segs[len(segs)-1].txns = append(segs[len(segs)-1].txns, &Transaction{
			from:     "1", // sample read of account 1
			readOnly: true,
			ts:       readTs,
		})
		processSegments(clients, segs, tc.liveNodes)
		perf.Performance()

		fmt.Printf("Finished processing set #%d.\n", tc.setNo)
		syncClientsToClusterLeader(clients)

		for {
			fmt.Print("Type command (views/logs/txn/db/pbal/perf) or just ENTER to continue: ")
			line, _ := reader.ReadString('\n')
			line = strings.TrimSpace(line)
			if line == "" {
				break
			}
			switch {
			case strings.HasPrefix(line, "views"):
				parts := strings.Fields(line)
				addr := ":5001"
				if len(parts) > 1 {
					addr = parts[1]
				}
				printViews(addr)
			case strings.HasPrefix(line, "logs"):
				parts := strings.Fields(line)
				addr := ":5001"
				if len(parts) > 1 {
					addr = parts[1]
				}
				printLogs(addr)
			case strings.HasPrefix(line, "txn"):
				parts := strings.Fields(line)
				if len(parts) >= 3 {
					addr := parts[1]
					seq, _ := strconv.ParseInt(parts[2], 10, 64)
					printTxnStatus(addr, seq)
				} else {
					fmt.Println("usage: txn :500x <timestamp>")
				}
			case strings.HasPrefix(line, "db"):
				parts := strings.Fields(line)
				if len(parts) > 0 && parts[0] == "dball" {
					printAllDB(ports)
				} else {
					addr := ":5001"
					if len(parts) > 1 {
						addr = parts[1]
					}
					printDB(addr)
				}
			case strings.HasPrefix(line, "pbal"):
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					PrintBalance(parts[1])
				} else {
					fmt.Println("usage: pbal <id>")
				}
			case strings.HasPrefix(line, "perf"):
				perf.Performance()
			case strings.HasPrefix(line, "reshard"):
				doReshard()
			default:
				fmt.Println("unknown command; use views/logs/txn/db/pbal/perf/reshard or ENTER")
			}
		}
	}

	for _, c := range clients {
		if c != nil && c.conn != nil {
			c.conn.Close()
		}
	}
}
