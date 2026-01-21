// hw1/db/store.go
package db

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	balancesBucket = "balances"
	walBucket      = "wal"
	metaBucket     = "meta"
	modBucket      = "modified"
	hyperBucket    = "hypergraph"
	defaultBalance = int64(10)
	totalIDs       = 9000
)

type Store struct {
	db *bolt.DB
}

type LockTable struct {
	mu    sync.Mutex
	byKey map[string]string // accountID -> txnID
}

func NewLockTable() *LockTable {
	return &LockTable{byKey: make(map[string]string)}
}

func (lt *LockTable) TryLock(keys []string, txnID string) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	for _, k := range keys {
		if holder, ok := lt.byKey[k]; ok && holder != txnID {
			return false
		}
	}
	for _, k := range keys {
		lt.byKey[k] = txnID
	}
	return true
}

func (lt *LockTable) Unlock(keys []string, txnID string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	for _, k := range keys {
		if holder, ok := lt.byKey[k]; ok && holder == txnID {
			delete(lt.byKey, k)
		}
	}
}

func (s *Store) NewStore(path string, prepopulate bool) (*Store, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		buckets := []string{balancesBucket, walBucket, metaBucket, modBucket, hyperBucket}
		for _, name := range buckets {
			if _, e := tx.CreateBucketIfNotExists([]byte(name)); e != nil {
				return fmt.Errorf("create bucket %s: %w", name, e)
			}
		}
		if prepopulate {
			b := tx.Bucket([]byte(balancesBucket))
			for i := 1; i <= totalIDs; i++ {
				k := keyForID(i)
				if v := b.Get(k); v == nil {
					if e := b.Put(k, encInt(defaultBalance)); e != nil {
						return fmt.Errorf("prepopulate id %d: %w", i, e)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) ResetBalances(ids []int, val int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		targets := ids
		if targets == nil {
			targets = make([]int, totalIDs)
			for i := 0; i < totalIDs; i++ {
				targets[i] = i + 1
			}
		}
		for _, id := range targets {
			if err := b.Put(keyForID(id), encInt(val)); err != nil {
				return fmt.Errorf("reset id %d: %w", id, err)
			}
		}

		if mod := tx.Bucket([]byte(modBucket)); mod != nil {
			_ = tx.DeleteBucket([]byte(modBucket))
			if nb, _ := tx.CreateBucket([]byte(modBucket)); nb != nil {
			}
		}
		if wal := tx.Bucket([]byte(walBucket)); wal != nil {
			_ = wal.ForEach(func(k, _ []byte) error {
				return wal.Delete(k)
			})
		}
		if hyper := tx.Bucket([]byte(hyperBucket)); hyper != nil {
			_ = hyper.ForEach(func(k, _ []byte) error {
				return hyper.Delete(k)
			})
		}
		return nil
	})
}

func (s *Store) ListBalances(filter func(string) bool) (map[string]float64, error) {
	out := make(map[string]float64)
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		return b.ForEach(func(k, v []byte) error {
			id := string(k)
			if filter != nil && !filter(id) {
				return nil
			}
			out[id] = float64(decInt(v))
			return nil
		})
	})
	return out, err
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) ListModifiedBalances() (map[string]float64, error) {
	out := make(map[string]float64)
	err := s.db.View(func(tx *bolt.Tx) error {
		bal := tx.Bucket([]byte(balancesBucket))
		mod := tx.Bucket([]byte(modBucket))
		if bal == nil || mod == nil {
			return fmt.Errorf("buckets missing")
		}
		return mod.ForEach(func(k, _ []byte) error {
			v := bal.Get(k)
			if v == nil {
				return nil
			}
			out[string(k)] = float64(decInt(v))
			return nil
		})
	})
	return out, err
}

func (s *Store) GetBalance(id int) (int64, error) {
	var bal int64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		v := b.Get(keyForID(id))
		if v == nil {
			bal = 0
			return nil
		}
		bal = decInt(v)
		return nil
	})
	return bal, err
}

func (s *Store) ApplyTransfer(sender, receiver int, amt int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		sb := decInt(b.Get(keyForID(sender)))
		rb := decInt(b.Get(keyForID(receiver)))
		if sb < amt {
			return fmt.Errorf("insufficient funds: id %d has %d, needs %d", sender, sb, amt)
		}

		if wal := tx.Bucket([]byte(walBucket)); wal != nil {
			wk := []byte(fmt.Sprintf("%d-%d-%d-%d", sender, receiver, amt, time.Now().UnixNano()))
			wv := fmt.Sprintf("%d,%d", sb, rb)
			_ = wal.Put(wk, []byte(wv))
		}

		if err := b.Put(keyForID(sender), encInt(sb-amt)); err != nil {
			return err
		}
		if err := b.Put(keyForID(receiver), encInt(rb+amt)); err != nil {
			return err
		}

		if mod := tx.Bucket([]byte(modBucket)); mod != nil {
			_ = mod.Put(keyForID(sender), []byte{1})
			_ = mod.Put(keyForID(receiver), []byte{1})
		}

		if wal := tx.Bucket([]byte(walBucket)); wal != nil {
			_ = wal.Delete([]byte(fmt.Sprintf("%d-%d-%d-%d", sender, receiver, amt, time.Now().UnixNano())))
		}
		return nil
	})
}

func (s *Store) SubtractBalance(sender int, amt int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		sb := decInt(b.Get(keyForID(sender)))
		if sb < amt {
			return fmt.Errorf("insufficient funds: id %d has %d, needs %d", sender, sb, amt)
		}

		if wal := tx.Bucket([]byte(walBucket)); wal != nil {
			wk := []byte(fmt.Sprintf("debit-%d-%d-%d", sender, amt, time.Now().UnixNano()))
			wv := fmt.Sprintf("%d", sb)
			_ = wal.Put(wk, []byte(wv))
			defer wal.Delete(wk)
		}

		if err := b.Put(keyForID(sender), encInt(sb-amt)); err != nil {
			return err
		}
		if mod := tx.Bucket([]byte(modBucket)); mod != nil {
			_ = mod.Put(keyForID(sender), []byte{1})
		}
		return nil
	})
}

func (s *Store) AddBalance(receiver int, amt int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		rb := decInt(b.Get(keyForID(receiver)))

		if wal := tx.Bucket([]byte(walBucket)); wal != nil {
			wk := []byte(fmt.Sprintf("credit-%d-%d-%d", receiver, amt, time.Now().UnixNano()))
			wv := fmt.Sprintf("%d", rb)
			_ = wal.Put(wk, []byte(wv))
			defer wal.Delete(wk)
		}

		if err := b.Put(keyForID(receiver), encInt(rb+amt)); err != nil {
			return err
		}

		if mod := tx.Bucket([]byte(modBucket)); mod != nil {
			_ = mod.Put(keyForID(receiver), []byte{1})
		}
		return nil
	})
}

func (s *Store) RevertTransfer(sender, receiver int, amt int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		sb := decInt(b.Get(keyForID(sender)))
		rb := decInt(b.Get(keyForID(receiver)))
		if rb < amt {
			return fmt.Errorf("cannot revert: receiver %d has only %d", receiver, rb)
		}
		if err := b.Put(keyForID(sender), encInt(sb+amt)); err != nil {
			return err
		}
		if err := b.Put(keyForID(receiver), encInt(rb-amt)); err != nil {
			return err
		}
		if mod := tx.Bucket([]byte(modBucket)); mod != nil {
			_ = mod.Put(keyForID(sender), []byte{1})
			_ = mod.Put(keyForID(receiver), []byte{1})
		}
		return nil
	})
}

func (s *Store) SetBalance(id int, val int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(balancesBucket))
		if b == nil {
			return fmt.Errorf("bucket %s missing", balancesBucket)
		}
		if err := b.Put(keyForID(id), encInt(val)); err != nil {
			return err
		}
		if mod := tx.Bucket([]byte(modBucket)); mod != nil {
			_ = mod.Put(keyForID(id), []byte{1})
		}
		return nil
	})
}

func (s *Store) ClearModified(id int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mod := tx.Bucket([]byte(modBucket))
		if mod == nil {
			return fmt.Errorf("bucket %s missing", modBucket)
		}
		return mod.Delete(keyForID(id))
	})
}

type storedWAL struct {
	FromID     int
	ToID       int
	BeforeFrom int64
	BeforeTo   int64
	TrackFrom  bool
	TrackTo    bool
}

func (s *Store) SaveWAL(seq int64, fromID, toID int, beforeFrom, beforeTo int64, trackFrom, trackTo bool) error {
	rec := storedWAL{FromID: fromID, ToID: toID, BeforeFrom: beforeFrom, BeforeTo: beforeTo, TrackFrom: trackFrom, TrackTo: trackTo}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		wal := tx.Bucket([]byte(walBucket))
		if wal == nil {
			return fmt.Errorf("bucket %s missing", walBucket)
		}
		return wal.Put([]byte(fmt.Sprintf("2pc-%d", seq)), data)
	})
}

func (s *Store) LoadWAL(seq int64) (storedWAL, bool, error) {
	var rec storedWAL
	err := s.db.View(func(tx *bolt.Tx) error {
		wal := tx.Bucket([]byte(walBucket))
		if wal == nil {
			return fmt.Errorf("bucket %s missing", walBucket)
		}
		data := wal.Get([]byte(fmt.Sprintf("2pc-%d", seq)))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &rec)
	})
	if err != nil {
		return storedWAL{}, false, err
	}
	if rec.FromID == 0 && rec.ToID == 0 && !rec.TrackFrom && !rec.TrackTo {
		return storedWAL{}, false, nil
	}
	return rec, true, nil
}

func (s *Store) DeleteWAL(seq int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		wal := tx.Bucket([]byte(walBucket))
		if wal == nil {
			return fmt.Errorf("bucket %s missing", walBucket)
		}
		return wal.Delete([]byte(fmt.Sprintf("2pc-%d", seq)))
	})
}

func (s *Store) MarkModified(id int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mod := tx.Bucket([]byte(modBucket))
		if mod == nil {
			return fmt.Errorf("bucket %s missing", modBucket)
		}
		return mod.Put(keyForID(id), []byte{1})
	})
}

func (s *Store) WasModified(id int) (bool, error) {
	var hit bool
	err := s.db.View(func(tx *bolt.Tx) error {
		mod := tx.Bucket([]byte(modBucket))
		if mod == nil {
			return fmt.Errorf("bucket %s missing", modBucket)
		}
		hit = mod.Get(keyForID(id)) != nil
		return nil
	})
	return hit, err
}

func (s *Store) RecordHyperedge(edgeID string, participants []int) error {
	sorted := append([]int(nil), participants...)
	sort.Ints(sorted)
	parts := make([]string, len(sorted))
	for i, p := range sorted {
		parts[i] = strconv.Itoa(p)
	}
	val := strings.Join(parts, ",")
	return s.db.Update(func(tx *bolt.Tx) error {
		h := tx.Bucket([]byte(hyperBucket))
		if h == nil {
			return fmt.Errorf("bucket %s missing", hyperBucket)
		}
		return h.Put([]byte(edgeID), []byte(val))
	})
}

func (s *Store) GetHyperedge(edgeID string) ([]int, error) {
	var out []int
	err := s.db.View(func(tx *bolt.Tx) error {
		h := tx.Bucket([]byte(hyperBucket))
		if h == nil {
			return fmt.Errorf("bucket %s missing", hyperBucket)
		}
		v := h.Get([]byte(edgeID))
		if v == nil {
			return nil
		}
		for _, tok := range strings.Split(string(v), ",") {
			if tok == "" {
				continue
			}
			id, err := strconv.Atoi(tok)
			if err != nil {
				return fmt.Errorf("parse hyperedge %s token %q: %w", edgeID, tok, err)
			}
			out = append(out, id)
		}
		return nil
	})
	return out, err
}

func keyForID(id int) []byte {
	return []byte(strconv.Itoa(id))
}

func encInt(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

func decInt(b []byte) int64 {
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}
