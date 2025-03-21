package ml

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	RoomMetaKey       = "room:meta"
	RoomShardKey      = "room:shard"
	MaxShardSize      = 100000
	MaxShardCount     = 100
	InitialShardCount = 4
	HeartbeatTimeout  = 120 // Heartbeat timeout in seconds
	ExpandingDuration = 300 // Duration after expansion to delete old data
	ExpandFactor      = 2
)

type Meta struct {
	ShardCount      int   `json:"shard_count"`
	PrevShardCount  int   `json:"prev_shard_count"`
	Expanding       bool  `json:"expanding"`
	ExpandStartTime int64 `json:"expand_start_time"`
	Version         int   `json:"version"`
}

type MemberList struct {
	id  int
	rdb *redis.Client
}

func NewMemberList() *MemberList {
	return &MemberList{
		id: rand.Intn(100),
		rdb: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
	}
}

// Get metadata
func (ml *MemberList) getMeta(ctx context.Context) (*Meta, error) {
	data, err := ml.rdb.Get(ctx, ml.getRoomMetaKey(ml.id)).Bytes()
	if err == redis.Nil {
		return &Meta{ShardCount: InitialShardCount}, nil
	} else if err != nil {
		return nil, err
	}
	var meta Meta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (ml *MemberList) updateMeta(ctx context.Context, f func(*Meta) error) error {
	metaKey := ml.getRoomMetaKey(ml.id)
	const maxRetries = 3
	for retries := 0; retries < maxRetries; retries++ {
		err := ml.rdb.Watch(ctx, func(tx *redis.Tx) error {
			metaJson, err := tx.Get(ctx, metaKey).Result()
			if err != nil && err != redis.Nil {
				return err
			}
			var meta Meta
			if metaJson == "" {
				meta.ShardCount = InitialShardCount
			} else if err := json.Unmarshal([]byte(metaJson), &meta); err != nil {
				return err
			}
			if err := f(&meta); err != nil {
				return err
			}
			newMetaJson, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, metaKey, newMetaJson, 0)
				return nil
			})
			return err
		}, metaKey)
		if err == nil {
			return nil
		}
		if err != redis.TxFailedErr {
			return err
		}
	}

	err := fmt.Errorf("failed to update metadata after %d retries", maxRetries)
	log.Println(err)
	return err
}

// Calculate the shard key for a user
func (ml *MemberList) getShardKey(version int, shardId int) string {
	return fmt.Sprintf("%s:%d:%d", RoomShardKey, version, shardId)
}

func (ml *MemberList) getRoomMetaKey(roomId int) string {
	return fmt.Sprintf("%s:%d", RoomMetaKey, roomId)
}

// Update user heartbeat
func (ml *MemberList) updateMember(ctx context.Context, uid string) error {
	meta, err := ml.getMeta(ctx)
	if err != nil {
		return err
	}
	now := float64(time.Now().Unix())
	if _, err = ml.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		shard := ml.getShardKey(meta.Version, hash(uid)%meta.ShardCount)
		pipe.ZAdd(ctx, shard, &redis.Z{Score: now, Member: uid})
		if meta.Expanding {
			prevShard := ml.getShardKey(meta.Version-1, hash(uid)%meta.PrevShardCount)
			pipe.ZAdd(ctx, prevShard, &redis.Z{Score: now, Member: uid})
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (ml *MemberList) removeMember(ctx context.Context, uid string) error {
	meta, err := ml.getMeta(ctx)
	if err != nil {
		return err
	}
	if _, err = ml.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		shard := ml.getShardKey(meta.Version, hash(uid)%meta.ShardCount)
		pipe.ZRem(ctx, shard, uid)
		if meta.Expanding {
			prevShard := ml.getShardKey(meta.Version-1, hash(uid)%meta.PrevShardCount)
			pipe.ZRem(ctx, prevShard, uid)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (ml *MemberList) countSublist(ctx context.Context, meta *Meta) ([]int64, error) {
	expireTime := float64(time.Now().Unix() - HeartbeatTimeout)
	version := meta.Version
	shardCount := meta.ShardCount
	if meta.Expanding {
		shardCount = meta.PrevShardCount
		version--
	}

	pipe := ml.rdb.Pipeline()
	commands := make([]*redis.IntCmd, shardCount)
	for i := 0; i < shardCount; i++ {
		shard := ml.getShardKey(version, i)
		commands[i] = pipe.ZCount(ctx, shard, fmt.Sprintf("%f", expireTime), "+inf")
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	counts := make([]int64, shardCount)
	for i, cmd := range commands {
		if err := cmd.Err(); err != nil {
			return nil, err
		}
		counts[i] = cmd.Val()
	}
	return counts, nil
}

func (ml *MemberList) Count(ctx context.Context) (int64, error) {
	meta, err := ml.getMeta(ctx)
	if err != nil {
		return 0, err
	}
	counts, err := ml.countSublist(ctx, meta)
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, count := range counts {
		total += count
	}
	return total, nil
}

func (ml *MemberList) checkExpand(ctx context.Context, meta *Meta) (bool, error) {
	if meta.Expanding {
		return false, nil
	}
	if meta.ShardCount*ExpandFactor > MaxShardCount {
		return false, nil
	}
	counts, err := ml.countSublist(ctx, meta)
	if err != nil {
		return false, err
	}
	for _, count := range counts {
		if count > MaxShardSize {
			return true, nil
		}
	}
	return false, nil
}

// Clean up expired users and delete old shards
func (ml *MemberList) cleanupExpiredUsers(ctx context.Context) error {
	meta, err := ml.getMeta(ctx)
	if err != nil {
		log.Println("Failed to get metadata:", err)
		return err
	}

	now := time.Now().Unix()
	expire := float64(now - HeartbeatTimeout)
	pipe := ml.rdb.Pipeline()
	for i := 0; i < meta.ShardCount; i++ {
		shard := ml.getShardKey(meta.Version, i)
		pipe.ZRemRangeByScore(ctx, shard, "0", fmt.Sprintf("%f", expire))
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Printf("Failed to remove expired users: %v", err)
	}

	// If expanding and past ExpandingDuration, delete old shards
	if meta.Expanding {
		if now-meta.ExpandStartTime < ExpandingDuration {
			pipe := ml.rdb.Pipeline()
			for i := 0; i < meta.PrevShardCount; i++ {
				shard := ml.getShardKey(meta.Version-1, i)
				pipe.ZRemRangeByScore(ctx, shard, "0", fmt.Sprintf("%f", expire))
			}
			_, err = pipe.Exec(ctx)
			if err != nil {
				log.Printf("Failed to remove expired users: %v", err)
			}
		} else {
			pipe := ml.rdb.Pipeline()
			for i := 0; i < meta.PrevShardCount; i++ {
				shard := ml.getShardKey(meta.Version-1, i)
				pipe.Del(ctx, shard)
			}
			commands, err := pipe.Exec(ctx)
			if err != nil {
				log.Printf("Failed to execute pipeline: %v", err)
				return err
			}
			for _, cmd := range commands {
				if err := cmd.Err(); err != nil {
					log.Printf("Failed to execute command: %v", err)
					return err
				}
			}
			log.Printf("Deleted %d old shards", meta.PrevShardCount)

			// Update Meta to mark expansion as complete
			if err := ml.updateMeta(ctx, func(m *Meta) error {
				if !m.Expanding {
					return errors.New("meta is stable")
				}
				if m.Version != meta.Version {
					return errors.New("version changed")
				}
				m.Expanding = false
				return nil
			}); err != nil {
				log.Println("Failed to update metadata after cleanup:", err)
				return err
			}
			log.Println("Expansion completed")
		}
	}

	return nil
}

// Expansion logic
func (ml *MemberList) expand(ctx context.Context) error {
	meta, err := ml.getMeta(ctx)
	if err != nil {
		log.Println("Failed to get metadata:", err)
		return err
	}

	ok, err := ml.checkExpand(ctx, meta)
	if err != nil {
		log.Println("Failed to get metadata:", err)
		return err
	}
	if !ok {
		return nil
	}

	if err = ml.updateMeta(ctx, func(m *Meta) error {
		if m.Expanding {
			return errors.New("is expanding now")
		}
		if m.Version != meta.Version {
			return errors.New("version changed")
		}
		if m.ShardCount*ExpandFactor > MaxShardCount {
			return errors.New("too many shards")
		}
		m.Expanding = true
		m.ExpandStartTime = time.Now().Unix()
		m.PrevShardCount = m.ShardCount
		m.ShardCount *= ExpandFactor
		m.Version += 1
		return nil
	}); err != nil {
		log.Println("Failed to expand shards:", err)
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()
	ml := NewMemberList()

	// Periodically clean up expired users
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			if err := ml.cleanupExpiredUsers(ctx); err != nil {
				fmt.Println(err)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			if err := ml.expand(ctx); err != nil {
				log.Println(err)
				continue
			}
		}
	}()

	// Simulate user heartbeat
	for {
		ml.updateMember(ctx, "user123")
		time.Sleep(30 * time.Second)
	}
}

func hash(uid string) int {
	return int(crc32.ChecksumIEEE([]byte(uid)) & 0x7fffffff)
}
