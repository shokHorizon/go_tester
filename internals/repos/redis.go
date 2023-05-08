package repos

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	redisTracebackPrefix = "traceback:"
	redisTracebackExpire = time.Minute * 30
)

type TracebackRepository interface {
	SaveTraceback(taskId int, userId int, traceback string) error
	GetTraceback(taskId int, userId int) (string, error)
}

type RedisTracebackRepository struct {
	redisClient *redis.Client
}

func NewRedisTracebackRepository(addr, password string, db int) (*RedisTracebackRepository, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// ping redis to check if the connection is working
	if err := redisClient.Ping(redisClient.Context()).Err(); err != nil {
		return nil, err
	}

	return &RedisTracebackRepository{
		redisClient: redisClient,
	}, nil
}

func (r *RedisTracebackRepository) SaveTraceback(taskId int, userId int, traceback string) error {
	key := fmt.Sprintf("%s%d-%d", redisTracebackPrefix, taskId, userId)

	err := r.redisClient.Set(r.redisClient.Context(), key, traceback, redisTracebackExpire).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisTracebackRepository) GetTraceback(taskId int, userId int) (string, error) {
	key := fmt.Sprintf("%s%d-%d", redisTracebackPrefix, taskId, userId)

	val, err := r.redisClient.Get(r.redisClient.Context(), key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("traceback not found")
		}
		return "", err
	}

	return val, nil
}
