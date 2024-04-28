package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math/rand"
	"time"
)

type User struct {
	Id       int
	Username string
	Email    string
}

type Config struct {
	redisClient *redis.Client
	dbClient    *sql.DB
	circuit     *CircuitBreaker
}

type CircuitBreaker struct {
	failureThreshold   int
	consecutiveFailure int
	open               bool
	openedAt           time.Time
}

const circuitBreakerResetTimeout = 2 * time.Second

func NewCircuitBreaker(failureThreshold int) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold:   failureThreshold,
		consecutiveFailure: 0,
		open:               false,
	}
}

func NewUser(dbClient *sql.DB, redisClient *redis.Client, failureThreshold int) *Config {
	return &Config{
		dbClient:    dbClient,
		redisClient: redisClient,
		circuit:     NewCircuitBreaker(failureThreshold),
	}
}

func (cb *CircuitBreaker) IsOpen() bool {
	if cb.open {
		// Check last time opened
		if time.Since(cb.openedAt) >= circuitBreakerResetTimeout {
			cb.open = false
			cb.consecutiveFailure = 0
			log.Println("Circuit Breaker closed")
		} else {
			return true
		}
	}
	return false
}

func (cb *CircuitBreaker) IncrementConsecutiveFailure() {
	cb.consecutiveFailure++
	if cb.consecutiveFailure >= cb.failureThreshold {
		cb.open = true
		cb.openedAt = time.Now()
		log.Println("Circuit Breaker opened")
	}
}

func (e *Config) getDataFromMysql(username string) (*User, error) {
	row := e.dbClient.QueryRow("SELECT * FROM users WHERE username = ?", username)

	user := &User{}
	err := row.Scan(&user.Id, &user.Username, &user.Email)
	if err != nil {
		return nil, err
	}

	// Save to redis
	err = e.saveToRedis(username, user)
	if err != nil {
		return nil, err
	}

	return user, nil

}

func (e *Config) getDataFromRedis(username string) (*User, error) {
	if e.circuit.IsOpen() {
		return nil, fmt.Errorf("circuit breaker is open")
	}

	val, err := e.redisClient.Get(username).Result()
	if err != nil {
		log.Printf("failed to get redis with key [%s], err: %v", username, err)

		log.Println("get data from mysql")
		user, err := e.getDataFromMysql(username)
		if err != nil {
			e.circuit.IncrementConsecutiveFailure()
			return nil, err
		}

		return user, nil

	}

	user := &User{}
	err = json.Unmarshal([]byte(val), &user)
	if err != nil {
		return nil, err
	}

	return user, nil
}

func (e *Config) saveToRedis(key string, data *User) error {
	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Save to redis
	ttl := 2 * time.Millisecond
	err = e.redisClient.Set(key, jsonData, ttl).Err()
	if err != nil {
		return err
	}

	return nil

}

func main() {
	mysqlConn, err := sql.Open("mysql", "root:mysqlsecret@tcp(localhost:3306)/employees")
	if err != nil {
		panic(err)
	}

	redisConn := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	failureThreshold := 3
	client := NewUser(mysqlConn, redisConn, failureThreshold)

	username := "user20001"

	// Simulate get cache miss as asyncronous
	for i := 0; i < 100; i++ {
		go func() {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			val, err := client.getDataFromRedis(username)
			if err != nil {
				fmt.Println(err)

			} else {
				fmt.Printf("Got value: %v\n", val)

			}
		}()
	}

	select {}
}
