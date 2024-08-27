package main

import (
	"fmt"
	"log"
	"time"
)

type User struct {
	Id       int
	Username string
	Email    string
}

type Config struct {
	circuit *CircuitBreaker
}

type CircuitBreaker struct {
	failureThreshold   int
	successThreshold   int
	consecutiveFailure int
	successCount       int
	open               bool
	halfOpen           bool
	openedAt           time.Time
}

const (
	circuitBreakerResetTimeout = 2 * time.Second
	successThreshold           = 5
)

func NewCircuitBreaker(failureThreshold int) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold:   failureThreshold,
		successThreshold:   successThreshold,
		consecutiveFailure: 0,
		successCount:       0,
		open:               false,
		halfOpen:           false,
	}
}

func NewConfig(failureThreshold int) *Config {
	return &Config{
		circuit: NewCircuitBreaker(failureThreshold),
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	if !cb.open {
		return true
	}

	if time.Since(cb.openedAt) >= circuitBreakerResetTimeout {
		cb.halfOpen = true
		log.Println("Circuit Breaker half-open")
		return true
	}
	return false
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.consecutiveFailure = 0
	if cb.halfOpen {

		cb.successCount++

		if cb.successCount >= cb.successThreshold {
			cb.open = false
			cb.halfOpen = false
			cb.successCount = 0
			log.Println("Circuit Breaker closed")
		}
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.consecutiveFailure++
	cb.successCount = 0
	if cb.halfOpen || cb.consecutiveFailure >= cb.failureThreshold {
		cb.open = true
		cb.halfOpen = false
		cb.openedAt = time.Now()
		log.Println("Circuit Breaker opened")
	}
}

func (c *Config) simulateDataFetch(username string, requestNumber int) (*User, error) {
	if !c.circuit.AllowRequest() {
		return nil, fmt.Errorf("circuit breaker is open")
	}

	time.Sleep(50 * time.Millisecond)

	if requestNumber >= 5 && requestNumber <= 10 {
		c.circuit.RecordFailure()
		return nil, fmt.Errorf("simulated failure")
	}

	user := &User{
		Id:       requestNumber,
		Username: username,
		Email:    fmt.Sprintf("%s@example.com", username),
	}

	c.circuit.RecordSuccess()
	return user, nil
}

func main() {
	failureThreshold := 3
	client := NewConfig(failureThreshold)

	username := "user20001"

	for i := 1; i <= 80; i++ {
		val, err := client.simulateDataFetch(username, i)
		if err != nil {
			fmt.Printf("Request %d: %v\n", i, err)
		} else {
			fmt.Printf("Request %d: Got value: %v\n", i, val)
		}
		time.Sleep(100 * time.Millisecond) // Add delay between requests
	}
}
