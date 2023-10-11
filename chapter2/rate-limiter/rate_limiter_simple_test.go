package ratelimiter

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type Limiter struct {
	mu sync.Mutex
	// Bucket is filled with rate tokens per seconds
	rate int
	// Bucket size
	bucketSize int
	// Number of tokens in bucket
	nTokens int
	// Time last token was generated
	lastToken time.Time
}

func (s *Limiter) Wait() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// if there is enough tokens in the bucket
	if s.nTokens > 0 {
		s.nTokens--
		return
	}

	// if there is not enough tokens in the bucket
	tElapsed := time.Since(s.lastToken)
	period := time.Second / time.Duration(s.rate)
	nTokens := tElapsed.Nanoseconds() / period.Nanoseconds()
	s.nTokens = int(nTokens)
	if s.nTokens > s.bucketSize {
		s.nTokens = s.bucketSize
	}
	s.lastToken = s.lastToken.Add(time.Duration(nTokens) * period)

	// Filled the bucket. There may not be enough
	if s.nTokens > 0 {
		s.nTokens--
		return
	}

	// Wait until more token are available
	next := s.lastToken.Add(period)
	wait := next.Sub(time.Now())
	if wait >= 0 {
		time.Sleep(wait)
	}
	s.lastToken = next
}

func NewLimiter(rate int, limit int) *Limiter {
	return &Limiter{
		rate:       rate,
		bucketSize: limit,
		nTokens:    limit,
		lastToken:  time.Now(),
	}
}

func TestRateLimiter(t *testing.T) {
	limiter := NewLimiter(5, 10)

	for i := 1; i <= 100; i++ {
		limiter.Wait()
		fmt.Printf("Request: %v rate: %d bucketSize: %d nToken : %d lastToken: %v \n", i, limiter.rate, limiter.bucketSize, limiter.nTokens, limiter.lastToken)
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)))
	}
}
