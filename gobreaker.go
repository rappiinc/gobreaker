// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package gobreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

var (
	// ErrTooManyRequests is returned when the CB state is half open and the requests count is over the cb maxRequests
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the CB state is open
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// Counts holds the numbers of requests and their successes/failures.
// CircuitBreaker clears the internal Counts either
// on the change of the state or at the closed-state intervals.
// Counts ignores the results of the requests sent before clearing.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

type countsPerMoment struct {
	secondsToKeep int64
	mutex         *sync.Mutex
	counts        map[int64]*Counts
}

func (c *countsPerMoment) getOrCreateCount(now int64) *Counts {
	counts, exists := c.counts[now]
	if !exists {
		counts = &Counts{}
		c.counts[now] = counts
	}
	return counts
}
func (c *countsPerMoment) prune(now time.Time) {
	expiredKeys := []int64{}

	for k := range c.counts {
		if k < (now.Unix() - c.secondsToKeep) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		delete(c.counts, k)
	}
}

func (c *countsPerMoment) assembleCount(now time.Time) Counts {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.prune(now)

	accumulatedCount := Counts{}

	accumulateConsecutivesSuccesses := true
	accumulateConsecutivesFailures := true

	timeLowerBound := now.Unix() - c.secondsToKeep

	for moment := now.Unix(); moment >= timeLowerBound; moment-- {
		counts := c.counts[moment]
		if counts != nil {
			accumulatedCount.Requests = accumulatedCount.Requests + counts.Requests
			accumulatedCount.TotalSuccesses = accumulatedCount.TotalSuccesses + counts.TotalSuccesses
			accumulatedCount.TotalFailures = accumulatedCount.TotalFailures + counts.TotalFailures
			if accumulateConsecutivesSuccesses {
				accumulatedCount.ConsecutiveSuccesses = accumulatedCount.ConsecutiveSuccesses + counts.ConsecutiveSuccesses
			}
			if accumulateConsecutivesFailures {
				accumulatedCount.ConsecutiveFailures = accumulatedCount.ConsecutiveFailures + counts.ConsecutiveFailures
			}
			accumulateConsecutivesSuccesses = accumulateConsecutivesSuccesses && (accumulatedCount.ConsecutiveSuccesses == accumulatedCount.Requests)
			accumulateConsecutivesFailures = accumulateConsecutivesFailures && (accumulatedCount.ConsecutiveFailures == accumulatedCount.Requests)
		}
	}
	return accumulatedCount
}

func (c *countsPerMoment) onRequest(now time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	unix := now.Unix()
	cc := c.getOrCreateCount(unix)

	cc.Requests++

	c.prune(now)
}

func (c *countsPerMoment) onSuccess(now time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	unix := now.Unix()
	cc := c.getOrCreateCount(unix)

	cc.TotalSuccesses++
	cc.ConsecutiveSuccesses++
	cc.ConsecutiveFailures = 0

	c.prune(now)
}

func (c *countsPerMoment) onFailure(now time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	unix := now.Unix()
	cc := c.getOrCreateCount(unix)

	cc.TotalFailures++
	cc.ConsecutiveFailures++
	cc.ConsecutiveSuccesses = 0

	c.prune(now)
}

func (c *countsPerMoment) clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.counts = map[int64]*Counts{}
}

// Settings configures CircuitBreaker:
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
type Settings struct {
	Name                string
	MaxRequests         uint32
	SecondsToAccountFor int64
	Timeout             time.Duration
	ReadyToTrip         func(counts Counts) bool
	RecordError         func(err error) bool
	OnStateChange       func(name string, from State, to State)
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	timeout       time.Duration
	readyToTrip   func(counts Counts) bool
	recordError   func(err error) bool
	onStateChange func(name string, from State, to State)
	timeProvider  func() time.Time

	mutex      sync.Mutex
	state      State
	generation uint64
	counts     countsPerMoment
	expiry     time.Time
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange
	cb.timeProvider = func() time.Time {
		return time.Now()
	}

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	secondsToKeep := int64(0)

	if st.SecondsToAccountFor <= 0 {
		secondsToKeep = defaultSecondsToAccountFor
	} else {
		secondsToKeep = st.SecondsToAccountFor
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.RecordError == nil {
		cb.recordError = defaultRecordError
	} else {
		cb.recordError = st.RecordError
	}

	cb.counts = countsPerMoment{
		counts:        map[int64]*Counts{},
		mutex:         &sync.Mutex{},
		secondsToKeep: secondsToKeep,
	}

	cb.toNewGeneration(cb.timeProvider())

	return cb
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st),
	}
}

const defaultSecondsToAccountFor = 120
const defaultTimeout = time.Duration(60) * time.Second

func defaultReadyToTrip(counts Counts) bool {
	return counts.ConsecutiveFailures > 5
}

func defaultRecordError(err error) bool {
	return true
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := cb.timeProvider()
	state, _ := cb.currentState(now)
	return state
}

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	result, err := req()
	cb.afterRequest(generation, err == nil || !cb.recordError(err))
	return result, err
}

// Name returns the name of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) Name() string {
	return tscb.cb.Name()
}

// State returns the current state of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) State() State {
	return tscb.cb.State()
}

// Allow checks if a new request can proceed. It returns a callback that should be used to
// register the success or failure in a separate step. If the circuit breaker doesn't allow
// requests, it returns an error.
func (tscb *TwoStepCircuitBreaker) Allow() (done func(success bool), err error) {
	generation, err := tscb.cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		tscb.cb.afterRequest(generation, success)
	}, nil
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := cb.timeProvider()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.counts.assembleCount(now).Requests >= cb.maxRequests {
		return generation, ErrTooManyRequests
	}

	cb.counts.onRequest(now)
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := cb.timeProvider()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onSuccess(now)
	case StateHalfOpen:
		cb.counts.onSuccess(now)
		if cb.counts.assembleCount(now).ConsecutiveSuccesses >= cb.maxRequests {
			cb.setState(StateClosed, now)
		}
	}
}

func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onFailure(now)
		if cb.readyToTrip(cb.counts.assembleCount(now)) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	//case StateClosed:
	//	if !cb.expiry.IsZero() && cb.expiry.Before(now) {
	//		cb.toNewGeneration(now)
	//	}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.clear()

	var zero time.Time
	switch cb.state {
	case StateClosed:
		cb.expiry = zero
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.expiry = zero
	}
}
