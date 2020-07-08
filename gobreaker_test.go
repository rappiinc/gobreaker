package gobreaker

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var defaultCB *CircuitBreaker
var customCB *CircuitBreaker
var negativeDurationCB *CircuitBreaker
var customRecordErrorCB *CircuitBreaker

type StateChange struct {
	name string
	from State
	to   State
}

var stateChange StateChange

func pseudoSleep(cb *CircuitBreaker, period time.Duration) {
	f := cb.timeProvider

	cb.timeProvider = func() time.Time {
		return f().Add(period)
	}
}

func succeed(cb *CircuitBreaker) error {
	_, err := cb.Execute(func() (interface{}, error) { return nil, nil })
	return err
}

func succeedLater(cb *CircuitBreaker, delay time.Duration) (<-chan bool, <-chan error) {
	ch := make(chan error)
	startedCh := make(chan bool)

	go func() {
		_, err := cb.Execute(func() (interface{}, error) {
			startedCh <- true
			time.Sleep(delay)
			return nil, nil
		})
		ch <- err
	}()
	return startedCh, ch
}

func succeed2Step(cb *TwoStepCircuitBreaker) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(true)
	return nil
}

func fail(cb *CircuitBreaker) error {
	return failWithMessage(cb, "fail")
}

func failWithMessage(cb *CircuitBreaker, msg string) error {
	_, err := cb.Execute(func() (interface{}, error) { return nil, fmt.Errorf(msg) })
	if err.Error() == msg {
		return nil
	}
	return err
}

func fail2Step(cb *TwoStepCircuitBreaker) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(false)
	return nil
}

func causePanic(cb *CircuitBreaker) error {
	_, err := cb.Execute(func() (interface{}, error) { panic("oops"); return nil, nil })
	return err
}

func newCustom() *CircuitBreaker {
	var customSt Settings
	customSt.Name = "cb"
	customSt.MaxRequests = 3
	customSt.SecondsToAccountFor = int64(30)
	customSt.Timeout = time.Duration(90) * time.Second
	customSt.ReadyToTrip = func(counts Counts) bool {
		numReqs := counts.Requests
		failureRatio := float64(counts.TotalFailures) / float64(numReqs)
		return numReqs >= 3 && failureRatio >= 0.6
	}
	customSt.OnStateChange = func(name string, from State, to State) {
		stateChange = StateChange{name, from, to}
	}

	return NewCircuitBreaker(customSt)
}

const msgErrorToRecord = "record me"

func newCustomRecordErrorCB() *CircuitBreaker {
	var customSt Settings
	customSt.RecordError = func(err error) bool { return err.Error() == msgErrorToRecord }
	return NewCircuitBreaker(customSt)
}

func newNegativeDurationCB() *CircuitBreaker {
	var negativeSt Settings
	negativeSt.Name = "ncb"
	negativeSt.SecondsToAccountFor = int64(-30)
	negativeSt.Timeout = time.Duration(-90) * time.Second

	return NewCircuitBreaker(negativeSt)
}

func init() {
	defaultCB = NewCircuitBreaker(Settings{})
	customCB = newCustom()
	negativeDurationCB = newNegativeDurationCB()
	customRecordErrorCB = newCustomRecordErrorCB()
}

func TestStateConstants(t *testing.T) {
	assert.Equal(t, State(0), StateClosed)
	assert.Equal(t, State(1), StateHalfOpen)
	assert.Equal(t, State(2), StateOpen)

	assert.Equal(t, StateClosed.String(), "closed")
	assert.Equal(t, StateHalfOpen.String(), "half-open")
	assert.Equal(t, StateOpen.String(), "open")
	assert.Equal(t, State(100).String(), "unknown state: 100")
}

func TestNewCircuitBreaker(t *testing.T) {
	defaultCB := NewCircuitBreaker(Settings{})
	assert.Equal(t, "", defaultCB.name)
	assert.Equal(t, uint32(1), defaultCB.maxRequests)
	assert.Equal(t, int64(120), defaultCB.counts.secondsToKeep)
	assert.Equal(t, time.Duration(60)*time.Second, defaultCB.timeout)
	assert.NotNil(t, defaultCB.readyToTrip)
	assert.NotNil(t, negativeDurationCB.recordError)
	assert.Nil(t, defaultCB.onStateChange)
	assert.Equal(t, StateClosed, defaultCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))
	assert.True(t, defaultCB.expiry.IsZero())

	customCB := newCustom()
	assert.Equal(t, "cb", customCB.name)
	assert.Equal(t, uint32(3), customCB.maxRequests)
	assert.Equal(t, int64(30), customCB.counts.secondsToKeep)
	assert.Equal(t, time.Duration(90)*time.Second, customCB.timeout)
	assert.NotNil(t, customCB.readyToTrip)
	assert.NotNil(t, customCB.onStateChange)
	assert.NotNil(t, negativeDurationCB.recordError)
	assert.Equal(t, StateClosed, customCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts.assembleCount(customCB.timeProvider()))
	assert.True(t, customCB.expiry.IsZero())

	negativeDurationCB := newNegativeDurationCB()
	assert.Equal(t, "ncb", negativeDurationCB.name)
	assert.Equal(t, uint32(1), negativeDurationCB.maxRequests)
	assert.Equal(t, int64(120), negativeDurationCB.counts.secondsToKeep)
	assert.Equal(t, time.Duration(60)*time.Second, negativeDurationCB.timeout)
	assert.NotNil(t, negativeDurationCB.readyToTrip)
	assert.Nil(t, negativeDurationCB.onStateChange)
	assert.Equal(t, StateClosed, negativeDurationCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, negativeDurationCB.counts.assembleCount(negativeDurationCB.timeProvider()))
	assert.True(t, negativeDurationCB.expiry.IsZero())

	customRecordErrorCB = newCustomRecordErrorCB()
	assert.NotNil(t, negativeDurationCB.recordError)
}

func TestDefaultCircuitBreaker(t *testing.T) {
	defaultCB = NewCircuitBreaker(Settings{})

	assert.Equal(t, "", defaultCB.Name())

	for i := 0; i < 5; i++ {
		assert.Nil(t, fail(defaultCB))
	}
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{5, 0, 5, 0, 5}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))

	assert.Nil(t, succeed(defaultCB))
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{6, 1, 5, 1, 0}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))

	assert.Nil(t, fail(defaultCB))
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{7, 1, 6, 0, 1}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))

	// StateClosed to StateOpen
	for i := 0; i < 5; i++ {
		assert.Nil(t, fail(defaultCB)) // 6 consecutive failures
	}
	assert.Equal(t, StateOpen, defaultCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))
	assert.False(t, defaultCB.expiry.IsZero())

	assert.Error(t, succeed(defaultCB))
	assert.Error(t, fail(defaultCB))
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))

	pseudoSleep(defaultCB, time.Duration(59)*time.Second)
	assert.Equal(t, StateOpen, defaultCB.State())

	// StateOpen to StateHalfOpen
	pseudoSleep(defaultCB, time.Duration(1)*time.Second) // over Timeout
	assert.Equal(t, StateHalfOpen, defaultCB.State())
	assert.True(t, defaultCB.expiry.IsZero())

	// StateHalfOpen to StateOpen
	assert.Nil(t, fail(defaultCB))
	assert.Equal(t, StateOpen, defaultCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))
	assert.False(t, defaultCB.expiry.IsZero())

	// StateOpen to StateHalfOpen
	pseudoSleep(defaultCB, time.Duration(60)*time.Second)
	assert.Equal(t, StateHalfOpen, defaultCB.State())
	assert.True(t, defaultCB.expiry.IsZero())

	// StateHalfOpen to StateClosed
	assert.Nil(t, succeed(defaultCB))
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))
	assert.True(t, defaultCB.expiry.IsZero())
}

func TestCustomCircuitBreaker(t *testing.T) {
	customCB = newCustom()

	assert.Equal(t, "cb", customCB.Name())

	for i := 0; i < 5; i++ {
		assert.Nil(t, succeed(customCB))
		assert.Nil(t, fail(customCB))
	}
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{10, 5, 5, 0, 1}, customCB.counts.assembleCount(customCB.timeProvider()))

	pseudoSleep(customCB, time.Duration(29)*time.Second)
	assert.Nil(t, succeed(customCB))
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{11, 6, 5, 1, 0}, customCB.counts.assembleCount(customCB.timeProvider()))

	pseudoSleep(customCB, time.Duration(3)*time.Second) // over Interval
	assert.Nil(t, fail(customCB))
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{2, 1, 1, 0, 1}, customCB.counts.assembleCount(customCB.timeProvider()))

	// StateClosed to StateOpen
	assert.Nil(t, succeed(customCB))
	assert.Nil(t, fail(customCB))
	assert.Nil(t, fail(customCB)) // failure ratio: 2/3 >= 0.6
	assert.Equal(t, StateOpen, customCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts.assembleCount(customCB.timeProvider()))
	assert.False(t, customCB.expiry.IsZero())
	assert.Equal(t, StateChange{"cb", StateClosed, StateOpen}, stateChange)

	// StateOpen to StateHalfOpen
	pseudoSleep(customCB, time.Duration(90)*time.Second)
	assert.Equal(t, StateHalfOpen, customCB.State())
	assert.True(t, defaultCB.expiry.IsZero())
	assert.Equal(t, StateChange{"cb", StateOpen, StateHalfOpen}, stateChange)

	assert.Nil(t, succeed(customCB))
	assert.Nil(t, succeed(customCB))
	assert.Equal(t, StateHalfOpen, customCB.State())
	assert.Equal(t, Counts{2, 2, 0, 2, 0}, customCB.counts.assembleCount(customCB.timeProvider()))

	// StateHalfOpen to StateClosed
	startedChannel, ch := succeedLater(customCB, time.Duration(100)*time.Millisecond) // 3 consecutive successes
	<-startedChannel
	time.Sleep(time.Duration(50) * time.Millisecond)
	assert.Equal(t, Counts{3, 2, 0, 2, 0}, customCB.counts.assembleCount(customCB.timeProvider()))
	assert.Error(t, succeed(customCB)) // over MaxRequests
	assert.Nil(t, <-ch)
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts.assembleCount(customCB.timeProvider()))
	assert.True(t, customCB.expiry.IsZero())
	assert.Equal(t, StateChange{"cb", StateHalfOpen, StateClosed}, stateChange)
}

func TestCustomRecordErrorCircuitBreaker(t *testing.T) {
	assert.Equal(t, "", customRecordErrorCB.Name())

	for i := 0; i < 5; i++ {
		assert.Nil(t, failWithMessage(customRecordErrorCB, "do not record me"))
	}

	assert.Equal(t, StateClosed, customRecordErrorCB.State())
	assert.Equal(t, Counts{
		Requests:             5,
		TotalSuccesses:       5,
		TotalFailures:        0,
		ConsecutiveSuccesses: 5,
		ConsecutiveFailures:  0,
	}, customRecordErrorCB.counts.assembleCount(customRecordErrorCB.timeProvider()))

	for i := 0; i < 5; i++ {
		assert.Nil(t, failWithMessage(customRecordErrorCB, msgErrorToRecord))
	}

	assert.Equal(t, StateClosed, customRecordErrorCB.State())
	assert.Equal(t, Counts{
		Requests:             10,
		TotalSuccesses:       5,
		TotalFailures:        5,
		ConsecutiveSuccesses: 0,
		ConsecutiveFailures:  5,
	}, customRecordErrorCB.counts.assembleCount(customRecordErrorCB.timeProvider()))
}

func TestTwoStepCircuitBreaker(t *testing.T) {
	tscb := NewTwoStepCircuitBreaker(Settings{Name: "tscb"})
	assert.Equal(t, "tscb", tscb.Name())

	for i := 0; i < 5; i++ {
		assert.Nil(t, fail2Step(tscb))
	}

	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{5, 0, 5, 0, 5}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))

	assert.Nil(t, succeed2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{6, 1, 5, 1, 0}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))

	assert.Nil(t, fail2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{7, 1, 6, 0, 1}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))

	// StateClosed to StateOpen
	for i := 0; i < 5; i++ {
		assert.Nil(t, fail2Step(tscb)) // 6 consecutive failures
	}
	assert.Equal(t, StateOpen, tscb.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))
	assert.False(t, tscb.cb.expiry.IsZero())

	assert.Error(t, succeed2Step(tscb))
	assert.Error(t, fail2Step(tscb))
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))

	pseudoSleep(tscb.cb, time.Duration(59)*time.Second)
	assert.Equal(t, StateOpen, tscb.State())

	// StateOpen to StateHalfOpen
	pseudoSleep(tscb.cb, time.Duration(1)*time.Second) // over Timeout
	assert.Equal(t, StateHalfOpen, tscb.State())
	assert.True(t, tscb.cb.expiry.IsZero())

	// StateHalfOpen to StateOpen
	assert.Nil(t, fail2Step(tscb))
	assert.Equal(t, StateOpen, tscb.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))
	assert.False(t, tscb.cb.expiry.IsZero())

	// StateOpen to StateHalfOpen
	pseudoSleep(tscb.cb, time.Duration(60)*time.Second)
	assert.Equal(t, StateHalfOpen, tscb.State())
	assert.True(t, tscb.cb.expiry.IsZero())

	// StateHalfOpen to StateClosed
	assert.Nil(t, succeed2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts.assembleCount(tscb.cb.timeProvider()))
	assert.True(t, tscb.cb.expiry.IsZero())
}

func TestPanicInRequest(t *testing.T) {
	defaultCB = NewCircuitBreaker(Settings{})

	assert.Panics(t, func() { causePanic(defaultCB) })
	assert.Equal(t, Counts{1, 0, 1, 0, 1}, defaultCB.counts.assembleCount(defaultCB.timeProvider()))
}

func TestGeneration(t *testing.T) {
	customCB = newCustom()

	startedCh, ch := succeedLater(customCB, time.Duration(2)*time.Second)
	<-startedCh

	assert.Nil(t, fail(customCB))
	assert.Nil(t, fail(customCB))
	assert.NotNil(t, fail(customCB)) // We are open!

	pseudoSleep(customCB, time.Duration(91)*time.Second)

	assert.Nil(t, succeed(customCB))
	assert.Nil(t, succeed(customCB))
	assert.Nil(t, succeed(customCB)) // We are closed!

	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts.assembleCount(customCB.timeProvider()))

	// the request from the previous generation has no effect on customCB.counts
	assert.Nil(t, <-ch)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts.assembleCount(customCB.timeProvider()))

}

func TestCircuitBreakerInParallel(t *testing.T) {
	customCB = newCustom()

	runtime.GOMAXPROCS(runtime.NumCPU())

	ch := make(chan error)

	const numReqs = 10000
	routine := func() {
		for i := 0; i < numReqs; i++ {
			ch <- succeed(customCB)
		}
	}

	const numRoutines = 10
	for i := 0; i < numRoutines; i++ {
		go routine()
	}

	total := uint32(numReqs * numRoutines)
	for i := uint32(0); i < total; i++ {
		err := <-ch
		assert.Nil(t, err)
	}
	assert.Equal(t, Counts{total, total, 0, total, 0}, customCB.counts.assembleCount(customCB.timeProvider()))
}
