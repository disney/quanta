package core

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	u "github.com/araddon/gou"
	sch "github.com/disney/quanta/qlbridge/schema"
	"github.com/disney/quanta/shared"
)

// ErrPoolDrained - Special case error indicates that the pool is exhausted
var ErrPoolDrained = errors.New("session pool drained")

// SessionPool - Session pool encapsulates a Quanta session.
type SessionPool struct {
	AppHost      *shared.Conn
	schema       *sch.Schema
	baseDir      string
	sessPoolMap  map[string]*sessionPoolEntry
	sessPoolLock sync.RWMutex
	semaphores   chan struct{}
	poolSize     int
	maxUsed      int32

	TableCache *TableCacheStruct

	closed bool // when semaphores is closed, race warning
}

// SessionPool - Pool of Quanta connections
type sessionPoolEntry struct {
	pool chan *Session
}

// NewSessionPool - Construct a session pool to constrain resource consumption.
func NewSessionPool(tableCache *TableCacheStruct, appHost *shared.Conn, schema *sch.Schema, baseDir string, poolSize int) *SessionPool {

	if poolSize == 0 {
		poolSize = runtime.NumCPU()
	}

	p := &SessionPool{AppHost: appHost, schema: schema, baseDir: baseDir,
		sessPoolMap: make(map[string]*sessionPoolEntry), semaphores: make(chan struct{}, poolSize), poolSize: poolSize}
	p.TableCache = tableCache
	for i := 0; i < poolSize; i++ {
		p.semaphores <- struct{}{}
	}
	return p
}

func (m *SessionPool) newSessionPoolEntry() *sessionPoolEntry {
	return &sessionPoolEntry{pool: make(chan *Session, m.poolSize)}
}

func (m *SessionPool) getPoolByTableName(tableName string) *sessionPoolEntry {

	var cp *sessionPoolEntry
	var found bool
	if cp, found = m.sessPoolMap[tableName]; !found {
		cp = m.newSessionPoolEntry()
		m.sessPoolMap[tableName] = cp
	}
	return cp
}

// Borrow - Get a pooled connection.
func (m *SessionPool) Borrow(tableName string) (*Session, error) {

	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()
	cp := m.getPoolByTableName(tableName)
	select {
	case <-m.semaphores:
		max := atomic.LoadInt32(&m.maxUsed)
		used := int32(m.poolSize - len(m.semaphores))
		if used > max {
			atomic.StoreInt32(&m.maxUsed, used)
		}
		select {
		case r := <-cp.pool:
			var err error
			if r == nil || (m.schema != nil && m.schema.Since(time.Until(r.CreatedAt))) {
				u.Warnf("pooled connection is stale after schema change, refreshing.")
				if r != nil {
					r.CloseSession()
				}
				r, err = m.NewSession(tableName)
				if err != nil {
					return nil, err
				}
			}
			return r, nil
		default:
			conn, err := m.NewSession(tableName)
			if err != nil {
				return nil, fmt.Errorf("borrowSession %v", err)
			}
			return conn, nil
		}
	default:
		return nil, ErrPoolDrained
	}
}

// Return - Return a connection to the pool.
func (m *SessionPool) Return(tableName string, conn *Session) {

	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()
	conn.Flush()
	cp := m.getPoolByTableName(tableName)
	fmt.Println("SessionPool.Return", tableName, len(m.semaphores), m.closed)
	select {
	case m.semaphores <- struct{}{}:
		select {
		case cp.pool <- conn:
		default:
			conn.CloseSession()
		}
	default: //Don't block
	}
}

// NewSession - Construct a new session.
func (m *SessionPool) NewSession(tableName string) (*Session, error) {

	conn, err := OpenSession(m.TableCache, m.baseDir, tableName, false, m.AppHost)
	if err != nil {
		u.Errorf("error opening quanta connection - %v", err)
		return nil, err
	}
	return conn, nil
}

// Shutdown - Terminate and destroy the pool.
func (m *SessionPool) Shutdown() {

	fmt.Println("SessionPool.Shutdown")

	for _, v := range m.sessPoolMap {
		close(v.pool)
		for x := range v.pool {
			x.CloseSession()
		}
	}
	m.closed = true
	close(m.semaphores)
}

// Recover from network event.  Purge session and optionally recover unflushed buffers.
func (m *SessionPool) Recover(unflushedCh chan *shared.BatchBuffer) {

	if m.closed {
		return
	}

	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()

	for _, v := range m.sessPoolMap {
		// Drain and close bad sessions
	loop:
		for {
			select {
			case r := <-v.pool:
				if unflushedCh != nil && !r.BatchBuffer.IsEmpty() {
					unflushedCh <- r.BatchBuffer
				}
				// Push a replacement semaphore
				select {
				case m.semaphores <- struct{}{}:
				default:
					continue
				}
			default:
				break loop
			}
		}
	}
}

// Lock - Prevent operations while a table maintenance event is in progress
func (m *SessionPool) Lock() {
	m.sessPoolLock.Lock()
}

// Unlock - Allow operations after a table maintenance event is complete
func (m *SessionPool) Unlock() {
	m.sessPoolLock.Unlock()
}

// Metrics - Return pool size and usage.
func (m *SessionPool) Metrics() (poolSize, inUse, pooled, maxUsed int) {

	poolSize = m.poolSize
	inUse = poolSize - len(m.semaphores)
	for _, v := range m.sessPoolMap {
		pooled = pooled + len(v.pool)
	}
	maxUsed = int(atomic.LoadInt32(&m.maxUsed))
	return
}
