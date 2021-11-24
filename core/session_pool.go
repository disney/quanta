package core

import (
	"fmt"
	u "github.com/araddon/gou"
	sch "github.com/araddon/qlbridge/schema"
	"github.com/disney/quanta/client"
	"runtime"
	"sync"
	"time"
)

// SessionPoolConfig - Session pool configuration
type SessionPool struct {
	AppHost      *quanta.Conn
	schema       *sch.Schema
	baseDir      string
	sessPoolMap  map[string]*sessionPoolEntry
	sessPoolLock sync.Mutex
	semaphores   chan struct{}
}

// SessionPool - Pool of Quanta connections
type sessionPoolEntry struct {
	pool chan *Session
}

func NewSessionPool(appHost *quanta.Conn, schema *sch.Schema, baseDir string) *SessionPool {
	p := &SessionPool{AppHost: appHost, schema: schema, baseDir: baseDir,
		sessPoolMap: make(map[string]*sessionPoolEntry), semaphores: make(chan struct{}, runtime.NumCPU())}
	for i := 0; i < runtime.NumCPU(); i++ {
		p.semaphores <- struct{}{}
	}
	return p
}

func (m *SessionPool) newSessionPoolEntry() *sessionPoolEntry {
	return &sessionPoolEntry{pool: make(chan *Session, runtime.NumCPU())}
}

func (m *SessionPool) getPoolByTableName(tableName string) *sessionPoolEntry {
	m.sessPoolLock.Lock()
	defer m.sessPoolLock.Unlock()
	var cp *sessionPoolEntry
	var found bool
	if cp, found = m.sessPoolMap[tableName]; !found {
		cp = m.newSessionPoolEntry()
		m.sessPoolMap[tableName] = cp
	}
	return cp
}

// BorrowSession - get a pooled connection.
func (m *SessionPool) Borrow(tableName string) (*Session, error) {

	cp := m.getPoolByTableName(tableName)
	select {
	case <-m.semaphores:
		select {
		case r := <-cp.pool:
			var err error
			if m.schema != nil && m.schema.Since(time.Until(r.CreatedAt)) {
				u.Debugf("pooled connection is stale after schema change, refreshing.")
				r.CloseSession()
				r, err = m.newSession(tableName)
				if err != nil {
					return nil, err
				}
			}
			return r, nil
		default:
			conn, err := m.newSession(tableName)
			if err != nil {
				return nil, fmt.Errorf("BorrowSession %v", err)
			}
			return conn, nil
		}
	default:
		return nil, fmt.Errorf("POOL IS DRAINED!!!")
	}
}

// ReturnSession - return a connection to the pool.
func (m *SessionPool) Return(tableName string, conn *Session) {

	conn.Flush()
	cp := m.getPoolByTableName(tableName)
	select {
	case m.semaphores <- struct{}{}:
		select {
		case cp.pool <- conn:
		default:
			conn.CloseSession()
		}
	default: //Don't block
	}
	return
}

func (m *SessionPool) newSession(tableName string) (*Session, error) {

	conn, err := OpenSession(m.baseDir, tableName, false, m.AppHost)
	if err != nil {
		u.Errorf("error opening quanta connection - %v", err)
		return nil, err
	}
	return conn, nil
}

func (m *SessionPool) Shutdown() {

	for _, v := range m.sessPoolMap {
		close(v.pool)
		for x := range v.pool {
			x.CloseSession()
		}
	}
	close(m.semaphores)
}

func (m *SessionPool) Metrics() (poolSize, inUse int) {

	poolSize = runtime.NumCPU()
	inUse = poolSize - len(m.semaphores)
	return
}
