package statistic

import (
	"os"
	"time"

	"github.com/metacubex/mihomo/common/atomic"

	"github.com/metacubex/mihomo/common/observable"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/shirou/gopsutil/v4/process"
)

var DefaultManager *Manager

func init() {
	DefaultManager = &Manager{
		connections:   xsync.NewMapOf[string, Tracker](),
		uploadTemp:    atomic.NewInt64(0),
		downloadTemp:  atomic.NewInt64(0),
		uploadBlip:    atomic.NewInt64(0),
		downloadBlip:  atomic.NewInt64(0),
		uploadTotal:   atomic.NewInt64(0),
		downloadTotal: atomic.NewInt64(0),
		process:       &process.Process{Pid: int32(os.Getpid())},

		joinCh:  make(chan Tracker),
		leaveCh: make(chan Tracker),
	}
	DefaultManager.joinObservable = observable.NewObservable[Tracker](DefaultManager.joinCh)
	DefaultManager.leaveObservable = observable.NewObservable[Tracker](DefaultManager.leaveCh)

	go DefaultManager.handle()
}

type Manager struct {
	connections   *xsync.MapOf[string, Tracker]
	uploadTemp    atomic.Int64
	downloadTemp  atomic.Int64
	uploadBlip    atomic.Int64
	downloadBlip  atomic.Int64
	uploadTotal   atomic.Int64
	downloadTotal atomic.Int64
	process       *process.Process
	memory        uint64

	joinCh          chan Tracker
	joinObservable  *observable.Observable[Tracker]
	leaveCh         chan Tracker
	leaveObservable *observable.Observable[Tracker]
}

// 新连接
func (m *Manager) Join(c Tracker) {
	m.connections.Store(c.ID(), c)
	m.joinCh <- c
}

// 断开连接
func (m *Manager) Leave(c Tracker) {
	if _, ok := m.connections.LoadAndDelete(c.ID()); ok {
		m.leaveCh <- c
	}
}

// 订阅Join事件
func (m *Manager) SubscribeJoin() observable.Subscription[Tracker] {
	sub, _ := m.joinObservable.Subscribe()
	return sub
}

// 移除Join事件的订阅者
func (m *Manager) UnsubscribeJoin(sub observable.Subscription[Tracker]) {
	m.joinObservable.UnSubscribe(sub)
}

// 订阅Leave事件
func (m *Manager) SubscribeLeave() observable.Subscription[Tracker] {
	sub, _ := m.leaveObservable.Subscribe()
	return sub
}

// 移除Leave事件的订阅者
func (m *Manager) UnsubscribeLeave(sub observable.Subscription[Tracker]) {
	m.leaveObservable.UnSubscribe(sub)
}

func (m *Manager) Get(id string) (c Tracker) {
	if value, ok := m.connections.Load(id); ok {
		c = value
	}
	return
}

func (m *Manager) Range(f func(c Tracker) bool) {
	m.connections.Range(func(key string, value Tracker) bool {
		return f(value)
	})
}

func (m *Manager) PushUploaded(size int64) {
	m.uploadTemp.Add(size)
	m.uploadTotal.Add(size)
}

func (m *Manager) PushDownloaded(size int64) {
	m.downloadTemp.Add(size)
	m.downloadTotal.Add(size)
}

func (m *Manager) Now() (up int64, down int64) {
	return m.uploadBlip.Load(), m.downloadBlip.Load()
}

func (m *Manager) Memory() uint64 {
	m.updateMemory()
	return m.memory
}

func (m *Manager) Snapshot() *Snapshot {
	var connections []*TrackerInfo
	m.Range(func(c Tracker) bool {
		connections = append(connections, c.Info())
		return true
	})
	return &Snapshot{
		UploadTotal:   m.uploadTotal.Load(),
		DownloadTotal: m.downloadTotal.Load(),
		Connections:   connections,
		Memory:        m.memory,
	}
}

func (m *Manager) updateMemory() {
	stat, err := m.process.MemoryInfo()
	if err != nil {
		return
	}
	m.memory = stat.RSS
}

func (m *Manager) ResetStatistic() {
	m.uploadTemp.Store(0)
	m.uploadBlip.Store(0)
	m.uploadTotal.Store(0)
	m.downloadTemp.Store(0)
	m.downloadBlip.Store(0)
	m.downloadTotal.Store(0)
}

func (m *Manager) handle() {
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		m.uploadBlip.Store(m.uploadTemp.Swap(0))
		m.downloadBlip.Store(m.downloadTemp.Swap(0))
	}
}

type Snapshot struct {
	DownloadTotal int64          `json:"downloadTotal"`
	UploadTotal   int64          `json:"uploadTotal"`
	Connections   []*TrackerInfo `json:"connections"`
	Memory        uint64         `json:"memory"`
}
