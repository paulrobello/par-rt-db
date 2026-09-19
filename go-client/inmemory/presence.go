// Presence rooms — the Go port of rust in_memory/presence.rs: a
// room → connection → member map with per-room subscribers and the ENH-015
// per-member state expiry. Two Clients sharing one PresenceRooms see each
// other's joins/updates/leaves.
package inmemory

import (
	"sync"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type presenceListener struct {
	alive    *syncFlag
	callback func([]wire.PresenceMember)
}

type presenceEntry struct {
	connID string
	member wire.PresenceMember
}

// PresenceRooms is the shared presence backing. The zero value is usable.
type PresenceRooms struct {
	mu      sync.Mutex
	members map[string][]presenceEntry
	subs    map[string][]presenceListener
	expiry  map[string]map[string]int64
	now     func() int64
}

// mu is a tiny alias keeping the struct fields honest without importing
// sync at every use site.
// NewPresenceRooms constructs a shared backing with the system clock.
func NewPresenceRooms() *PresenceRooms {
	return &PresenceRooms{
		members: map[string][]presenceEntry{},
		subs:    map[string][]presenceListener{},
		expiry:  map[string]map[string]int64{},
		now:     defaultNow,
	}
}

// Snapshot returns room's members in join order.
func (p *PresenceRooms) Snapshot(room string) []wire.PresenceMember {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries := p.members[room]
	out := make([]wire.PresenceMember, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.member)
	}
	return out
}

// Join adds or replaces member in room and fans out.
func (p *PresenceRooms) Join(room string, member wire.PresenceMember) {
	p.mu.Lock()
	entries := p.members[room]
	replaced := false
	for i := range entries {
		if entries[i].connID == member.ConnectionID {
			entries[i].member = member
			replaced = true
			break
		}
	}
	if !replaced {
		entries = append(entries, presenceEntry{connID: member.ConnectionID, member: member})
	}
	p.members[room] = entries
	p.fanOutLocked(room)
	p.mu.Unlock()
}

// Update sets connectionID's state in room (no-op for a non-member). A
// ttlMs > 0 schedules a state-nulling expiry at now+ttlMs; otherwise any
// pending expiry is cleared. The LIVE SERVER rejects ttlMs <= 0; this
// harness treats it as no expiry (the offline approximation the other
// engines pin).
func (p *PresenceRooms) Update(room, connectionID string, state wire.JSONValue, ttlMs *uint64, now int64) {
	p.mu.Lock()
	entries, ok := p.members[room]
	if !ok {
		p.mu.Unlock()
		return
	}
	found := false
	for i := range entries {
		if entries[i].connID == connectionID {
			entries[i].member.State = state
			found = true
			break
		}
	}
	if !found {
		p.mu.Unlock()
		return
	}
	exp := p.expiry[room]
	if exp == nil {
		exp = map[string]int64{}
		p.expiry[room] = exp
	}
	if ttlMs != nil && *ttlMs > 0 {
		exp[connectionID] = now + int64(*ttlMs)
	} else {
		delete(exp, connectionID)
	}
	p.fanOutLocked(room)
	p.mu.Unlock()
}

// Leave removes connectionID from room and fans out (no-op for a non-member).
func (p *PresenceRooms) Leave(room, connectionID string) {
	p.mu.Lock()
	entries, ok := p.members[room]
	if ok {
		out := entries[:0]
		for _, e := range entries {
			if e.connID != connectionID {
				out = append(out, e)
			}
		}
		p.members[room] = out
		if p.expiry[room] != nil {
			delete(p.expiry[room], connectionID)
		}
		if len(out) != len(entries) {
			p.fanOutLocked(room)
		}
	}
	p.mu.Unlock()
}

// Expire nulls the state of every member whose expiry has passed (the member
// stays listed) and reports whether anything expired.
func (p *PresenceRooms) Expire(now int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	any := false
	for room, exp := range p.expiry {
		for connID, at := range exp {
			if at > now {
				continue
			}
			entries := p.members[room]
			for i := range entries {
				if entries[i].connID == connID {
					entries[i].member.State = wire.Null{}
					any = true
				}
			}
			delete(exp, connID)
		}
	}
	return any
}

// Subscribe registers a room listener; the returned release func detaches it.
func (p *PresenceRooms) Subscribe(room string, callback func([]wire.PresenceMember)) func() {
	alive := &syncFlag{}
	alive.set(true)
	p.mu.Lock()
	p.subs[room] = append(p.subs[room], presenceListener{alive: alive, callback: callback})
	p.mu.Unlock()
	return func() { alive.set(false) }
}

// fanOutLocked delivers a fresh snapshot to every live listener, lazily
// compacting dead ones. Caller holds p.mu.
func (p *PresenceRooms) fanOutLocked(room string) {
	snapshot := func() []wire.PresenceMember {
		entries := p.members[room]
		out := make([]wire.PresenceMember, 0, len(entries))
		for _, e := range entries {
			out = append(out, e.member)
		}
		return out
	}()
	listeners := p.subs[room]
	live := listeners[:0]
	for _, l := range listeners {
		if !l.alive.get() {
			continue
		}
		live = append(live, l)
		l.callback(snapshot)
	}
	p.subs[room] = live
}
