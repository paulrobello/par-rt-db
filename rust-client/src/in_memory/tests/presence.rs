use super::*;

// ---- presence ----------------------------------------------------------
//
// Ports the presence surface of `ts-client/src/in_memory.ts:1217-1285`.
// A private PresenceRooms sees only self; a shared backing lets two clients
// see each other's joins/updates/leaves — approximating the server's
// per-connection registry for tests.

fn new_presence_client(conn: &str, rooms: Arc<Mutex<PresenceRooms>>) -> InMemoryRtDbClient {
    InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .connection_id(conn)
            .presence_user(AuthedUser {
                kind: crate::wire::UserKind::User,
                email: Some(format!("{conn}@x.com")),
                name: None,
                github_login: None,
                github_id: None,
            })
            .presence_rooms(rooms),
    )
}

#[tokio::test]
async fn presence_join_fires_initial_snapshot_with_self() {
    // Brief: join a room; callback fires immediately with a one-member
    // snapshot (the joining connection itself).
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().connection_id("c1"));
    let snaps: Arc<Mutex<Vec<Vec<PresenceMember>>>> = Arc::new(Mutex::new(Vec::new()));
    let snaps_clone = snaps.clone();
    let _h = c.presence("doc:1", Some(json!({"cursor": 5})), move |members| {
        snaps_clone.lock().unwrap().push(members);
    });
    let got = snaps.lock().unwrap();
    assert_eq!(got.len(), 1, "initial snapshot delivered on join");
    assert_eq!(got[0].len(), 1);
    assert_eq!(got[0][0].connection_id, "c1");
    assert_eq!(got[0][0].state, json!({"cursor": 5}));
}

#[tokio::test]
async fn presence_update_broadcasts_new_state() {
    // Brief: update_presence fans out a fresh snapshot with the new state.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().connection_id("c1"));
    let snaps: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let snaps_clone = snaps.clone();
    let _h = c.presence("room", None, move |members| {
        if let Some(m) = members.first() {
            snaps_clone.lock().unwrap().push(m.state.clone());
        }
    });
    c.update_presence("room", json!({"typing": true}), None);
    let got = snaps.lock().unwrap();
    assert_eq!(got.len(), 2, "initial + update");
    assert_eq!(got[1], json!({"typing": true}));
}

#[tokio::test]
async fn presence_update_noop_for_unjoined_room() {
    // Brief: update_presence on a room we haven't joined does nothing.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let snaps: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let snaps_clone = snaps.clone();
    let _h = c.presence("room", None, move |members| {
        snaps_clone.lock().unwrap().push(members.len());
    });
    // Update a different room — no fan-out for "room".
    c.update_presence("other", json!({}), None);
    assert_eq!(snaps.lock().unwrap().len(), 1, "no new snapshot");
}

#[tokio::test]
async fn presence_leave_removes_member_and_drops_listeners() {
    // Brief: leave_presence removes the member and fans out; further updates
    // to the room from a peer do not invoke the (now-dropped) callback.
    let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
    let mut c1 = new_presence_client("c1", rooms.clone());
    let mut c2 = new_presence_client("c2", rooms.clone());

    let c1_snaps: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let c1_snaps_clone = c1_snaps.clone();
    let h1 = c1.presence("room", None, move |members| {
        c1_snaps_clone.lock().unwrap().push(members.len());
    });

    // c2 joins → c1 sees 2 members.
    let _h2 = c2.presence("room", None, |_| {});
    assert_eq!(*c1_snaps.lock().unwrap(), [1, 2]);

    // c1 leaves → its listener is dropped; the fan-out goes to remaining
    // listeners only. h1 is now inert.
    c1.leave_presence("room");
    drop(h1);

    // c2 updates — c1's callback must not fire (listener dropped).
    c2.update_presence("room", json!({"x": 1}), None);
    assert_eq!(
        *c1_snaps.lock().unwrap(),
        [1, 2],
        "no further fire after leave"
    );
}

#[tokio::test]
async fn presence_two_clients_on_shared_rooms_see_each_other() {
    // Brief: two clients sharing a PresenceRooms instance see each other's
    // joins and leaves — approximating the server's per-db registry.
    let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
    let mut c1 = new_presence_client("c1", rooms.clone());
    let mut c2 = new_presence_client("c2", rooms.clone());

    let c1_snaps: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let c1_snaps_clone = c1_snaps.clone();
    let _h1 = c1.presence("room", None, move |members| {
        let ids: Vec<String> = members.into_iter().map(|m| m.connection_id).collect();
        c1_snaps_clone.lock().unwrap().push(ids);
    });

    // c2 joins → c1 sees [c1, c2].
    let _h2 = c2.presence("room", None, |_| {});
    {
        let got = c1_snaps.lock().unwrap();
        assert_eq!(got.len(), 2, "initial self + c2 join");
        assert_eq!(got[1], ["c1", "c2"]);
    }

    // c2 leaves → c1 sees [c1] again.
    c2.leave_presence("room");
    {
        let got = c1_snaps.lock().unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[2], ["c1"]);
    }
}

// ---- presence ttl (ENH-015) ------------------------------------------
//
// Mirrors `PresenceRooms.expire` + `update(..., ttlMs, now)` in
// `ts-client/src/in_memory.ts`: a refresh with a ttl schedules an expiry
// sweep that nulls this member's `state` to Value::Null at `now + ttl`
// (the member stays listed); a refresh with no ttl clears any pending
// expiry. Mirrors the live server's `expire_once`.
//
// These tests drive `PresenceRooms` directly with controlled `now` values
// (the harness's `update`/`expire` take `now` explicitly) so the expiry
// math is deterministic without relying on the client's injected clock.
// The client-surface helper is covered separately below.

fn presence_member(conn: &str, state: Value) -> PresenceMember {
    PresenceMember {
        connection_id: conn.to_string(),
        user: AuthedUser {
            kind: crate::wire::UserKind::User,
            email: Some(format!("{conn}@x.com")),
            name: None,
            github_login: None,
            github_id: None,
        },
        state,
    }
}

#[tokio::test]
async fn presence_ttl_expires_state_to_null_member_stays() {
    // Brief: c1 and c2 share a PresenceRooms. c1 updates with ttl_ms = 1000
    // at t = 5000. At t = 5999 nothing has expired. At t = 6000+ the sweep
    // nulls c1's state, c2 observes the null, c1 is still a member.
    let mut rooms = PresenceRooms::default();

    let c2_states: Arc<Mutex<Vec<(Value, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let c2_states_clone = c2_states.clone();
    let _h2 = rooms.subscribe("room", move |members| {
        if let Some(c1) = members.iter().find(|m| m.connection_id == "c1") {
            c2_states_clone
                .lock()
                .unwrap()
                .push((c1.state.clone(), c1.connection_id.clone()));
        }
    });

    // c1 joins, then refreshes with a ttl at t = 5000.
    rooms.join("room", presence_member("c1", Value::Null));
    rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
    {
        let got = c2_states.lock().unwrap();
        // Two observations of c1's state so far: c1 join (null), c1 update
        // (typing). (c2 has no presence entry — it only subscribes.)
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].0, json!({"typing": true}));
    }

    // Before expiry: no change, expire returns false.
    assert!(!rooms.expire(5999));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.len(), 2, "no fire before expiry");
    }

    // At/after expiry: state → null, member stays, expire returns true.
    assert!(rooms.expire(6000));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.len(), 3, "one fire on expiry");
        assert_eq!(got[2].0, Value::Null, "state cleared to null");
        assert_eq!(got[2].1, "c1", "member stays in the room");
    }
    let snap = rooms.snapshot("room");
    assert_eq!(snap.len(), 1, "member stays listed after expiry");
    assert_eq!(snap[0].state, Value::Null);

    // Idempotent: a second sweep at the same instant is a no-op.
    assert!(!rooms.expire(6000));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.len(), 3, "no further fire");
    }
}

#[tokio::test]
async fn presence_ttl_refresh_without_ttl_clears_expiry() {
    // Brief: a refresh with ttl_ms = None clears any pending expiry — the
    // state persists past the original expiry instant.
    let mut rooms = PresenceRooms::default();
    rooms.join("room", presence_member("c1", Value::Null));
    rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
    rooms.update("room", "c1", json!({"typing": false}), None, 5500);
    // Past the original expiry instant — no expiry, state persists.
    assert!(!rooms.expire(10_000));
    let snap = rooms.snapshot("room");
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].state, json!({"typing": false}));
}

#[tokio::test]
async fn presence_ttl_leave_clears_expiry_entry() {
    // Brief: leaving clears the expiry entry, so a re-join with the same
    // connectionId does not inherit a stale ttl.
    let mut rooms = PresenceRooms::default();
    rooms.join("room", presence_member("c1", Value::Null));
    rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
    rooms.leave("room", "c1");
    // After leave, the expiry map should be empty (no fire, no panic).
    assert!(!rooms.expire(10_000));
    // And re-join with the same connId does not carry the old ttl.
    rooms.join("room", presence_member("c1", json!({"fresh": true})));
    assert!(!rooms.expire(10_000));
    let snap = rooms.snapshot("room");
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].state, json!({"fresh": true}));
}

#[tokio::test]
async fn presence_ttl_client_expire_presence_helper() {
    // Brief: the client's `expire_presence(now)` helper drives the same
    // sweep through the client's injected clock, mirroring `tick` for the
    // document reaper. Two clients on shared rooms; one updates with a
    // short ttl; the other observes the null at expiry.
    let t: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));
    let t_clone = t.clone();
    let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
    let make = |conn: &'static str| {
        let t = t_clone.clone();
        let rooms = rooms.clone();
        InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .connection_id(conn)
                .now(move || *t.lock().unwrap())
                .presence_user(AuthedUser {
                    kind: crate::wire::UserKind::User,
                    email: Some(format!("{conn}@x.com")),
                    name: None,
                    github_login: None,
                    github_id: None,
                })
                .presence_rooms(rooms),
        )
    };
    let mut c1 = make("c1");
    let mut c2 = make("c2");

    let c2_states: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let c2_states_clone = c2_states.clone();
    let _h2 = c2.presence("room", None, move |members| {
        if let Some(c1) = members.iter().find(|m| m.connection_id == "c1") {
            c2_states_clone.lock().unwrap().push(c1.state.clone());
        }
    });

    let _h1 = c1.presence("room", None, |_| {});

    // Advance the clock to t = 5000 and refresh c1 with a 1000ms ttl.
    *t.lock().unwrap() = 5000;
    c1.update_presence("room", json!({"typing": true}), Some(1000));

    // Before expiry: helper returns false, no new observation.
    assert!(!c2.expire_presence(Some(5999)));
    {
        let got = c2_states.lock().unwrap();
        assert!(got.len() >= 2);
        assert_eq!(got.last().unwrap(), &json!({"typing": true}));
    }

    // After expiry: helper returns true, c2 observes the null.
    assert!(c2.expire_presence(Some(6000)));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.last().unwrap(), &Value::Null);
    }
}
