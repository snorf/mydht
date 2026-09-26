# MyDHT

A small distributed hash table in Python. It has no dependencies, you talk to it with `curl`, and the code is short enough to read in one sitting.

```sh
python -m mydht cluster --nodes 5          # five nodes on ports 50140-50144
curl -T photo.jpg localhost:50140/keys/photo.jpg
curl -o copy.jpg  localhost:50143/keys/photo.jpg
open http://localhost:50141/               # status page for one node
```

Any node accepts any request. Each key is stored on 3 nodes, and you can add or remove nodes while the cluster is running.

---

## Then and now

### 2011

I wrote MyDHT in March 2011 as a coding test for Spotify. It ran on Python 2.6 and did the following:

- It used a **consistent hash ring** to decide which nodes own a key. The ring was adapted from [a blog post by amix](http://amix.dk/blog/post/19367).
- It **replicated** every key to the next N nodes on the ring.
- Nodes could **join and leave** at runtime. A node that received SIGINT handed its data over before it quit, and a crashed node could be removed by hand.
- **Load balancing** compared timestamps between replicas and copied the newest version around.
- Every node served a small **HTML status page**, so you could browse the cluster from a web browser.

The ideas were fine. They are the same ones Dynamo and Cassandra are built on. The execution was a few evenings' work, and it showed:

- **A home-made wire protocol.** Every message started with a 4 KB header of fields separated by ASCII 30 and padded with `"0"` characters. A second padded 4 KB block carried the response length. To make the status page work, the server also detected browsers by checking whether the first bytes were `GET /`.
- **A home-made client** (`mydhtclient.py -c put -k key -val value`) was the only way to talk to it.
- **No consistency model.** Writes went to every replica and the server returned whichever status came back last. A replica always accepted an incoming write, even one older than what it already had. Deletes removed the key outright, so the next load balance could copy it back from a replica that missed the delete.
- **Load balancing was chatty.** It sent one `HASKEY` round trip per key per replica.
- **Fragile concurrency.** It started a raw `thread.start_new_thread` for every connection and used locks without `try/finally`. One code path could return a variable that had never been assigned.
- **The tests were scripts**, not tests. They needed servers already running and files in `upload/`.
- It is **Python 2** (`xrange`, `long`, `print` statements, `except X, e`), so it no longer runs on a current Python.

The original code is kept unchanged in [`legacy/`](legacy/), together with its [README](legacy/README).

### 2026

This is a rewrite for Python 3.10+ that keeps the 2011 design and fixes how it was built:

| | 2011 | Now |
|---|---|---|
| Protocol | Custom socket protocol with padded 4 KB headers | Plain HTTP |
| Client | `mydhtclient.py` | `curl`, a browser, or any HTTP library |
| Hash ring | Linear scan, 3 virtual nodes per server | `bisect`, 64 virtual nodes per server, always N distinct replicas |
| Writes | Sent to all replicas, no success criterion | Sent to all replicas in parallel, succeed when a majority store the value |
| Reads | First replica that had the key | Ask all replicas, return the newest version, repair stale replicas in the background |
| Conflicts | A replica accepted any incoming write | Last write wins by timestamp on every replica |
| Deletes | Key removed, could come back | Tombstones, so a delete survives anti-entropy |
| Load balancing | One `HASKEY` round trip per key and replica | Each node fetches one digest per peer, then pulls and pushes only what differs |
| Purge | Dropped keys without checking | Drops a key only when every current replica has a version at least as new |
| Storage | Memory | Memory, or an SQLite file per node with `--data-dir` |
| Running as a service | Background shell jobs from `createservers.py` | systemd unit and install script in [`contrib/`](contrib/) |
| Tests | Manual scripts | 33 unit and end-to-end tests with real nodes on ephemeral ports |
| Dependencies | None | Still none, only the standard library |

---

## Running it

You need Python 3.10 or later. The package has no dependencies, so running it from a checkout works:

```sh
python -m mydht serve                                  # first node, localhost:50140
python -m mydht serve --port 50141 --join localhost:50140
python -m mydht serve --port 50142 --join localhost:50140
```

`pip install .` also works and gives you a `mydht` command.

- `--host` is the name other nodes use to reach this node. Across machines, set it to something the other nodes can resolve. Use `--bind 0.0.0.0` if the node should listen on a different address than `--host`.
- `--replicas` sets how many copies each key gets (default 3). Only the first node's setting counts; nodes that join use the cluster's value.
- `--data-dir DIR` keeps the node's data in an SQLite file in `DIR`, so it survives restarts. Without it, everything is kept in memory.
- `--join` takes one node or a comma-separated list. The node skips itself, joins the first one that answers, and if none answers it starts alone and retries every 10 seconds. That means every node can get the same list and the nodes can start in any order.
- **Ctrl-C** (SIGINT or SIGTERM) makes a node leave cleanly: it leaves the ring and hands its keys to their new replicas.
- `python -m mydht cluster --nodes 5` runs several nodes in one process, which is handy for trying things out.

### As a systemd service

[`contrib/`](contrib/) has what you need to run one node per machine, for example one per host or container in a small home lab. On each machine:

```sh
git clone https://github.com/snorf/mydht && cd mydht
sudo ./contrib/install.sh           # virtualenv in /opt/mydht, unit, /etc/default/mydht
sudoedit /etc/default/mydht         # set MYDHT_HOST to this machine's address
sudo systemctl enable --now mydht
journalctl -u mydht -f
```

- **Settings:** `/etc/default/mydht` sets the host, port and bind address, and `MYDHT_JOIN` lists every node in the cluster. The same `MYDHT_JOIN` line works on all machines.
- **Data:** stored in `/var/lib/mydht`. The service runs as a dynamic user that can write only there.
- **Stopping:** `systemctl stop` sends SIGTERM, so the node hands over its keys before it exits. `systemctl restart` rejoins the ring and pulls any changes the node missed while it was down.
- **Upgrading:** pull the new code, run `install.sh` again and restart the service.

### Keeping a file in sync on every node

`contrib/mydht-sync` keeps a local file identical on every machine. For example, it can share one `hosts.conf` between three DNS servers. `install.sh` installs the script together with a timer, but doesn't enable the timer. Every minute the timer does the following:

1. **Pull.** It asks the DHT for the key with `If-None-Match`. The answer is usually `304`, and then nothing happens. If there is a new version, it replaces the target file atomically and runs your reload command.
2. **Publish (optional).** If `MYDHT_SYNC_UPSTREAM` is set, the node fetches that URL (for example a raw file on GitLab) and uploads it if it differs from the DHT copy. It only does this in its own turn: with `MYDHT_SYNC_SLOTS=3` and `MYDHT_SYNC_INTERVAL=5`, one node polls upstream every 5 minutes and each node every 15. If a node is down, only its turns are skipped. An empty or failed upstream response is never published.

```sh
sudoedit /etc/default/mydht-sync     # key, target file, reload command, upstream, slot
sudo systemctl enable --now mydht-sync.timer
journalctl -u mydht-sync -f
```

You can also push a new version by hand from any machine. It reaches every node within a minute:

```sh
curl -T hosts.conf localhost:50140/keys/nextdns/hosts.conf
```

## Using it with curl

```sh
# Store a value, from a string, a file or stdin
curl -X PUT --data-binary 'hello' localhost:50140/keys/greeting
curl -T report.pdf localhost:50140/keys/docs/report.pdf
tar c src | curl -T - localhost:50140/keys/backup.tar

# Read it from any node
curl localhost:50142/keys/greeting
curl -I localhost:50142/keys/docs/report.pdf     # headers only

# Delete it
curl -X DELETE localhost:50141/keys/greeting

# Inspect the cluster
curl localhost:50140/ring                        # members and replica count
curl localhost:50140/keys                        # what this node stores
curl localhost:50140/whereis/docs/report.pdf     # which nodes should hold a key
```

Keys can contain slashes and spaces; URL-encode anything else. If you send a `Content-Type` header with a value, reads return it. If you don't, the content type is guessed from the key, so `photo.jpg` is served as `image/jpeg`.

As in 2011, if you store a key named `favicon.ico`, every node serves it as its browser icon:

```sh
curl -T legacy/favicon.ico localhost:50140/keys/favicon.ico
```

### API

| Request | What it does |
|---|---|
| `GET /` | HTML status page for this node |
| `PUT /keys/{key}` | Store the request body. Returns `201`, or `503` if a majority of replicas couldn't be reached. |
| `GET` / `HEAD /keys/{key}` | Newest value across the replicas. Returns `404` if the key doesn't exist and `503` without a majority. Responses carry `ETag` and `Last-Modified`. With `If-None-Match` or `If-Modified-Since`, the node answers `304 Not Modified` when you already have the newest version. |
| `DELETE /keys/{key}` | Delete the key (writes a tombstone) |
| `GET /keys` | Keys stored on this node, as JSON |
| `GET /ring` | Ring members and replica count |
| `GET /whereis/{key}` | The nodes responsible for a key |
| `POST /balance` | Run anti-entropy on every node |
| `POST /purge` | Drop keys this node no longer owns, if they are safely stored elsewhere |
| `DELETE /ring/{host:port}` | Remove a crashed node from the ring and re-replicate its keys |

Nodes talk to each other through endpoints under `/internal/`. They are not meant for clients.

---

## How it works

**The ring.** Each node is hashed with MD5 to 64 points on a ring. A key is hashed the same way. Its replicas are the first N distinct nodes found walking clockwise from the key's position. When a node joins or leaves, only the keys next to its points move. See [`mydht/hashring.py`](mydht/hashring.py).

**Writes.** The node that receives a `PUT` or `DELETE` becomes the coordinator for that request. It stamps the value with the current time in nanoseconds and sends it to all N replicas in parallel. The write succeeds once a majority (2 of 3) have stored it. A write that fails the majority can still be stored on some replicas, because nothing is rolled back.

**Reads.** The coordinator sends a `HEAD` to every replica. It needs answers from a majority, then fetches the value from the replica with the newest version. Replicas that turn out to be behind are sent that version in the background, which is called read repair.

**Conflicts.** The version with the highest timestamp wins on every replica. On an exact tie, a delete wins. Deletes are stored as tombstones, so a replica that missed the delete cannot copy the old value back.

**Anti-entropy** (`POST /balance`). Each node fetches a digest of `{key: timestamp}` from every peer. For each key it is a replica of, it pulls a newer version if a peer has one. Then it pushes its own version to every replica that is behind. This runs automatically when a node joins or leaves, or when a crashed node is removed.

**Membership.**
- **Join:** a new node asks any existing node to join. That node tells the rest of the ring and returns the member list. The newcomer then triggers a balance so it receives the keys it now owns. A node restarted with `--data-dir` does the same: its old data comes back from disk, and the balance pulls newer versions and tombstones from the others while pushing anything the others lack.
- **Leave:** a node that shuts down cleanly first removes itself everywhere, then pushes its keys to their new owners.
- **Crash:** the cluster keeps working as long as a majority of each key's replicas is up. `DELETE /ring/{node}` removes the dead node for good and rebuilds the missing copies.

**Purge.** Nodes never delete data on their own after the ring changes. `POST /purge` removes the keys a node no longer owns, but only those that every current owner already holds in a version at least as new.

## Limitations

This is still a toy, just a better-built one. Don't put data you care about in it.

- **Memory by default.** Without `--data-dir` nothing is written to disk, and if every replica of a key goes down, the key is lost. With `--data-dir`, SQLite runs in WAL mode with `synchronous=NORMAL`, so a power cut can lose the last few writes on a node. The other replicas normally still have them.
- **Split brain at startup.** A node that cannot reach any of its `--join` nodes starts a ring of its own. If the network is split when the nodes boot, you get two rings, which accept writes separately until they reach each other again. A node that is alone keeps retrying and merges its ring with the others, and the newest version of each key wins. With three nodes one side of a split is always a single node, so the rings always merge. With more nodes, two rings of several nodes each stay apart until you restart one side.
- **Wall-clock timestamps.** "Last write wins" trusts the clocks on the nodes. If the clocks drift, a newer write can lose to an older one. Real systems use vector clocks or hybrid logical clocks for this.
- **Tombstones are kept forever.** They are never garbage-collected.
- **Membership is not consensus.** Nodes that join or leave at the same moment can leave members with different views of the ring for a while. Anti-entropy eventually fixes the data, but nothing guarantees the members agree on the ring.
- **Whole values in memory.** Every value is buffered in memory, and every node-to-node call opens a new connection.
- **No authentication or TLS.** Anyone who can reach a port can read, write, or remove nodes. Only run it on a network you trust.

## Tests

```sh
python -m unittest -v
```

The tests start real nodes on ephemeral ports and exercise the following:

- replication
- quorum failures
- joins, clean leaves and crashes
- read repair
- tombstones surviving anti-entropy
- safe purge
- chunked uploads (`curl -T -`)
- restarts with `--data-dir`, including a whole cluster starting in any order
- conditional GETs, and `contrib/mydht-sync` against a real cluster (needs `curl`)
