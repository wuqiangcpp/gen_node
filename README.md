# gen_node — Gossip-based Distributed Node Library for Erlang

`gen_node` is a `gen_server`-like OTP behaviour for building distributed Erlang nodes with an embedded HyParView gossip protocol. Each instance acts as an independent gossip peer, maintaining its own membership table and epidemic communication channels.

## Features

- **gen_server interface** — familiar `call`/`cast`/`broadcast`/`gossip`/`ask` APIs
- **HyParView gossip** — active view (default 6) + passive view (default 100) + shuffle for diversity
- **Epidemic `ask`** — TTL-limited query with backtrack routing and seen dedup; initiator self-ask included
- **Async ask** — `ask_async/5` streams replies as messages, `ask_cancel/2` closes early, `{final, _}` ends it
- **Failure detection** — periodic ping/ack with suspect → promote/dead lifecycle
- **Fixed topology mode** — `gossip_enabled: false` disables gossip; `peers` option or `join/2` for direct topology
- **Global name registration** — optional `register_name/2` via Erlang `global`
- **Periodic timer API** — `send_interval/3` + `cancel_timer/2` with automatic cleanup on terminate
- **Process context** — `local_peers/0` reads active peers from process dictionary (injected at all callback dispatch sites)
- **Topology helpers** — `start_tree/5`, `start_line/4`, `start_grid/5` for rapid cluster setup
- **WS visualization** — bundled Cowboy WebSocket server and vis-network frontend
- **Node monitoring** — standalone `gen_node_monitor` for periodic health polling

## Dependencies

- Erlang/OTP 24+
- [Cowboy](https://github.com/ninenines/cowboy) 2.12.0 (optional, for WS server)
- [jsx](https://github.com/talentdeficit/jsx) 3.1.0 (optional, for WS server)

## Quick Start

```erlang
%% Define a callback module
-module(my_node).
-behaviour(gen_node).

init(_Args) -> {ok, #{}}.
handle_call(get_state, _From, State) -> {reply, State, State}.
handle_cast({msg, X}, State) -> {noreply, maps:put(last_msg, X, State)}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.

%% Start and connect
{ok, N1} = gen_node:start_link(my_node, [], []).
{ok, N2} = gen_node:start_link(my_node, [], []).
gen_node:join(N1, [N2]).
gen_node:broadcast(N1, {msg, "hello"}).
```

## Behaviour Callbacks

| Callback | Signature | Optional |
|---|---|---|
| `init/1` | `Args → {ok, State} \| {stop, Reason}` | no |
| `handle_call/3` | `(Request, From, State) → {reply, Reply, State} \| {noreply, State} \| {stop, ...}` | no |
| `handle_cast/2` | `(Request, State) → {noreply, State} \| {stop, ...}` | no |
| `handle_info/2` | `(Info, State) → {noreply, State} \| {stop, ...}` | no |
| `terminate/2` | `(Reason, State) → ok` | no |
| `handle_broadcast/3` | `(FromPid, Data, State) → {noreply, State} \| {stop, ...}` | **yes** |
| `handle_gossip/3` | `(FromPid, Data, State) → {noreply, State} \| {stop, ...}` | **yes** |
| `handle_ask/3` | `(FromPid, Query, State) → {reply, Reply, State} \| {noreply, State}` | **yes** |

Within any callback, `gen_node:local_peers/0` returns the current active peer list.

## Public API

### Lifecycle

```erlang
start_link(Module, Args, Options) -> {ok, pid()} | {error, term()}
start(Module, Args, Options)       -> {ok, pid()} | {error, term()}
stop(Pid)                          -> ok
```

### Messaging

```erlang
call(Pid, Request)              -> Reply
call(Pid, Request, Timeout)     -> Reply
cast(Pid, Request)              -> ok
broadcast(Pid, Data)            -> ok          %% direct to all peers
gossip(Pid, Data)               -> ok          %% epidemic spread
ask(Pid, Query, TTL, Timeout)   -> {ok, [{pid(), term()}]}
ask_async(Pid, Query, TTL, Timeout, NotifyPid) -> {ok, AskId}
ask_cancel(Pid, AskId)          -> ok
```

### Membership

```erlang
join(Pid, [Peers])  -> ok
leave(Pid)          -> ok
peers(Pid)          -> [pid()]
```

### Naming

```erlang
register_name(Name, Pid) -> ok | {error, {already_registered, pid()}}
whereis_name(Name)       -> pid() | undefined
```

### Timers

```erlang
send_interval(Pid, TimeMs, Msg) -> reference()  %% returns Ref for cancellation
cancel_timer(Pid, Ref)          -> ok
```

Internally uses `erlang:send_after` self-scheduling (no drift, no message pileup). Timer references are stored in the `timers` map and automatically cleaned up in `terminate/2`.

### Process Context

```erlang
local_peers() -> [pid()]
```

### Topology Helpers

```erlang
start_tree(M, L, Mod, Args, symmetric|asymmetric)       -> pid()       %% M-ary tree, depth L
start_tree(M, L, Mod, Args, symmetric|asymmetric, Opts)  -> pid()       %% with gen_node options
start_line(M, Mod, Args, symmetric|asymmetric)          -> [pid()]     %% chain of M nodes
start_line(M, Mod, Args, symmetric|asymmetric, Opts)     -> [pid()]     %% with gen_node options
start_grid(M, N, Mod, Args, symmetric|asymmetric)       -> [pid()]     %% M×N grid
start_grid(M, N, Mod, Args, symmetric|asymmetric, Opts)  -> [pid()]     %% with gen_node options
```

All use `gossip_enabled: false`. In `symmetric` mode, each node also `join`s back to its parents; `asymmetric` does not. The `Opts` argument is a proplist of `gen_node` options (e.g. `dead_timeout`).

## Configuration Options

Passed as a proplist to `start_link/3` / `start/3`:

| Option | Default | Description |
|---|---|---|
| `name` | `undefined` | Global name via `global:register_name/2` |
| `gossip_enabled` | `true` | Enable HyParView gossip protocol |
| `peers` | `[]` | Initial peers (sends `gossip_join` when gossip enabled) |
| `gossip_interval` | `1000` | Failure detection / gossip sync interval (ms) |
| `fanout` | `3` | Peers to forward gossip/ask to per hop |
| `ttl` | `6` | Max gossip/forward hops |
| `suspect_timeout` | `3000` | No-ping before marking suspect (ms) |
| `dead_timeout` | `6000` | No-ping before marking dead (ms) |
| `seen_ttl` | `60` | Seconds to keep seen message ids |
| `active_view_size` | `6` | Max active peers (HyParView) |
| `passive_view_size` | `100` | Max passive peers (HyParView) |
| `shuffle_interval` | `5000` | Passive view shuffle interval (ms) |
| `shuffle_fanout` | `4` | Passive peers to exchange per shuffle |
| `shuffle_ttl` | `3` | Shuffle forward TTL |

## Gossip Protocol

`gen_node` implements **HyParView** with:

- **Active view** — direct connections used for gossip sync, broadcast, ask forwarding, and failure detection
- **Passive view** — backup pool sampled for shuffle; promoted to active when slots open
- **Paper-compliant join** — `try_promote_evict/5` implements the HyParView join protocol: when active view is full, a random active peer is **evicted** (demoted to passive) to make room for the new node; the evicted peer receives a `disconnect` message and demotes the evictor from its own active view, ensuring symmetric connections
- **Forward join** — new node propagates via **random-walk** (not broadcast) through active view up to `shuffle_ttl` hops, avoiding message explosion
- **Gossip sync** — periodic pairwise active view exchange (random active peer each tick); uses `try_promote/5` (passive-only promotion, no eviction) for view maintenance stability
- **Shuffle** — periodic passive view subset exchange with a random active peer (forwarded up to `shuffle_ttl` hops)
- **Failure detection** — periodic ping/ack; suspect peers are re-pinged; dead peers removed and replaced from passive view

### Fixed Topology Mode

When `gossip_enabled: false`:
- No gossip, shuffle, or failure detection timers
- `join/2` and `peers` option add peers directly
- `{add_peer, FromPid}` messages from remote on join
- `broadcast`, `call`, `cast` still work

## Epidemic Ask

`gen_node:ask/4` implements a TTL-limited epidemic query:

1. Initiator evaluates `Mod:handle_ask` on self (reply included in results)
2. Forwards query to fanout-selected active peers with TTL decrement
3. Each peer replies via backtrack routing (`ask_routes` map)
4. Initiator collects all replies until timeout
5. Seen dedup prevents cycles

### Async ask

`ask/4` is a `gen_server:call`: the caller blocks until the timeout and only
then gets every reply collected so far. `ask_async/5` returns an `AskId`
immediately and delivers replies as they arrive:

    {gen_node_ask, AskId, FromPid, Reply}
    {gen_node_ask_done, AskId, timeout | final | cancelled}

- The initiator's own `handle_ask` answer is delivered first.
- A reply of the form `{final, Payload}` ends the ask early — useful when the
  target answers in chunks and knows which one is last. It is delivered like
  any other reply, followed by `{gen_node_ask_done, AskId, final}`.
- `ask_cancel/2` removes the timer and closes with `cancelled`; replies already
  in flight are dropped.
- Each hop forgets its `ask_routes` entry when the deadline carried by the
  query expires (plus a 5s grace for late replies), so long-lived asks do not
  leak routes. Senders of the older six-tuple `ask_forward` still work; their
  routes never expire.

## Neuron — Boltzmann Machine Application

`neuron.erl` is an example application built on `gen_node` that implements an **asynchronous Boltzmann machine** (Ising model).

### State

| Field | Description |
|---|---|
| `S` | Current state (0 or 1) |
| `Wj` | Weights to neighbor neurons (`#{pid() => float()}`) |
| `Pj` | Co-occurrence probability per neighbor |
| `β` (beta) | Temperature — higher = more random |
| `clamp` | Force S to fixed value (override sampling) |
| `update_weights` | Enable/disable Hebbian-like learning |
| `init_w` | Default weight for new neighbors |

### Behaviour

Each tick (configured via `interval`):
1. Compute energy `E = Σ Sj · Wj`
2. Sample S via logistic: `P(S=1) = 1 / (1 + e^{-β·E})`
3. If `clamp` is set, S is forced to that value
4. Broadcast S to neighbors via `gen_node:broadcast/2`
5. After `max_samples` ticks, update weights: `ΔW = r · (PD - OldP)` where `PD` is co-occurrence probability over the batch

### API

```erlang
neuron:set_s(Pid, 0|1)                        %% Set S directly
neuron:set_w(Pid, Peer, Weight)               %% Set weight to peer
neuron:get_state(Pid) -> {S, W, Pj}           %% Full state
neuron:get_s(Pid) -> S                        %% Current S
neuron:clamp(Pid, 0|1)                        %% Force S (training)
neuron:unclamp(Pid)                           %% Release clamp (free phase)
neuron:set_beta(Pid, Beta)                    %% Set temperature
neuron:set_beta_gossip(Pid, Beta)             %% Set temperature via gossip
neuron:set_update_weights(Pid, Bool)          %% Enable/disable learning
neuron:set_p(Pid, Peer, Pj)                   %% Restore Pj (for load/save)
```

### XOR Training

The included `test_neuron.erl` demonstrates 3-node XOR training using clamp/unclamp schedule with `update_weights` toggled via the WebSocket frontend.

## Tools

Generic Cowboy WebSocket + HTTP server. Serves static files from `client/` and dispatches WebSocket messages via a handler dictionary using **JSON-only** protocol.

Messages must be JSON: `{"cmd": "command", "value": ...}`. The `cmd` field matches a handler key; the optional `value` is appended to the handler's argument list.

```erlang
%% Start on default port 8081
Handlers = #{<<"get">> => {MyMod, my_fun, [Args]}},
gen_node_ws:start(Handlers).
gen_node_ws:start(Handlers, Port).

%% Stop
gen_node_ws:stop().
```

Handlers dict: `#{binary() => {M, F, A}}`. For a message `{"cmd":"set","value":V}`, the handler is called as `apply(M, F, A ++ [V])`. Reply is `jsx:encode(Result)`.

### gen_node_monitor

Standalone gen_server that periodically polls nodes for health.

```erlang
gen_node_monitor:start_link([{seeds, [SeedPid]}, {interval, 5000}]).

gen_node_monitor:add_node(Pid).
gen_node_monitor:remove_node(Pid).
gen_node_monitor:get_nodes()   -> [{pid(), #{status => up|down, peers => [...], ...}}]
gen_node_monitor:get_node(Pid) -> {ok, map()} | {error, not_found}
```

Options: `interval` (default 5000ms), `request` (default `get_state`), `timeout` (default 3000ms), `auto_discover` (default `true`), `seeds`.

### Frontend

Two vis-network HTML pages are provided in `client/`:

- **`show.html`** — generic topology viewer with physics stabilization (`network.once("stabilized", ...)`)
- **`show_neuron.html`** — Boltzmann machine visualizer with S-color-coded nodes (S=1 red, S=0 blue), edge labels (weight + Pj), Beta slider, Start/Stop training buttons, and input toggle buttons

Both consume WebSocket messages in format:

```json
{"nodes": [{"id": "...", "props": {}}], "links": [{"from": "...", "to": "...", "props": {}}], "info": "..."}
```

Arrow direction controlled by `(directed)` / `(undirected)` in the info string. Physics is enabled once on new node arrival then disabled after stabilization.

## Build

```bash
rebar3 compile
```

## License

MIT
