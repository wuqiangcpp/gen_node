-ifndef(GEN_NODE_HRL).
-define(GEN_NODE_HRL, true).

-type peer_status() :: alive | suspect | dead.

-record(peer_info, {
    status :: peer_status(),
    heartbeat :: non_neg_integer(),
    incarnation :: non_neg_integer(),
    last_seen :: non_neg_integer()
}).

-record(gen_node_state, {
    mod :: module(),
    mod_state :: term(),
    name :: term() | undefined,
    active_peers :: #{pid() => #peer_info{}},
    passive_peers :: #{pid() => #peer_info{}},
    seen :: #{term() => non_neg_integer()},
    asks :: map(),
    ask_routes :: map(),
    gossip_timer :: reference() | undefined,
    shuffle_timer :: reference() | undefined,
    gossip_interval :: pos_integer(),
    fanout :: pos_integer(),
    ttl :: pos_integer(),
    active_view_size :: pos_integer(),
    passive_view_size :: pos_integer(),
    shuffle_interval :: pos_integer(),
    shuffle_fanout :: pos_integer(),
    shuffle_ttl :: pos_integer(),
    gossip_enabled :: boolean(),
    suspect_timeout :: non_neg_integer(),
    dead_timeout :: non_neg_integer(),
    seen_ttl :: non_neg_integer()
}).

-endif.
