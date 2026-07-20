-module(test).
-behaviour(gen_node).

-export([init/1, handle_call/3, handle_cast/2,
         handle_broadcast/3, handle_gossip/3, handle_ask/3,
         handle_info/2, terminate/2]).
-export([test/0, test_gossip_disabled/0,  discover_topology/2, monitor_topology/0]).

-record(state, {
}).

init(_Args) -> {ok, #state{}}.

handle_call(get_state, _From, State) -> {reply, State, State};
handle_call(_Req, _From, State) -> {reply, ok, State}.

handle_cast(_Req, State) -> {noreply, State}.

handle_broadcast(_From, _Data, State) ->
    {noreply, State}.

handle_gossip(_From, _Data, State) ->
    {noreply, State}.

handle_ask(_From, get_peers, State) ->
    {reply, gen_node:local_peers(), State};
handle_ask(_From, _Query, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.


test() ->
    io:format("=== gen_node gossip test ===~n~n"),
    {ok, Seed} = gen_node:start_link(?MODULE, [], [{active_view_size,4}]),
    gen_node:register_name(seed, Seed),
    io:format("Seed: ~p~n", [Seed]),

    Handlers = #{<<"get">> => {?MODULE, discover_topology, [[Seed], true]}},
    gen_node_ws:start(Handlers, 8081),
    io:format("WS server: http://localhost:8081/show.html~n"),

    lists:foreach(fun(I) ->
        timer:sleep(5000),
        {ok, P} = gen_node:start_link(?MODULE, [], [{active_view_size,4}]),
        gen_node:join(P, [Seed]),
        io:format("Node ~p/10: ~p joined seed~n", [I, P])
    end, lists:seq(1, 10-1)),

    timer:sleep(3000),
    Topo = discover_topology([Seed], false),
    io:format("Final topology: ~p nodes, ~p links~n",
              [length(maps:get(nodes, Topo)), length(maps:get(links, Topo))]),
    ok.

test_gossip_disabled() ->
    io:format("~n=== gen_node gossip_disabled tests ===~n~n"),
    Pids = gen_node:start_grid(3, 3, test, [], asymmetric),
    io:format("~p~n",[Pids]),
    [Pid|_]=Pids,
    timer:sleep(100),
    Topo = discover_topology([Pid], true),
    io:format("Nodes: ~p~nLinks: ~p~n",
              [length(maps:get(nodes, Topo)), length(maps:get(links, Topo))]),
    Handlers = #{<<"get">> => {?MODULE, discover_topology, [[Pid], true]}},
    gen_node_ws:start(Handlers, 8081),
    ok.


%% ===================================================================
%% Topology discovery (use ask to discover all reachable nodes)
%% ===================================================================

discover_topology(Seeds, Directed) ->
    PeerMap = aggregate_peers(Seeds, #{}),
    Nodes = [#{id => pid_to_binary(P), props => #{}}
             || P <- maps:keys(PeerMap)],
    Links = case Directed of
        true  -> directed_links(PeerMap);
        false -> undirected_links(PeerMap)
    end,
    NC = length(Nodes),
    LC = length(Links),
    Mode = case Directed of true -> "directed"; false -> "undirected" end,
    Info = list_to_binary(io_lib:format("~p nodes, ~p links (~s)", [NC, LC, Mode])),
    #{nodes => Nodes, links => Links, info => Info}.

directed_links(PeerMap) ->
    [#{from => pid_to_binary(P), to => pid_to_binary(Q), props => #{}}
     || {P, Peers} <- maps:to_list(PeerMap), Q <- Peers, Q =/= P].

undirected_links(PeerMap) ->
    Pairs = [{P, Q} || {P, Peers} <- maps:to_list(PeerMap), Q <- Peers, Q =/= P],
    DedupSet = lists:foldl(
        fun({P, Q}, S) ->
            Key = if P < Q -> {P, Q}; true -> {Q, P} end,
            sets:add_element(Key, S)
        end, sets:new(), Pairs),
    [#{from => pid_to_binary(P1), to => pid_to_binary(P2), props => #{}}
     || {P1, P2} <- sets:to_list(DedupSet)].

aggregate_peers([Seed | Rest], Acc) ->
    Peers = case gen_node:ask(Seed, get_peers, 20, 1000) of
        {ok, Replies} -> Replies;
        _ -> [{Seed, gen_node:peers(Seed)}]
    end,
    Acc2 = lists:foldl(fun({P, Ps}, M) ->
        maps:put(P, Ps, M)
    end, Acc, Peers),
    aggregate_peers(Rest, Acc2);
aggregate_peers([], Acc) -> Acc.

pid_to_binary(Pid) ->
    list_to_binary(pid_to_list(Pid)).

%% ===================================================================
%% Monitor topology (data source: gen_node_monitor)
%% ===================================================================

monitor_topology() ->
    case catch gen_node_monitor:get_nodes() of
        {'EXIT', _} -> #{error => <<"monitor not started">>};
        Nodes when is_list(Nodes) -> format_monitor(Nodes)
    end.

format_monitor(Nodes) ->
    NodeList = [#{id => pid_to_binary(P), props => #{
        status => atom_to_binary(Status, utf8),
        peers => length(Peers)
    }} || {P, #{status := Status, peers := Peers}} <- Nodes],
    LinkList = lists:flatmap(
        fun({P, #{peers := Peers}}) ->
            [#{from => pid_to_binary(P), to => pid_to_binary(Q), props => #{}}
             || Q <- Peers]
        end, Nodes),
    Up = length([1 || {_, #{status := up}} <- Nodes]),
    Down = length([1 || {_, #{status := down}} <- Nodes]),
    Info = list_to_binary(io_lib:format("~p up, ~p down (directed)", [Up, Down])),
    #{nodes => NodeList, links => LinkList, info => Info}.
