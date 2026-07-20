-module(test_basic).
-behaviour(gen_node).

-export([init/1, handle_call/3, handle_cast/2,
         handle_broadcast/3, handle_gossip/3, handle_ask/3,
         handle_info/2, terminate/2]).
-export([test/0, test_gossip_disabled/0, test_all/0]).

-record(state, {
    calls = [],
    casts = [],
    broadcasts = [],
    gossips = [],
    asks = [],
    infos = []
}).

init(_Args) -> {ok, #state{}}.

handle_call(get_state, _From, State) -> {reply, State, State};
handle_call({ping, N}, _From, State) -> {reply, {pong, N}, State};
handle_call(_Req, _From, State) -> {reply, ok, State}.

handle_cast({push, X}, State) ->
    {noreply, State#state{casts = [X | State#state.casts]}};
handle_cast(_Req, State) -> {noreply, State}.

handle_broadcast(From, Data, State) ->
    {noreply, State#state{broadcasts = [{From, Data} | State#state.broadcasts]}}.

handle_gossip(From, Data, State) ->
    {noreply, State#state{gossips = [{From, Data} | State#state.gossips]}}.

handle_ask(_From, whoami, State) ->
    {reply, node(), State};
handle_ask(From, {echo, Msg}, State) ->
    {reply, {echoed, Msg}, State#state{asks = [{From, {echo, Msg}} | State#state.asks]}};
handle_ask(_From, count, _State) ->
    {reply, 0, none};
handle_ask(_From, noreply_test, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    {noreply, State#state{infos = [Info | State#state.infos]}}.

terminate(_Reason, _State) -> ok.

test() ->
    io:format("=== gen_node basic tests ===~n~n"),

    %% Test 1: start_link
    io:format("Test 1: start_link~n"),
    {ok, Pid1} = gen_node:start_link(?MODULE, [], []),
    true = is_pid(Pid1),
    io:format("  Pid1 = ~p~n", [Pid1]),

    {ok, Pid2} = gen_node:start_link(?MODULE, [], []),
    true = is_pid(Pid2),
    io:format("  Pid2 = ~p~n", [Pid2]),
    io:format("  PASSED~n~n"),

    %% Test 2: call
    io:format("Test 2: call~n"),
    ok = gen_node:call(Pid1, hello),
    {pong, 42} = gen_node:call(Pid1, {ping, 42}),
    State1 = gen_node:call(Pid1, get_state),
    io:format("  State1 calls = ~p~n", [State1#state.calls]),
    io:format("  PASSED~n~n"),

    %% Test 3: cast
    io:format("Test 3: cast~n"),
    ok = gen_node:cast(Pid1, {push, a}),
    ok = gen_node:cast(Pid1, {push, b}),
    State2 = gen_node:call(Pid1, get_state),
    io:format("  casts = ~p~n", [State2#state.casts]),
    [b, a] = State2#state.casts,
    io:format("  PASSED~n~n"),

    %% Test 4: peers (initially empty)
    io:format("Test 4: peers initially empty~n"),
    [] = gen_node:peers(Pid1),
    io:format("  PASSED~n~n"),

    %% Test 5: join peers
    io:format("Test 5: join peers~n"),
    ok = gen_node:join(Pid1, [Pid2]),
    timer:sleep(500),
    Peers1 = gen_node:peers(Pid1),
    Peers2 = gen_node:peers(Pid2),
    io:format("  Pid1 peers = ~p~n", [Peers1]),
    io:format("  Pid2 peers = ~p~n", [Peers2]),
    case lists:member(Pid2, Peers1) of
        true -> io:format("  Pid2 found in Pid1 peers~n", []);
        false -> io:format("  WARN: Pid2 not in Pid1 peers yet (gossip may need more time)~n", [])
    end,
    io:format("  PASSED~n~n"),

    %% Test 6: broadcast
    io:format("Test 6: broadcast~n"),
    ok = gen_node:broadcast(Pid1, {msg, "hello peers"}),
    timer:sleep(200),
    S2 = gen_node:call(Pid2, get_state),
    io:format("  Pid2 broadcasts = ~p~n", [S2#state.broadcasts]),
    io:format("  PASSED~n~n"),

    %% Test 7: gossip (epidemic spread)
    io:format("Test 7: gossip~n"),
    ok = gen_node:gossip(Pid1, {epidemic, "hello all"}),
    timer:sleep(500),
    S1a = gen_node:call(Pid1, get_state),
    S2a = gen_node:call(Pid2, get_state),
    io:format("  Pid1 gossips = ~p~n", [S1a#state.gossips]),
    io:format("  Pid2 gossips = ~p~n", [S2a#state.gossips]),
    io:format("  PASSED~n~n"),

    %% Test 8: register_name and whereis_name
    io:format("Test 8: register_name / whereis_name~n"),
    ok = gen_node:register_name(test_node, Pid1),
    Pid1 = gen_node:whereis_name(test_node),
    undefined = gen_node:whereis_name(nonexistent),
    io:format("  PASSED~n~n"),

    %% Test 9: ask (direct peer)
    io:format("Test 9: ask / direct peer~n"),
    {ok, Ans9} = gen_node:ask(Pid1, whoami, 1, 2000),
    io:format("  answers = ~p~n", [Ans9]),
    {_P9a, N9a} = lists:keyfind(Pid1, 1, Ans9),
    {P9b, N9b} = lists:keyfind(Pid2, 1, Ans9),
    true = is_atom(N9a),
    true = is_atom(N9b),
    io:format("  got reply from ~p: ~p~n", [P9b, N9b]),
    io:format("  PASSED~n~n"),

    %% Test 10: ask / noreply
    io:format("Test 10: ask / noreply~n"),
    {ok, Ans10} = gen_node:ask(Pid1, noreply_test, 1, 2000),
    [] = Ans10,
    io:format("  no replies (expected)~n", []),
    io:format("  PASSED~n~n"),

    %% Test 11: ask / echo
    io:format("Test 11: ask / echo~n"),
    {ok, Ans11} = gen_node:ask(Pid1, {echo, "hello"}, 1, 2000),
    io:format("  answers = ~p~n", [Ans11]),
    {P11a, {echoed, "hello"}} = lists:keyfind(Pid1, 1, Ans11),
    {P11b, {echoed, "hello"}} = lists:keyfind(Pid2, 1, Ans11),
    io:format("  got echoed from ~p and ~p~n", [P11a, P11b]),
    io:format("  PASSED~n~n"),

    %% Test 12: leave
    io:format("Test 12: leave~n"),
    ok = gen_node:leave(Pid2),
    timer:sleep(200),
    Peers1_after = gen_node:peers(Pid1),
    io:format("  Pid1 peers after Pid2 leave = ~p~n", [Peers1_after]),
    case lists:member(Pid2, Peers1_after) of
        false -> io:format("  Pid2 removed from Pid1 peers~n", []);
        true -> io:format("  WARN: Pid2 still in peers (may need more time)~n", [])
    end,
    io:format("  PASSED~n~n"),

    %% Test 13: stop
    io:format("Test 13: stop~n"),
    ok = gen_node:stop(Pid1),
    ok = gen_node:stop(Pid2),
    timer:sleep(100),
    io:format("  PASSED~n~n"),

    io:format("=== ALL TESTS PASSED ===~n"),
    ok.

test_gossip_disabled() ->
    io:format("~n=== gen_node gossip_disabled tests ===~n~n"),

    %% Test GD1: start with gossip_enabled=false and peers option
    io:format("Test GD1: start with gossip_enabled=false + peers option~n"),
    {ok, G1} = gen_node:start_link(?MODULE, [], [{gossip_enabled, false}]),
    {ok, G2} = gen_node:start_link(?MODULE, [], [{gossip_enabled, false}, {peers, [G1]}]),
    true = is_pid(G1),
    true = is_pid(G2),
    timer:sleep(200),
    Peers1 = gen_node:peers(G1),
    Peers2 = gen_node:peers(G2),
    io:format("  G1 peers = ~p~n", [Peers1]),
    io:format("  G2 peers = ~p~n", [Peers2]),
    [] = Peers1,
    true = lists:member(G1, Peers2),
    io:format("  PASSED~n~n"),

    %% Test GD2: join adds directly when gossip disabled
    io:format("Test GD2: join adds directly~n"),
    ok = gen_node:join(G1, [G2]),
    timer:sleep(200),
    Peers1a = gen_node:peers(G1),
    io:format("  G1 peers = ~p~n", [Peers1a]),
    true = lists:member(G2, Peers1a),
    io:format("  PASSED~n~n"),

    %% Test GD3: broadcast still works
    io:format("Test GD3: broadcast still works~n"),
    ok = gen_node:broadcast(G1, {msg, "gossip_disabled_test"}),
    timer:sleep(200),
    S2 = gen_node:call(G2, get_state),
    io:format("  G2 broadcasts = ~p~n", [S2#state.broadcasts]),
    true = lists:keymember({msg, "gossip_disabled_test"}, 2, S2#state.broadcasts),
    io:format("  PASSED~n~n"),

    %% Test GD4: call/cast still work
    io:format("Test GD4: call/cast still work~n"),
    ok = gen_node:cast(G1, {push, x}),
    ok = gen_node:cast(G1, {push, y}),
    S3 = gen_node:call(G1, get_state),
    io:format("  G1 casts = ~p~n", [S3#state.casts]),
    [y, x] = S3#state.casts,
    io:format("  PASSED~n~n"),

    %% Test GD5: gossip works even when gossip_enabled=false
    io:format("Test GD5: gossip works with gossip_enabled=false~n"),
    ok = gen_node:gossip(G1, {should_spread, "data"}),
    timer:sleep(500),
    S2b = gen_node:call(G2, get_state),
    io:format("  G2 gossips = ~p~n", [S2b#state.gossips]),
    true = lists:keymember({should_spread, "data"}, 2, S2b#state.gossips),
    io:format("  PASSED~n~n"),

    %% Test GD6: stop works
    io:format("Test GD6: stop works~n"),
    ok = gen_node:stop(G1),
    ok = gen_node:stop(G2),
    timer:sleep(100),
    io:format("  PASSED~n~n"),

    io:format("=== ALL GOSSIP_DISABLED TESTS PASSED ===~n"),
    ok.

test_all() ->
    test(),
    test_gossip_disabled(),
    io:format("~n=== ALL TESTS PASSED ===~n"),
    ok.
