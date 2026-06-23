-module(test).
-behaviour(gen_node).

-export([init/1, handle_call/3, handle_cast/2,
         handle_broadcast/3, handle_gossip/3, handle_ask/3,
         handle_info/2, terminate/2]).
-export([test/0, test_gossip_disabled/0, test_all/0, discover/2, init_tree/5]).

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
    io:format("=== gen_node tests ===~n~n"),
    ok.

test_gossip_disabled() ->
    io:format("~n=== gen_node gossip_disabled tests ===~n~n"),
    Pid = init_tree(2, 3, test, [], asymmetric),
    timer:sleep(100),
    io:format("Root: ~p, peers: ~p~n", [Pid, gen_node:peers(Pid)]),
    io:format("topology discovery:~n"),
    discover(Pid, 6),
    ok.

init_tree(M, L, Mod, Args, Sym) when L > 1 ->
    Children = [init_tree(M, L - 1, Mod, Args, Sym) || _ <- lists:seq(1, M)],
    start_node(Children, Mod, Args, Sym);
init_tree(_, 1, Mod, Args, _Sym) ->
    start_node([], Mod, Args, asymmetric).

start_node(Peers, Mod, Args, symmetric) ->
    {ok, Pid} = gen_node:start_link(Mod, Args, [{gossip_enabled, false}, {peers, Peers}]),
    [gen_node:join(Pid, [P]) || P <- Peers],
    Pid;
start_node(Peers, Mod, Args, asymmetric) ->
    {ok, Pid} = gen_node:start_link(Mod, Args, [{gossip_enabled, false}, {peers, Peers}]),
    Pid.

test_all() ->
    test(),
    test_gossip_disabled(),
    ok.

%% 拓扑发现：通过 ask 遍历所有可达节点
%% Root: 任意已知节点 PID
%% TTL:  搜索深度
discover(Root, TTL) ->
    case gen_node:ask(Root, get_peers, TTL, 5000) of
        {ok, Replies} ->
            Nodes = lists:usort([P || {P, _} <- Replies]),
            Links = lists:usort(lists:flatmap(
                fun({P, Ps}) -> [{P, Q} || Q <- Ps, Q =/= P] end, Replies)),
            io:format("Nodes (~p):~n", [length(Nodes)]),
            [io:format("  ~p~n", [N]) || N <- Nodes],
            io:format("Links (~p):~n", [length(Links)]),
            [io:format("  ~p -- ~p~n", [A, B]) || {A, B} <- Links],
            #{nodes => Nodes, links => Links}
    end.
