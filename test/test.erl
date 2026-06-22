-module(test).
-behaviour(gen_node).

-export([init/1, handle_call/3, handle_cast/2,
         handle_broadcast/3, handle_gossip/3, handle_ask/3,
         handle_info/2, terminate/2]).
-export([test/0, test_gossip_disabled/0, test_all/0]).

-record(state, {
}).

init(_Args) -> {ok, #state{}}.

handle_call(get_state, _From, State) -> {reply, State, State};
handle_call(_Req, _From, State) -> {reply, ok, State}.

handle_cast(_Req, State) -> {noreply, State}.

handle_broadcast(From, Data, State) ->
    {noreply, State}.

handle_gossip(From, Data, State) ->
    {noreply, State}.

handle_ask(_From, noreply_test, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.



init_tree(M,NLayers,Args) when NLayers>1 ->
    Peers=lists:map(fun(_) -> init_tree(M,NLayers-1,Mod,Args) end,lists:seq(1,M)),
    {ok,Pid}=gen_node:start_link(?MODULE,Args,[{gossip_enabled, false}, {peers, Peers}]),
    Pid;
init_tree(_,1,Args) ->
    {ok,Pid}=gen_node:start_link(?MODULE,Args,[{gossip_enabled, false}]),
    Pid.

test() ->
    io:format("=== gen_node tests ===~n~n"),
    ok.

test_gossip_disabled() ->
    io:format("~n=== gen_node gossip_disabled tests ===~n~n"),
    init_tree(4,4,[]),
    ok.

test_all() ->
    test(),
    test_gossip_disabled(),
    ok.
