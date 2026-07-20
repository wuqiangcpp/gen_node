-module(gen_node_monitor).
-behaviour(gen_server).

-export([start_link/0, start_link/1, add_node/1, remove_node/1, get_nodes/0, get_node/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_INTERVAL, 5000).
-define(DEFAULT_TIMEOUT, 3000).
-define(DEFAULT_REQUEST, get_state).

-record(node_info, {
    status :: up | down,
    peers :: [pid()],
    last_seen :: non_neg_integer() | undefined,
    last_reply :: term() | undefined
}).

-record(mon_state, {
    nodes :: #{pid() => #node_info{}},
    interval :: pos_integer(),
    timer :: reference() | undefined,
    request :: term(),
    timeout :: pos_integer(),
    auto_discover :: boolean(),
    seeds :: [pid()]
}).

start_link() -> start_link([]).
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

add_node(Pid) -> gen_server:call(?MODULE, {add_node, Pid}).
remove_node(Pid) -> gen_server:call(?MODULE, {remove_node, Pid}).
get_nodes() -> gen_server:call(?MODULE, get_nodes).
get_node(Pid) -> gen_server:call(?MODULE, {get_node, Pid}).

init(Opts) ->
    Interval = proplists:get_value(interval, Opts, ?DEFAULT_INTERVAL),
    Req = proplists:get_value(request, Opts, ?DEFAULT_REQUEST),
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    AutoDiscover = proplists:get_value(auto_discover, Opts, true),
    Seeds = proplists:get_value(seeds, Opts, []),
    NodeInfos = lists:foldl(fun(P, Acc) ->
        Acc#{P => #node_info{status = up, peers = [], last_seen = undefined, last_reply = undefined}}
    end, #{}, Seeds),
    State = #mon_state{
        nodes = NodeInfos, interval = Interval, request = Req,
        timeout = Timeout, auto_discover = AutoDiscover, seeds = Seeds
    },
    Timer = erlang:send_after(Interval, self(), tick),
    {ok, State#mon_state{timer = Timer}}.

handle_call({add_node, Pid}, _From, State) ->
    Nodes = State#mon_state.nodes,
    {reply, ok, State#mon_state{nodes = case maps:is_key(Pid, Nodes) of
        true -> Nodes;
        false -> Nodes#{Pid => #node_info{status = up, peers = [], last_seen = undefined, last_reply = undefined}}
    end}};
handle_call({remove_node, Pid}, _From, State) ->
    {reply, ok, State#mon_state{nodes = maps:remove(Pid, State#mon_state.nodes)}};
handle_call(get_nodes, _From, State) ->
    Res = [{P, format_info(I)} || {P, I} <- maps:to_list(State#mon_state.nodes)],
    {reply, Res, State};
handle_call({get_node, Pid}, _From, State) ->
    case maps:find(Pid, State#mon_state.nodes) of
        {ok, Info} -> {reply, {ok, format_info(Info)}, State};
        error -> {reply, {error, not_found}, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Req, State) -> {noreply, State}.

handle_info(tick, State) ->
    Nodes0 = State#mon_state.nodes,
    Req = State#mon_state.request,
    Timeout = State#mon_state.timeout,
    Auto = State#mon_state.auto_discover,
    {Nodes1, NewPids} = probe_nodes(Nodes0, Req, Timeout, Auto, []),
    FinalNodes = case Auto of
        true ->
            lists:foldl(fun(P, Acc) ->
                case maps:is_key(P, Acc) of
                    true -> Acc;
                    false -> Acc#{P => #node_info{status = up, peers = [], last_seen = undefined, last_reply = undefined}}
                end
            end, Nodes1, NewPids);
        false -> Nodes1
    end,
    Timer = erlang:send_after(State#mon_state.interval, self(), tick),
    {noreply, State#mon_state{nodes = FinalNodes, timer = Timer}};
handle_info(_Info, State) -> {noreply, State}.

probe_nodes(Nodes, Req, Timeout, Auto, NewAcc) ->
    maps:fold(fun(Pid, Info, {NAcc, NP}) ->
        CT = erlang:system_time(millisecond),
        Peers = try gen_node:peers(Pid) of
            P when is_list(P) -> P
        catch _:_ -> []
        end,
        Status = case catch gen_server:call(Pid, {call, Req}, Timeout) of
            {ok, Reply} ->
                Info#node_info{status = up, peers = Peers, last_seen = CT, last_reply = Reply};
            {reply, Reply, _} ->
                Info#node_info{status = up, peers = Peers, last_seen = CT, last_reply = Reply};
            Reply when is_tuple(Reply) ->
                Info#node_info{status = up, peers = Peers, last_seen = CT, last_reply = Reply};
            _ ->
                Info#node_info{status = down, peers = Peers, last_seen = CT, last_reply = undefined}
        end,
        NewPids = case Auto of
            true -> [Q || Q <- Peers, not maps:is_key(Q, NAcc), Q =/= Pid] ++ NP;
            false -> NP
        end,
        {NAcc#{Pid => Status}, NewPids}
    end, {maps:filter(fun(_, I) -> I#node_info.status =/= down end, Nodes), NewAcc}, Nodes).

format_info(I) ->
    #{status => I#node_info.status, peers => I#node_info.peers,
      last_seen => I#node_info.last_seen, last_reply => I#node_info.last_reply}.
