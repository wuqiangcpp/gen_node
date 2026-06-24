-module(gen_node).
-behaviour(gen_server).

%% Behavior callbacks
-callback init(Args :: term()) ->
    {ok, State :: term()} | {stop, Reason :: term()}.
-callback handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {noreply, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()}.
-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_broadcast(FromPid :: pid(), Data :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_gossip(FromPid :: pid(), Data :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_ask(FromPid :: pid(), Query :: term(), State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {noreply, NewState :: term()}.
-callback handle_info(Info :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback terminate(Reason :: term(), State :: term()) -> ok.
-optional_callbacks([handle_broadcast/3, handle_gossip/3, handle_ask/3]).

%% Public API
-export([start_link/3, start/3, stop/1]).
-export([call/2, call/3, cast/2]).
-export([broadcast/2, gossip/2, ask/4]).
-export([join/2, leave/1, peers/1]).
-export([register_name/2, whereis_name/1]).
-export([local_peers/0]).
-export([start_tree/5, start_line/4, start_grid/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("gen_node.hrl").

-define(DEFAULT_GOSSIP_INTERVAL, 1000).
-define(DEFAULT_FANOUT, 3).
-define(DEFAULT_TTL, 6).
-define(DEFAULT_SUSPECT_TIMEOUT, 3000).
-define(DEFAULT_DEAD_TIMEOUT, 6000).
-define(DEFAULT_SEEN_TTL, 60).
-define(SEEN_MAX_SIZE, 10000).
-define(DEFAULT_ACTIVE_VIEW_SIZE, 6).
-define(DEFAULT_PASSIVE_VIEW_SIZE, 100).
-define(DEFAULT_SHUFFLE_INTERVAL, 5000).
-define(DEFAULT_SHUFFLE_FANOUT, 4).
-define(DEFAULT_SHUFFLE_TTL, 3).

%%%===================================================================
%%% Public API
%%%===================================================================

-spec start_link(module(), term(), list()) -> {ok, pid()} | {error, term()}.
start_link(Mod, Args, Options) ->
    gen_server:start_link(?MODULE, [Mod, Args, Options], []).
-spec start(module(), term(), list()) -> {ok, pid()} | {error, term()}.
start(Mod, Args, Options) ->
    gen_server:start(?MODULE, [Mod, Args, Options], []).
-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:cast(Pid, stop).

-spec call(pid(), term()) -> term().
call(Pid, Request) -> gen_server:call(Pid, {call, Request}).
-spec call(pid(), term(), timeout()) -> term().
call(Pid, Request, Timeout) -> gen_server:call(Pid, {call, Request}, Timeout).
-spec cast(pid(), term()) -> ok.
cast(Pid, Request) -> gen_server:cast(Pid, {cast, Request}).

-spec broadcast(pid(), term()) -> ok.
broadcast(Pid, Data) -> gen_server:cast(Pid, {broadcast, Data}).
-spec gossip(pid(), term()) -> ok.
gossip(Pid, Data) -> gen_server:cast(Pid, {gossip, Data}).
-spec ask(pid(), term(), pos_integer(), timeout()) -> {ok, [{pid(), term()}]}.
ask(Pid, Query, TTL, Timeout) ->
    gen_server:call(Pid, {ask, Query, TTL, Timeout}, Timeout + 100).

-spec join(pid(), [pid() | {name, term()}]) -> ok.
join(Pid, Peers) -> gen_server:cast(Pid, {join, Peers}).
-spec leave(pid()) -> ok.
leave(Pid) -> gen_server:cast(Pid, leave).
-spec peers(pid()) -> [pid()].
peers(Pid) -> gen_server:call(Pid, peers).

-spec register_name(term(), pid()) -> ok | {error, {already_registered, pid()}}.
register_name(Name, Pid) -> gen_server:call(Pid, {register_name, Name}).
-spec whereis_name(term()) -> pid() | undefined.
whereis_name(Name) ->
    case global:whereis_name(Name) of
        undefined -> undefined;
        Pid when is_pid(Pid) -> Pid
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Mod, Args, Options]) ->
    case Mod:init(Args) of
        {ok, ModState} ->
            Name = proplists:get_value(name, Options, undefined),
            GI = proplists:get_value(gossip_interval, Options, ?DEFAULT_GOSSIP_INTERVAL),
            Fanout = proplists:get_value(fanout, Options, ?DEFAULT_FANOUT),
            Ttl = proplists:get_value(ttl, Options, ?DEFAULT_TTL),
            STO = proplists:get_value(suspect_timeout, Options, ?DEFAULT_SUSPECT_TIMEOUT),
            DTO = proplists:get_value(dead_timeout, Options, ?DEFAULT_DEAD_TIMEOUT),
            SeenTtl = proplists:get_value(seen_ttl, Options, ?DEFAULT_SEEN_TTL),
            AVS = proplists:get_value(active_view_size, Options, ?DEFAULT_ACTIVE_VIEW_SIZE),
            PVS = proplists:get_value(passive_view_size, Options, ?DEFAULT_PASSIVE_VIEW_SIZE),
            SI = proplists:get_value(shuffle_interval, Options, ?DEFAULT_SHUFFLE_INTERVAL),
            SF = proplists:get_value(shuffle_fanout, Options, ?DEFAULT_SHUFFLE_FANOUT),
            ST = proplists:get_value(shuffle_ttl, Options, ?DEFAULT_SHUFFLE_TTL),
            GossipEnabled = proplists:get_value(gossip_enabled, Options, true),
            PeersOption = proplists:get_value(peers, Options, []),
            Self = self(),
            ResolvedPeers = lists:map(fun resolve_peer/1, PeersOption),
            ValidPeers = [P || P <- ResolvedPeers, is_pid(P), P =/= Self],
            InitialActive = case GossipEnabled of
                true ->
                    _ = [P ! {gossip_join, Self} || P <- ValidPeers],
                    #{};
                false ->
                    lists:foldl(fun(P, Acc) ->
                        Acc#{P => #peer_info{status = alive, heartbeat = 0,
                                             incarnation = erlang:system_time(second),
                                             last_seen = erlang:system_time(millisecond)}}
                    end, #{}, ValidPeers)
            end,
            State = #gen_node_state{
                mod = Mod, mod_state = ModState, name = Name,
                active_peers = InitialActive, passive_peers = #{},
                seen = #{}, asks = #{}, ask_routes = #{},
                gossip_interval = GI, fanout = Fanout, ttl = Ttl,
                active_view_size = AVS, passive_view_size = PVS,
                shuffle_interval = SI, shuffle_fanout = SF, shuffle_ttl = ST,
                suspect_timeout = STO, dead_timeout = DTO, seen_ttl = SeenTtl,
                gossip_enabled = GossipEnabled
            },
            case Name of
                undefined -> ok;
                _ -> case global:register_name(Name, self()) of
                        yes -> ok;
                        {no, P} -> error_logger:warning_msg(
                            "gen_node: name ~p already registered by ~p~n", [Name, P])
                     end
            end,
            GT = erlang:send_after(GI, self(), gossip_tick),
            ST2 = case GossipEnabled of
                true -> erlang:send_after(SI, self(), shuffle_tick);
                false -> undefined
            end,
            {ok, State#gen_node_state{gossip_timer = GT, shuffle_timer = ST2}};
        {stop, Reason} -> {stop, Reason}
    end.

handle_call({call, Request}, From, State) ->
    try Mod = State#gen_node_state.mod,
        put('$gn_peers', maps:keys(State#gen_node_state.active_peers)),
        try case Mod:handle_call(Request, From, State#gen_node_state.mod_state) of
                {reply, Reply, NMS} -> {reply, Reply, State#gen_node_state{mod_state = NMS}};
                {noreply, NMS} -> {noreply, State#gen_node_state{mod_state = NMS}};
                {stop, R, Reply, NMS} -> {stop, R, Reply, State#gen_node_state{mod_state = NMS}};
                {stop, R, NMS} -> {stop, R, State#gen_node_state{mod_state = NMS}}
            end
        after erase('$gn_peers') end
    catch Class:Err:ST ->
        error_logger:error_msg("gen_node: handle_call ~p crashed ~p:~p~n~p~n",
            [Request, Class, Err, ST]),
        {reply, {error, {Class, Err}}, State}
    end;
handle_call(peers, _From, State) ->
    {reply, maps:keys(State#gen_node_state.active_peers), State};
handle_call({register_name, Name}, _From, State) ->
    case global:register_name(Name, self()) of
        yes -> {reply, ok, State#gen_node_state{name = Name}};
        {no, P} -> {reply, {error, {already_registered, P}}, State}
    end;
handle_call({ask, Query, TTL, Timeout}, From, State) ->
    AskId = {erlang:unique_integer([positive]), node()},
    Timer = erlang:send_after(Timeout, self(), {ask_timeout, AskId}),
    Self = self(),
    Mod = State#gen_node_state.mod,
    {SelfReply, St1} = case erlang:function_exported(Mod, handle_ask, 3) of
        true ->
            put('$gn_peers', maps:keys(State#gen_node_state.active_peers)),
            try case Mod:handle_ask(Self, Query, State#gen_node_state.mod_state) of
                    {reply, R, NMS} -> {[{Self, R}], State#gen_node_state{mod_state = NMS}};
                    {noreply, NMS}  -> {[], State#gen_node_state{mod_state = NMS}}
                end
            catch _:_ -> {[], State}
            after erase('$gn_peers') end;
        false -> {[], State}
    end,
    Asks = (St1#gen_node_state.asks)#{AskId => {From, SelfReply, Timer}},
    Peers = maps:keys(St1#gen_node_state.active_peers),
    case Peers of
        [] -> {noreply, St1#gen_node_state{asks = Asks}};
        _  -> FP = select_fanout_peers(Peers, St1#gen_node_state.fanout, Self),
              _ = [P ! {ask_forward, AskId, Self, Query, TTL, Self} || P <- FP],
              {noreply, St1#gen_node_state{asks = Asks}}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({cast, Request}, State) ->
    try Mod = State#gen_node_state.mod,
        put('$gn_peers', maps:keys(State#gen_node_state.active_peers)),
        try case Mod:handle_cast(Request, State#gen_node_state.mod_state) of
                {noreply, NMS} -> {noreply, State#gen_node_state{mod_state = NMS}};
                {stop, R, NMS} -> {stop, R, State#gen_node_state{mod_state = NMS}}
            end
        after erase('$gn_peers') end
    catch Class:Err:ST ->
        error_logger:error_msg("gen_node: handle_cast ~p crashed ~p:~p~n~p~n",
            [Request, Class, Err, ST]),
        {noreply, State}
    end;
handle_cast({broadcast, Data}, State) ->
    Self = self(),
    _ = [P ! {deliver_broadcast, Self, Data}
         || P <- maps:keys(State#gen_node_state.active_peers)],
    {noreply, State};
handle_cast({gossip, Data}, State) ->
    Self = self(),
    Peers = maps:keys(State#gen_node_state.active_peers),
    case Peers of
        [] -> {noreply, State};
        _ -> MsgId = {erlang:unique_integer([positive]), node()},
             Ttl = State#gen_node_state.ttl,
             FP = select_fanout_peers(Peers, State#gen_node_state.fanout, Self),
             _ = [P ! {gossip_forward, MsgId, Self, Data, Ttl} || P <- FP],
             {noreply, State}
    end;
handle_cast({join, PeersToJoin}, State) ->
    Self = self(),
    Resolved = lists:map(fun resolve_peer/1, PeersToJoin),
    Valid = [P || P <- Resolved, is_pid(P), P =/= Self],
    case State#gen_node_state.gossip_enabled of
        true -> _ = [P ! {gossip_join, Self} || P <- Valid],
                {noreply, State};
        false -> CT = erlang:system_time(millisecond),
                 Act0 = State#gen_node_state.active_peers,
                 Act1 = lists:foldl(fun(P, A) ->
                     case maps:is_key(P, A) of
                         true -> A;
                         false -> A#{P => #peer_info{status = alive, heartbeat = 0,
                                                      incarnation = erlang:system_time(second),
                                                      last_seen = CT}}
                     end
                 end, Act0, Valid),
                 _ = [P ! {add_peer, Self} || P <- Valid],
                 {noreply, State#gen_node_state{active_peers = Act1}}
    end;
handle_cast(leave, State) ->
    Self = self(),
    Act = maps:keys(State#gen_node_state.active_peers),
    Pas = maps:keys(State#gen_node_state.passive_peers),
    case State#gen_node_state.gossip_enabled of
        true -> _ = [P ! {gossip_leave, Self} || P <- Act ++ Pas];
        false -> ok
    end,
    case State#gen_node_state.gossip_timer of undefined -> ok; R1 -> erlang:cancel_timer(R1) end,
    case State#gen_node_state.shuffle_timer of undefined -> ok; R2 -> erlang:cancel_timer(R2) end,
    {noreply, State#gen_node_state{active_peers = #{}, passive_peers = #{},
                                   gossip_timer = undefined, shuffle_timer = undefined}};
handle_cast(stop, State) -> {stop, normal, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(gossip_tick, State) ->
    NewState = do_gossip_tick(State),
    Timer = erlang:send_after(State#gen_node_state.gossip_interval, self(), gossip_tick),
    {noreply, NewState#gen_node_state{gossip_timer = Timer}};
handle_info(shuffle_tick, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info(shuffle_tick, State) ->
    NewState = do_shuffle_tick(State),
    Timer = erlang:send_after(State#gen_node_state.shuffle_interval, self(), shuffle_tick),
    {noreply, NewState#gen_node_state{shuffle_timer = Timer}};

%% gossip_sync: exchange active views between active peers
handle_info({gossip_sync, _, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({gossip_sync, FromPid, RemoteActive, SendTS}, State) ->
    CT = erlang:system_time(millisecond),
    Self = self(),
    Act0 = State#gen_node_state.active_peers,
    Pas0 = State#gen_node_state.passive_peers,
    {Act1, Pas1} = case maps:find(FromPid, Act0) of
        {ok, Info} ->
            UpdInfo = Info#peer_info{last_seen = CT, status = alive,
                                     heartbeat = Info#peer_info.heartbeat + 1},
            {Act0#{FromPid => UpdInfo}, Pas0};
        error ->
            Info = #peer_info{status = alive, heartbeat = 0,
                              incarnation = erlang:system_time(second), last_seen = CT},
            try_promote(FromPid, Info, Act0, Pas0, State)
    end,
    %% Unknown peers from remote active → add to passive only
    Pas2 = maps:fold(fun(Pid, _, P) ->
        case Pid =:= Self orelse maps:is_key(Pid, Act1) orelse maps:is_key(Pid, P) of
            true -> P;
            false ->
                P#{Pid => #peer_info{status = alive, heartbeat = 0,
                                     incarnation = erlang:system_time(second),
                                     last_seen = CT}}
        end
    end, Pas1, RemoteActive),
    Pas3 = maps:remove(Self, Pas2),
    FromPid ! {gossip_sync_reply, Self, Act1, SendTS},
    {noreply, State#gen_node_state{active_peers = Act1, passive_peers = trim_passive(Pas3, State)}};
handle_info({gossip_sync_reply, _, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({gossip_sync_reply, FromPid, RemoteActive, _SendTS}, State) ->
    CT = erlang:system_time(millisecond),
    Self = self(),
    Act0 = State#gen_node_state.active_peers,
    Pas0 = State#gen_node_state.passive_peers,
    {Act1, Pas1} = case maps:find(FromPid, Act0) of
        {ok, Info} ->
            UpdInfo = Info#peer_info{last_seen = CT, status = alive,
                                     heartbeat = Info#peer_info.heartbeat + 1},
            {Act0#{FromPid => UpdInfo}, Pas0};
        error ->
            Info = #peer_info{status = alive, heartbeat = 0,
                              incarnation = erlang:system_time(second), last_seen = CT},
            try_promote(FromPid, Info, Act0, Pas0, State)
    end,
    Pas2 = maps:fold(fun(Pid, _, P) ->
        case Pid =:= Self orelse maps:is_key(Pid, Act1) orelse maps:is_key(Pid, P) of
            true -> P;
            false ->
                P#{Pid => #peer_info{status = alive, heartbeat = 0,
                                     incarnation = erlang:system_time(second),
                                     last_seen = CT}}
        end
    end, Pas1, RemoteActive),
    Pas3 = maps:remove(Self, Pas2),
    {noreply, State#gen_node_state{active_peers = Act1, passive_peers = trim_passive(Pas3, State)}};

%% join: new node sends gossip_join to seed
handle_info({gossip_join, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({gossip_join, FromPid}, State) ->
    CT = erlang:system_time(millisecond),
    Self = self(),
    Act = State#gen_node_state.active_peers,
    Pas = State#gen_node_state.passive_peers,
    NewInfo = #peer_info{status = alive, heartbeat = 0,
                         incarnation = erlang:system_time(second), last_seen = CT},
    %% Add to passive first
    Pas1 = Pas#{FromPid => NewInfo},
    %% Try to promote to active if there's room
    {Act1, Pas2} = try_promote(FromPid, NewInfo, Act, Pas1, State),
    Pas3 = maps:remove(Self, trim_passive(Pas2, State)),
    %% Reply with our active view
    FromPid ! {gossip_join_reply, Self, Act1},
    %% Forward join to active peers
    ForwardPeers = [P || P <- maps:keys(Act1), P =/= FromPid, P =/= Self],
    STTL = State#gen_node_state.shuffle_ttl,
    _ = [P ! {forward_join, Self, FromPid, STTL} || P <- ForwardPeers],
    {noreply, State#gen_node_state{active_peers = Act1, passive_peers = Pas3}};

%% gossip_join_reply: seed's active view comes back
handle_info({gossip_join_reply, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({gossip_join_reply, FromPid, RemoteActive}, State) ->
    Self = self(),
    Act = State#gen_node_state.active_peers,
    Pas0 = State#gen_node_state.passive_peers,
    CT = erlang:system_time(millisecond),
    SeedInfo = #peer_info{status = alive, heartbeat = 0,
                          incarnation = erlang:system_time(second), last_seen = CT},
    {Act1, Pas1} = try_promote(FromPid, SeedInfo, Act, Pas0, State),
    {Act2, Pas2} = maps:fold(fun(Pid, _, {A, P}) ->
        case maps:is_key(Pid, A) orelse Pid =:= Self of
            true -> {A, P};
            false -> add_to_passive(Pid, A, P, State)
        end
    end, {Act1, Pas1}, RemoteActive),
    Pas3 = maps:remove(Self, trim_passive(Pas2, State)),
    {noreply, State#gen_node_state{active_peers = Act2, passive_peers = Pas3}};

%% forward_join: propagate new node knowledge through active view
handle_info({forward_join, _, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({forward_join, _OrigPid, NewPid, TTL}, State) ->
    Self = self(),
    Act = State#gen_node_state.active_peers,
    case maps:is_key(NewPid, Act) of
        true -> {noreply, State};
        false ->
            CT = erlang:system_time(millisecond),
            Info = #peer_info{status = alive, heartbeat = 0,
                              incarnation = erlang:system_time(second), last_seen = CT},
            Pas0 = State#gen_node_state.passive_peers,
            {Act1, Pas1} = try_promote(NewPid, Info, Act, Pas0, State),
            Pas2 = maps:remove(Self, trim_passive(Pas1, State)),
            case TTL > 0 of
                true ->
                    FwdPeers = [P || P <- maps:keys(Act1), P =/= NewPid, P =/= Self],
                    _ = [P ! {forward_join, Self, NewPid, TTL - 1} || P <- FwdPeers],
                    {noreply, State#gen_node_state{active_peers = Act1, passive_peers = Pas2}};
                false ->
                    {noreply, State#gen_node_state{active_peers = Act1, passive_peers = Pas2}}
            end
    end;

%% add_peer: direct peer addition (gossip disabled only)
handle_info({add_peer, FromPid}, State) ->
    CT = erlang:system_time(millisecond),
    Act = State#gen_node_state.active_peers,
    case maps:is_key(FromPid, Act) of
        true -> {noreply, State};
        false ->
            Info = #peer_info{status = alive, heartbeat = 0,
                              incarnation = erlang:system_time(second), last_seen = CT},
            {noreply, State#gen_node_state{active_peers = Act#{FromPid => Info}}}
    end;

%% gossip_leave: remove from both views
handle_info({gossip_leave, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({gossip_leave, FromPid}, State) ->
    Act = maps:remove(FromPid, State#gen_node_state.active_peers),
    Pas = maps:remove(FromPid, State#gen_node_state.passive_peers),
    %% Fill active slot from passive
    {Act1, Pas1} = fill_active_view(Act, Pas, State),
    {noreply, State#gen_node_state{active_peers = Act1, passive_peers = trim_passive(Pas1, State)}};

%% shuffle: exchange passive views for diversity
handle_info({shuffle_req, _, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({shuffle_req, FromPid, RemotePassiveList, TTL}, State) ->
    Self = self(),
    Act = State#gen_node_state.active_peers,
    Pas0 = State#gen_node_state.passive_peers,
    CT = erlang:system_time(millisecond),
    Pas1 = lists:foldl(fun(Pid, P) ->
        case Pid =:= Self orelse maps:is_key(Pid, Act) orelse maps:is_key(Pid, P) of
            true -> P;
            false -> P#{Pid => #peer_info{status = alive, heartbeat = 0,
                                          incarnation = erlang:system_time(second),
                                          last_seen = CT}}
        end
    end, Pas0, RemotePassiveList),
    Pas2 = maps:remove(Self, trim_passive(Pas1, State)),
    PasKeys = maps:keys(Pas2),
    SF = State#gen_node_state.shuffle_fanout,
    ReplySubset = pick_random_n(PasKeys, min(SF, length(PasKeys))),
    FromPid ! {shuffle_reply, Self, ReplySubset},
    case TTL > 0 of
        true -> FwdPeers = [P || P <- maps:keys(Act), P =/= FromPid, P =/= Self],
                _ = [P ! {shuffle_req, Self, RemotePassiveList, TTL - 1} || P <- FwdPeers],
                {noreply, State#gen_node_state{passive_peers = Pas2}};
        false -> {noreply, State#gen_node_state{passive_peers = Pas2}}
    end;
handle_info({shuffle_reply, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({shuffle_reply, _FromPid, RemotePassiveList}, State) ->
    Self = self(),
    Act = State#gen_node_state.active_peers,
    Pas0 = State#gen_node_state.passive_peers,
    CT = erlang:system_time(millisecond),
    Pas1 = lists:foldl(fun(Pid, P) ->
        case Pid =:= Self orelse maps:is_key(Pid, Act) orelse maps:is_key(Pid, P) of
            true -> P;
            false -> P#{Pid => #peer_info{status = alive, heartbeat = 0,
                                          incarnation = erlang:system_time(second),
                                          last_seen = CT}}
        end
    end, Pas0, RemotePassiveList),
    Pas2 = maps:remove(Self, trim_passive(Pas1, State)),
    {noreply, State#gen_node_state{passive_peers = Pas2}};

%% deliver_broadcast: from a peer
handle_info({deliver_broadcast, FromPid, Data}, State) ->
    Mod = State#gen_node_state.mod,
    case erlang:function_exported(Mod, handle_broadcast, 3) of
        true ->
            put('$gn_peers', maps:keys(State#gen_node_state.active_peers)),
            try case Mod:handle_broadcast(FromPid, Data, State#gen_node_state.mod_state) of
                    {noreply, NMS} -> {noreply, State#gen_node_state{mod_state = NMS}};
                    {stop, R, NMS} -> {stop, R, State#gen_node_state{mod_state = NMS}}
                end
            catch Class:Err:ST ->
                error_logger:error_msg("gen_node: handle_broadcast crashed ~p:~p~n~p~n",
                    [Class, Err, ST]),
                {noreply, State}
            after
                erase('$gn_peers')
            end;
        false -> {noreply, State}
    end;
%% gossip_forward: epidemic data spread
handle_info({gossip_forward, _, _, _, _}, #gen_node_state{gossip_enabled = false} = State) ->
    {noreply, State};
handle_info({gossip_forward, MsgId, OrigPid, Data, TTL}, State) ->
    Seen = State#gen_node_state.seen,
    CT = erlang:system_time(second),
    case maps:is_key(MsgId, Seen) of
        true -> {noreply, State};
        false ->
            Seen1 = case map_size(Seen) >= ?SEEN_MAX_SIZE of
                true -> clean_seen(Seen, State#gen_node_state.seen_ttl, CT);
                false -> Seen
            end,
            Seen2 = Seen1#{MsgId => CT},
            NS0 = State#gen_node_state{seen = Seen2},
            Mod = NS0#gen_node_state.mod,
            NS = case erlang:function_exported(Mod, handle_gossip, 3) of
                true ->
                    put('$gn_peers', maps:keys(NS0#gen_node_state.active_peers)),
                    try case Mod:handle_gossip(OrigPid, Data, NS0#gen_node_state.mod_state) of
                            {noreply, NMS} -> NS0#gen_node_state{mod_state = NMS};
                            {stop, _, NMS} -> NS0#gen_node_state{mod_state = NMS}
                        end
                    catch Class:Err:ST ->
                        error_logger:error_msg("gen_node: handle_gossip crashed ~p:~p~n~p~n",
                            [Class, Err, ST]),
                        NS0
                    after
                        erase('$gn_peers')
                    end;
                false -> NS0
            end,
            APeers = maps:keys(NS#gen_node_state.active_peers),
            case TTL > 0 andalso APeers =/= [] of
                true -> FP = select_fanout_peers(APeers, NS#gen_node_state.fanout, OrigPid),
                        _ = [P ! {gossip_forward, MsgId, OrigPid, Data, TTL - 1} || P <- FP],
                        {noreply, NS};
                false -> {noreply, NS}
            end
    end;
%% ask_forward / ask_reply / ask_timeout
handle_info({ask_forward, AskId, OrigPid, Query, TTL, FromHop}, State) ->
    Seen = State#gen_node_state.seen,
    SeenCT = erlang:system_time(second),
    case maps:is_key(AskId, Seen) of
        true -> {noreply, State};
        false ->
            Seen1 = case map_size(Seen) >= ?SEEN_MAX_SIZE of
                true -> clean_seen(Seen, State#gen_node_state.seen_ttl, SeenCT);
                false -> Seen
            end,
            Seen2 = Seen1#{AskId => SeenCT},
            StSeen = State#gen_node_state{seen = Seen2},
            AR0 = StSeen#gen_node_state.ask_routes,
            AR = AR0#{AskId => FromHop},
            St1 = StSeen#gen_node_state{ask_routes = AR},
            Mod = St1#gen_node_state.mod,
            Self = self(),
            St2 = case erlang:function_exported(Mod, handle_ask, 3) of
                true ->
                    put('$gn_peers', maps:keys(St1#gen_node_state.active_peers)),
                    try case Mod:handle_ask(OrigPid, Query, St1#gen_node_state.mod_state) of
                            {reply, Reply, NMS} ->
                                send_ask_reply(OrigPid, Self, AskId, Reply, St1),
                                St1#gen_node_state{mod_state = NMS};
                            {noreply, NMS} -> St1#gen_node_state{mod_state = NMS}
                        end
                    catch Class:Err:ST ->
                        error_logger:error_msg("gen_node: handle_ask crashed ~p:~p~n~p~n",
                            [Class, Err, ST]),
                        St1
                    after
                        erase('$gn_peers')
                    end;
                false -> St1
            end,
            APeers = maps:keys(St2#gen_node_state.active_peers),
            case TTL > 0 andalso APeers =/= [] of
                true -> FP = select_fanout_peers(APeers, St2#gen_node_state.fanout, FromHop),
                        _ = [P ! {ask_forward, AskId, OrigPid, Query, TTL - 1, Self} || P <- FP],
                        {noreply, St2};
                false -> {noreply, St2}
            end
    end;
handle_info({ask_reply, AskId, OrigPid, ReplyPid, Reply}, State) ->
    Self = self(),
    case Self =:= OrigPid of
        true ->
            case maps:find(AskId, State#gen_node_state.asks) of
                {ok, {From, Replies, Timer}} ->
                    Asks0 = State#gen_node_state.asks,
                    Asks = Asks0#{AskId => {From, [{ReplyPid, Reply} | Replies], Timer}},
                    {noreply, State#gen_node_state{asks = Asks}};
                error -> {noreply, State}
            end;
        false ->
            case maps:find(AskId, State#gen_node_state.ask_routes) of
                {ok, HopBack} ->
                    HopBack ! {ask_reply, AskId, OrigPid, ReplyPid, Reply},
                    {noreply, State};
                error -> {noreply, State}
            end
    end;
handle_info({ask_timeout, AskId}, State) ->
    case maps:find(AskId, State#gen_node_state.asks) of
        {ok, {From, Replies, _Timer}} ->
            gen_server:reply(From, {ok, lists:reverse(Replies)}),
            {noreply, State#gen_node_state{asks = maps:remove(AskId, State#gen_node_state.asks)}};
        error -> {noreply, State}
    end;
%% ping/ack for failure detection
handle_info({gossip_ping, Ref, FromPid}, State) ->
    FromPid ! {gossip_ack, Ref, self()}, {noreply, State};
handle_info({gossip_ack, _Ref, FromPid}, State) ->
    CT = erlang:system_time(millisecond),
    Act = State#gen_node_state.active_peers,
    case maps:find(FromPid, Act) of
        {ok, Info} ->
            Upd = Info#peer_info{status = alive, last_seen = CT},
            {noreply, State#gen_node_state{active_peers = Act#{FromPid => Upd}}};
        error -> {noreply, State}
    end;
%% default: pass to user callback
handle_info(Info, State) ->
    try Mod = State#gen_node_state.mod,
        put('$gn_peers', maps:keys(State#gen_node_state.active_peers)),
        try case Mod:handle_info(Info, State#gen_node_state.mod_state) of
                {noreply, NMS} -> {noreply, State#gen_node_state{mod_state = NMS}};
                {stop, R, NMS} -> {stop, R, State#gen_node_state{mod_state = NMS}}
            end
        after erase('$gn_peers') end
    catch Class:Err:ST ->
        error_logger:error_msg("gen_node: handle_info ~p crashed ~p:~p~n~p~n",
            [Info, Class, Err, ST]),
        {noreply, State}
    end.

terminate(Reason, State) ->
    Self = self(),
    case State#gen_node_state.gossip_enabled of
        true ->
            All = maps:keys(State#gen_node_state.active_peers)
                  ++ maps:keys(State#gen_node_state.passive_peers),
            _ = [P ! {gossip_leave, Self} || P <- All];
        false -> ok
    end,
    lists:foreach(fun(T) ->
        case T of undefined -> ok; Ref -> erlang:cancel_timer(Ref) end
    end, [State#gen_node_state.gossip_timer, State#gen_node_state.shuffle_timer]),
    case State#gen_node_state.name of
        undefined -> ok;
        N -> global:unregister_name(N)
    end,
    try Mod = State#gen_node_state.mod, Mod:terminate(Reason, State#gen_node_state.mod_state)
    after ok end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal: gossip tick (failure detection + sync)
%%%===================================================================

do_gossip_tick(State) ->
    CT = erlang:system_time(millisecond),
    Self = self(),
    Act0 = State#gen_node_state.active_peers,
    Pas0 = State#gen_node_state.passive_peers,
    STO = State#gen_node_state.suspect_timeout,
    DTO = State#gen_node_state.dead_timeout,
    GossipEnabled = State#gen_node_state.gossip_enabled,

    %% Check active peers for timeout
    DeadAct = maps:fold(fun(Pid, Info, Acc) ->
        TSince = CT - Info#peer_info.last_seen,
        if TSince > DTO -> [Pid | Acc];
           true -> Acc
        end
    end, [], Act0),
    SuspectAct = maps:fold(fun(Pid, Info, Acc) ->
        TSince = CT - Info#peer_info.last_seen,
        if TSince > STO andalso TSince =< DTO -> [Pid | Acc];
           true -> Acc
        end
    end, [], Act0),

    %% Remove dead from active
    Act1 = lists:foldl(fun(P, A) -> maps:remove(P, A) end, Act0, DeadAct),

    %% Ping suspect nodes
    _ = case SuspectAct of
        [] -> ok;
        _ -> RPid = lists:nth(rand:uniform(length(SuspectAct)), SuspectAct),
             Ref = make_ref(),
             RPid ! {gossip_ping, Ref, Self}
    end,

    %% Clean stale passive peers
    Pas1 = maps:filter(fun(_, Info) ->
        CT - Info#peer_info.last_seen =< DTO * 3
    end, Pas0),

    %% Fill active from passive if room
    {Act2, Pas2} = fill_active_view(Act1, Pas1, State),

    case GossipEnabled of
        true ->
            %% Pick random active peer for gossip_sync
            case maps:keys(Act2) of
                [] -> ok;
                PeerList ->
                    RP = lists:nth(rand:uniform(length(PeerList)), PeerList),
                    RP ! {gossip_sync, Self, Act2, CT}
            end;
        false -> ok
    end,
    State#gen_node_state{active_peers = Act2, passive_peers = trim_passive(Pas2, State)}.

%%%===================================================================
%%% Internal: shuffle tick
%%%===================================================================

do_shuffle_tick(State) ->
    Act = maps:keys(State#gen_node_state.active_peers),
    Pas = maps:keys(State#gen_node_state.passive_peers),
    case Act of
        [] -> State;
        _ ->
            RP = lists:nth(rand:uniform(length(Act)), Act),
            SF = State#gen_node_state.shuffle_fanout,
            Subset = pick_random_n(Pas, min(SF, length(Pas))),
            RP ! {shuffle_req, self(), Subset, State#gen_node_state.shuffle_ttl},
            State
    end.

%%%===================================================================
%%% Internal: view management
%%%===================================================================

add_to_passive(Pid, Act, Pas, _State) ->
    case maps:is_key(Pid, Pas) of
        true -> {Act, Pas};
        false ->
            CT = erlang:system_time(millisecond),
            Info = #peer_info{status = alive, heartbeat = 0,
                              incarnation = erlang:system_time(second), last_seen = CT},
            {Act, Pas#{Pid => Info}}
    end.

try_promote(Pid, Info, Act, Pas, State) ->
    Max = State#gen_node_state.active_view_size,
    case map_size(Act) < Max of
        true -> {Act#{Pid => Info}, maps:remove(Pid, Pas)};
        false -> {Act, Pas#{Pid => Info}}
    end.

fill_active_view(Act, Pas, State) ->
    Max = State#gen_node_state.active_view_size,
    ActSize = map_size(Act),
    case ActSize < Max andalso Pas =/= #{} of
        true ->
            CT = erlang:system_time(millisecond),
            PasList = maps:to_list(Pas),
            {Pid, Info} = lists:nth(rand:uniform(length(PasList)), PasList),
            Upd = Info#peer_info{status = alive, last_seen = CT},
            Act2 = Act#{Pid => Upd},
            Pas2 = maps:remove(Pid, Pas),
            fill_active_view(Act2, Pas2, State);
        false ->
            {Act, Pas}
    end.

trim_passive(Passive, State) ->
    Max = State#gen_node_state.passive_view_size,
    case map_size(Passive) =< Max of
        true -> Passive;
        false ->
            Keys = maps:keys(Passive),
            ToRemove = length(Keys) - Max,
            Remove = pick_random_n(Keys, ToRemove),
            lists:foldl(fun(K, M) -> maps:remove(K, M) end, Passive, Remove)
    end.

%%%===================================================================
%%% Internal: utility
%%%===================================================================

resolve_peer(P) when is_pid(P) -> P;
resolve_peer({name, N, Node}) ->
    _ = net_kernel:connect_node(Node),
    case global:whereis_name(N) of
        undefined -> undefined;
        P when is_pid(P) -> P
    end;
resolve_peer(_) -> undefined.

select_fanout_peers(Peers, Fanout, Exclude) ->
    Filtered = [P || P <- Peers, P =/= Exclude],
    case length(Filtered) =< Fanout of
        true -> Filtered;
        false ->
            Shuffled = [X || {_, X} <- lists:sort(
                [{rand:uniform(), P} || P <- Filtered])],
            lists:sublist(Shuffled, Fanout)
    end.

send_ask_reply(OrigPid, Self, AskId, Reply, State) ->
    Act = State#gen_node_state.active_peers,
    case maps:is_key(OrigPid, Act) of
        true -> OrigPid ! {ask_reply, AskId, OrigPid, Self, Reply};
        false ->
            case maps:find(AskId, State#gen_node_state.ask_routes) of
                {ok, HopBack} -> HopBack ! {ask_reply, AskId, OrigPid, Self, Reply};
                error -> ok
            end
    end.

pick_random_n(List, N) when N >= length(List) -> List;
pick_random_n(_List, 0) -> [];
pick_random_n(List, N) ->
    Shuffled = [X || {_, X} <- lists:sort(
        [{rand:uniform(), P} || P <- List])],
    lists:sublist(Shuffled, N).

clean_seen(Seen, SeenTtl, CurrentTimeSec) ->
    Cutoff = CurrentTimeSec - SeenTtl,
    Filtered = maps:filter(fun(_, TS) -> TS >= Cutoff end, Seen),
    case map_size(Filtered) > (?SEEN_MAX_SIZE div 2) of
        true ->
            Sorted = lists:sort(fun({_, T1}, {_, T2}) -> T1 > T2 end, maps:to_list(Filtered)),
            maps:from_list(lists:sublist(Sorted, ?SEEN_MAX_SIZE div 2));
        false -> Filtered
    end.

%%%===================================================================
%%% Process context helpers
%%%===================================================================

local_peers() ->
    case get('$gn_peers') of
        undefined -> [];
        P -> P
    end.

%%%===================================================================
%%% Topology helpers
%%%===================================================================

%% Tree: M-ary of depth L, return root of the tree
start_tree(M, L, Mod, Args, Sym) when L > 1 ->
    Children = [start_tree(M, L - 1, Mod, Args, Sym) || _ <- lists:seq(1, M)],
    start_node(Children, Mod, Args, Sym);
start_tree(_, 1, Mod, Args, Sym) ->
    start_node([], Mod, Args, Sym).

%% Line: chain of M nodes, return a list of all nodes
start_line(M, Mod, Args, Sym) ->
    lists:foldl(fun(_, []) ->
        [start_node([], Mod, Args, Sym)];
    (_, [H | _] = L) ->
        [start_node([H], Mod, Args, Sym) | L]
    end, [], lists:seq(1, M)).

%% Grid: M rows × N columns, return M nodes in last column
start_grid(M, N, Mod, Args, Sym) ->
    lists:foldl(fun(_, []) -> start_line(M, Mod, Args, Sym);
                   (_, FL) -> start_line_with_line(FL, Mod, Args, Sym)
                end, [], lists:seq(1, N)).

start_line_with_line(FL, Mod, Args, Sym) ->
    lists:foldl(fun(F, []) ->
        [start_node([F], Mod, Args, Sym)];
    (F, [H | _] = L) ->
        [start_node([H, F], Mod, Args, Sym) | L]
    end, [], FL).

start_node(Peers, Mod, Args, symmetric) ->
    {ok, Pid} = start_link(Mod, Args, [{gossip_enabled, false}, {peers, Peers}]),
    [join(Pid, [P]) || P <- Peers],
    Pid;
start_node(Peers, Mod, Args, asymmetric) ->
    {ok, Pid} = start_link(Mod, Args, [{gossip_enabled, false}, {peers, Peers}]),
    Pid.
