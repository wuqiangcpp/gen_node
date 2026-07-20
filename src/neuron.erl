-module(neuron).
-behaviour(gen_node).

-export([init/1, handle_call/3, handle_cast/2,
         handle_broadcast/3, handle_gossip/3, handle_ask/3,
         handle_info/2, terminate/2]).
-export([set_s/2, set_w/3, get_state/1, get_s/1, set_beta/2, set_beta_gossip/2,
           clamp/2, unclamp/1, set_update_weights_gossip/2, set_update_weights/2, set_p/3]).
-export([debug/1]).

-record(state, {
    s = 0 :: 0 | 1,
    w = #{} :: #{pid() => float()},
    sj = #{} :: #{pid() => 0 | 1},
    c = #{} :: #{pid() => non_neg_integer()},
    p = #{} :: #{pid() => float()},
    sample_count = 0 :: non_neg_integer(),
    max_samples :: pos_integer(),
    r :: float(),
    interval :: pos_integer(),
    beta = 1.0 :: float(),
    update_weights = true :: boolean(),
    init_w = undefined :: float() | undefined,
    clamp = undefined :: undefined | 0 | 1
}).

set_s(Pid, V) when V =:= 0; V =:= 1 ->
    gen_node:cast(Pid, {set_s, V}).
set_w(Pid, Peer, W) ->
    gen_node:cast(Pid, {set_w, Peer, W}).
get_state(Pid) ->
    gen_node:call(Pid, get_state).
get_s(Pid) ->
    gen_node:call(Pid, get_s).

debug(Pid)->
	gen_node:call(Pid,debug).

set_beta(Pid, Beta) ->
    gen_node:cast(Pid, {set_beta, Beta}).
set_beta_gossip(Pid, Beta) ->
    gen_node:gossip(Pid, {set_beta, Beta}).

clamp(Pid, V) when V =:= 0; V =:= 1 ->
    gen_node:cast(Pid, {clamp, V}).
unclamp(Pid) ->
    gen_node:cast(Pid, unclamp).

set_update_weights_gossip(Pid, Bool) ->
    gen_node:gossip(Pid, {set_update_weights, Bool}).

set_update_weights(Pid, Bool) ->
    gen_node:cast(Pid, {set_update_weights, Bool}).

set_p(Pid, Peer, Pj) ->
    gen_node:cast(Pid, {set_p, Peer, Pj}).

init(Args) ->
    MaxSamples = proplists:get_value(max_samples, Args, 100),
    R = proplists:get_value(r, Args, 0.01),
    Interval = proplists:get_value(interval, Args, 1000),
    Beta = proplists:get_value(beta, Args, 1.0),
    UpdateWeights = proplists:get_value(update_weights, Args, true),
    InitW = proplists:get_value(init_w, Args, undefined),
    InitialS = rand:uniform(2) - 1,
    erlang:send_after(Interval, self(), tick),
    {ok, #state{s = InitialS, max_samples = MaxSamples, r = R,
                interval = Interval, beta = Beta,
                update_weights = UpdateWeights, init_w = InitW}}.

handle_call(get_s, _From, State) ->
    {reply, {ok, State#state.s}, State};
handle_call(get_state, _From, State) ->
    {reply, {State#state.s, State#state.w, State#state.p}, State};
handle_call({get_w, Pid}, _From, State) ->
    {reply, maps:get(Pid, State#state.w, 0.0), State};
handle_call(debug,_From,State)->
	{reply,State,State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({set_s, V}, State) ->
    {noreply, State#state{s = V}};
handle_cast({set_w, Pid, W}, State) ->
    {noreply, State#state{w = (State#state.w)#{Pid => W}}};
handle_cast({set_p, Peer, Pj}, State) ->
    {noreply, State#state{p = (State#state.p)#{Peer => Pj}}};
handle_cast({set_beta, Beta}, State) ->
    {noreply, State#state{beta = Beta}};
handle_cast({set_update_weights, Bool}, State) ->
    {noreply, State#state{update_weights = Bool}};
handle_cast({clamp, V}, State) ->
    {noreply, State#state{clamp = V}};
handle_cast(unclamp, State) ->
    {noreply, State#state{clamp = undefined}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_broadcast(FromPid, Data, State) ->
    {noreply, State#state{sj = (State#state.sj)#{FromPid => Data}}}.

handle_gossip(_FromPid, {set_beta, Beta}, State) ->
    {noreply, State#state{beta = Beta}};
handle_gossip(_FromPid, {set_update_weights, Bool}, State) ->
    {noreply, State#state{update_weights = Bool}};
handle_gossip(_FromPid, _Data, State) ->
    {noreply, State}.

handle_ask(_FromPid, get_s, State) ->
    {reply, State#state.s, State};
handle_ask(_FromPid, _Query, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    Peers = gen_node:local_peers(),
    Sj0 = State#state.sj,
    W0 = State#state.w,
    P0 = State#state.p,
    C0 = State#state.c,
    PeerSet = sets:from_list(Peers),
    WDef = case State#state.init_w of undefined -> 0.0; V -> V end,
    InitFun = fun(Pid, {SjA, WA, PA, CA}) ->
        {SjA#{Pid => maps:get(Pid, SjA, 0)},
         WA#{Pid => maps:get(Pid, WA, WDef)},
         PA,
         CA#{Pid => maps:get(Pid, CA, 0)}}
    end,
    {Sj1, W1, P1, C1} = lists:foldl(InitFun, {Sj0, W0, P0, C0}, Peers),
    Keep = fun(Pid, _) -> sets:is_element(Pid, PeerSet) end,
    Sj = maps:filter(Keep, Sj1),
    W = maps:filter(Keep, W1),
    P = maps:filter(Keep, P1),
    C = maps:filter(Keep, C1),
    E = lists:sum([maps:get(Pid, Sj, 0) * maps:get(Pid, W, 0.0) || Pid <- Peers]),
    Prob = 1.0 / (1.0 + math:exp(-State#state.beta * E)),
    NewS0 = case rand:uniform() < Prob of true -> 1; false -> 0 end,
    NewS = case State#state.clamp of undefined -> NewS0; Cv -> Cv end,
    C2 = maps:fold(fun(Pid, PjS, Acc) ->
        case NewS =:= 1 andalso PjS =:= 1 of
            true -> Acc#{Pid => maps:get(Pid, Acc, 0) + 1};
            false -> Acc
        end
    end, C, Sj),
    ok = gen_node:broadcast(self(), NewS),
    Sc = State#state.sample_count + 1,
    case Sc >= State#state.max_samples of
        true ->
            case State#state.update_weights of
                true ->
                    {W2, P2} = maps:fold(fun(Pid, Cj, {Wacc, Pacc}) ->
                        PD = Cj / State#state.max_samples,
                        OldP = maps:get(Pid, Pacc, PD),
                        OldW = maps:get(Pid, Wacc, 0.0),
                        NW = OldW + State#state.r * (PD - OldP),
                        %% Exact: NP = 1 / (1 + (1/OldP - 1) * math:exp(-State#state.beta * (NW - OldW)))
			%%NP = 1 / (1 + (1/OldP - 1) * math:exp(-State#state.beta * (NW - OldW))), 
                        LambdaEff = OldP * (1.0 - OldP) * State#state.beta * State#state.r,
                        NP = LambdaEff * PD + (1.0 - LambdaEff) * OldP,
                        {Wacc#{Pid => NW}, Pacc#{Pid => NP}}
                    end, {W, P}, C2),
                    erlang:send_after(State#state.interval, self(), tick),
                    {noreply, State#state{s = NewS, sj = Sj, w = W2, p = P2,
                                          c = #{}, sample_count = 0}};
                false ->
                    erlang:send_after(State#state.interval, self(), tick),
                    {noreply, State#state{s = NewS, sj = Sj, w = W, p = P,
                                          c = #{}, sample_count = 0}}
            end;
        false ->
            erlang:send_after(State#state.interval, self(), tick),
            {noreply, State#state{s = NewS, sj = Sj, w = W, p = P,
                                  c = C2, sample_count = Sc}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
