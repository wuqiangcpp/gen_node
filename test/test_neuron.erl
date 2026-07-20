-module(test_neuron).
-export([test/0, test_ising/0, discover_topology/0, handle_set_beta/2,
         handle_start_training/3, handle_stop_training/3,
         handle_set_inputs/3, test_growing/0, start_network/0, add_node/1,
         test_boltzmann/0, save_network/1, load_network/1,
         test_eval/3, test_eval/4]).

test() ->
    io:format("=== Boltzmann machine test ===~n~n"),
    Pids = gen_node:start_grid(3, 3, neuron,
               [{max_samples, 50}, {interval, 10}, {r, 0.05},
                {update_weights, false}], symmetric,
               [{dead_timeout, 86400000}]),
    io:format("Grid started: ~p nodes~n", [length(Pids)]),
    gen_node_monitor:start_link([
        {seeds, Pids}, {request, get_state}, {interval, 1000}
    ]),
    [In1, Out, In2 | _] = Pids,
    gen_node_ws:start(#{
        <<"get">> => {?MODULE, discover_topology, []},
        <<"set_inputs">> => {?MODULE, handle_set_inputs, [In1, In2]},
        <<"start_training">> => {?MODULE, handle_start_training, [In1, In2, Out]},
        <<"stop_training">> => {?MODULE, handle_stop_training, [In1, In2, Out]}
    }, 8081),
    io:format("WS: http://localhost:8081/show_neuron.html~n"),
    io:format("XOR: In1=~p, In2=~p, Out=~p~n", [In1, In2, Out]),
    io:format("(Boltzmann machine: update_weights=true, interval=50ms)~n"),
    timer:sleep(infinity).

test_boltzmann() ->
    io:format("=== Boltzmann machine test (bipartite) ===~n~n"),
    Args = [{max_samples, 10}, {interval, 50}, {r, 0.01},
            {update_weights, true}],
    Opts = [{gossip_enabled, false}, {dead_timeout, 86400000}],
    {ok, N1} = gen_node:start_link(neuron, Args, Opts),
    {ok, N2} = gen_node:start_link(neuron, Args, Opts),
    {ok, N3} = gen_node:start_link(neuron, Args, Opts),
    {ok, H1} = gen_node:start_link(neuron, Args, Opts),
    {ok, H2} = gen_node:start_link(neuron, Args, Opts),
    {ok, H3} = gen_node:start_link(neuron, Args, Opts),
    {ok, H4} = gen_node:start_link(neuron, Args, Opts),
    L1 = [N1, N2, N3],
    L2 = [H1, H2, H3, H4],
    [gen_node:join(P1, L2) || P1 <- L1],
    [gen_node:join(P2, L1) || P2 <- L2],
    Pids = L1 ++ L2,
    io:format("~p nodes started (3 L1 + 4 L2)~n", [length(Pids)]),
    gen_node_monitor:start_link([
        {seeds, Pids}, {request, get_state}, {interval, 1000}
    ]),
    [In1, In2, Out | _] = Pids,
    gen_node_ws:start(#{
        <<"get">> => {?MODULE, discover_topology, []},
        <<"set_inputs">> => {?MODULE, handle_set_inputs, [In1, In2]},
        <<"start_training">> => {?MODULE, handle_start_training, [In1, In2, Out]},
        <<"stop_training">> => {?MODULE, handle_stop_training, [In1, In2, Out]}
    }, 8081),
    io:format("WS: http://localhost:8081/show_neuron.html~n"),
    io:format("XOR: In1=~p, In2=~p, Out=~p~n", [In1, In2, Out]),
    io:format("(bipartite: 3 L1 ↔ 4 L2, update_weights=false, interval=50ms)~n"),
    ok.
    %timer:sleep(infinity).

start_network() ->
    io:format("=== Start network ===~n"),
    NeuronArgs = [{max_samples, 50}, {interval, 50}, {r, 0.1}],
    {ok, Seed} = gen_node:start_link(neuron, NeuronArgs,
        [{gossip_enabled, true}, {dead_timeout, 86400000},
         {active_view_size, 3}]),
    gen_node_monitor:start_link([
        {seeds, [Seed]}, {request, get_state}, {interval, 2000}
    ]),
    gen_node_ws:start(#{
        <<"get">> => {?MODULE, discover_topology, []}
    }, 8081),
    io:format("Seed=~p, WS: http://localhost:8081/show_neuron.html~n", [Seed]),
    Seed.

add_node(Seed) ->
    NeuronArgs = [{max_samples, 50}, {interval, 50}, {r, 0.1}],
    {ok, Pid} = gen_node:start_link(neuron, NeuronArgs,
        [{gossip_enabled, true}, {dead_timeout, 86400000},
         {active_view_size, 3}]),
    gen_node:join(Pid, [Seed]),
    gen_node_monitor:add_node(Pid),
    io:format("Added ~p~n", [Pid]),
    Pid.

test_growing() ->
    io:format("=== Growing network with gossip ===~n~n"),
    NeuronArgs = [{max_samples, 50}, {interval, 50}, {r, 0.1}],
    io:format("Starting seed node...~n"),
    {ok, Seed} = gen_node:start_link(neuron, NeuronArgs,
        [{gossip_enabled, true},{active_view_size,4} ]),

    Pids = [Seed | lists:map(fun(N) ->
        timer:sleep(1000),
        io:format("Starting node ~p/9...~n", [N]),
        {ok, Pid} = gen_node:start_link(neuron, NeuronArgs,
            [{gossip_enabled, true},{active_view_size,4}]),
        gen_node:join(Pid, [Seed]),
        Pid
    end, lists:seq(2, 9))],
    io:format("All ~p nodes started, waiting for gossip propagation...~n", [length(Pids)]),
    timer:sleep(2000),
    [In1, In2 | _] = Pids,
    Out = lists:last(Pids),
    gen_node_monitor:start_link([
        {seeds, Pids}, {request, get_state}, {interval, 1000}
    ]),
    gen_node_ws:start(#{
        <<"get">> => {?MODULE, discover_topology, []},
        <<"set_inputs">> => {?MODULE, handle_set_inputs, [In1, In2]},
        <<"start_training">> => {?MODULE, handle_start_training, [In1, In2, Out]},
        <<"stop_training">> => {?MODULE, handle_stop_training, [In1, In2, Out]}
    }, 8081),
    io:format("WS: http://localhost:8081/show_neuron.html~n"),
    io:format("XOR: In1=~p, In2=~p, Out=~p~n", [In1, In2, Out]),
    io:format("(gossip-enabled, 9 nodes growing over ~p seconds)~n~n", [8]),
    ok.
    %%timer:sleep(infinity).

handle_start_training(In1, In2, Out) ->
    [neuron:set_update_weights(P, true) || {P, _} <- gen_node_monitor:get_nodes()],
    case whereis(xor_trainer) of
        undefined ->
            spawn(fun() ->
                register(xor_trainer, self()),
                xor_train_loop(5000, 0, In1, In2, Out)
            end),
            #{ok => true};
        _ -> #{error => <<"already running">>}
    end.

handle_set_inputs(In1, In2, Value) when is_map(Value) ->
    V1 = maps:get(<<"in1">>, Value, 0),
    V2 = maps:get(<<"in2">>, Value, 0),
    neuron:clamp(In1, V1),
    neuron:clamp(In2, V2),
    #{ok => true, in1 => V1, in2 => V2}.

handle_stop_training(In1, In2, Out) ->
    [neuron:set_update_weights(P, false) || {P, _} <- gen_node_monitor:get_nodes()],
    neuron:unclamp(In1),
    neuron:unclamp(In2),
    neuron:unclamp(Out),
    case whereis(xor_trainer) of
        undefined -> #{error => <<"not running">>};
        Pid -> exit(Pid, kill), #{ok => true}
    end.

xor_train_loop(Interval, Step, In1, In2, Out) ->
    Patterns = [{0,0,0}, free, {0,1,1}, free, {1,0,1}, free, {1,1,0}, free],
    case lists:nth((Step rem 8) + 1, Patterns) of
        free ->
            neuron:unclamp(In1), neuron:unclamp(In2), neuron:unclamp(Out);
        {X1, X2, Y} ->
            neuron:clamp(In1, X1), neuron:clamp(In2, X2), neuron:clamp(Out, Y),
            io:format("[xor] step ~p: ~p ~p → ~p~n", [Step, X1, X2, Y])
    end,
    timer:sleep(Interval),
    xor_train_loop(Interval, Step + 1, In1, In2, Out).

test_ising() ->
    io:format("=== Ising model test ===~n~n"),
    Pids = gen_node:start_grid(4, 4, neuron,
               [{beta, 0.4}, {max_samples, 1}, {update_weights, false},
                {init_w, 1.0}, {interval, 500}], symmetric,
               [{dead_timeout, 86400000}]),
    io:format("Grid started: ~p nodes~n", [length(Pids)]),
    gen_node_monitor:start_link([
        {seeds, Pids}, {request, get_state}, {interval, 500}
    ]),
    [Seed | _] = Pids,
    gen_node_ws:start(#{
        <<"get">> => {?MODULE, discover_topology, []},
        <<"set_beta">> => {?MODULE, handle_set_beta, [Seed]}
    }, 8081),
    io:format("WS: http://localhost:8081/show_neuron.html~n"),
    timer:sleep(infinity).

handle_set_beta(Seed, Beta) when is_number(Beta) ->
    io:format("[neuron] β set to ~.1f via ~p~n", [Beta, Seed]),
    neuron:set_beta_gossip(Seed, Beta),
    #{ok => true, beta => Beta}.

discover_topology() ->
    AllNodes = gen_node_monitor:get_nodes(),
    Nodes = [{P, M} || {P, #{last_reply := {_, _, _}} = M} <- AllNodes],
    NodeList = [#{id => pid_to_binary(P), props => #{s => S}}
                || {P, #{last_reply := {S, _, _}}} <- Nodes],
    LinkList = lists:flatmap(fun({P, #{peers := Ps, last_reply := {_, W, Pj}}}) ->
        [#{from => pid_to_binary(P), to => pid_to_binary(Q),
           props => #{weight => maps:get(Q, W, 0), pj => maps:get(Q, Pj, 0)}}
         || Q <- Ps, Q =/= P]
    end, Nodes),
    Info = list_to_binary(io_lib:format("~p nodes, ~p links (directed)",
        [length(NodeList), length(LinkList)])),
    #{nodes => NodeList, links => LinkList, info => Info}.

pid_to_binary(Pid) ->
    list_to_binary(pid_to_list(Pid)).

save_network(FilePath) ->
    AllNodes = gen_node_monitor:get_nodes(),
    Pids = [P || {P, _} <- AllNodes],
    Idx = maps:from_list(lists:zip(Pids, lists:seq(0, length(Pids)-1))),
    Data = lists:map(fun(P) ->
        {_S, W, Pj} = neuron:get_state(P),
        W1 = [{maps:get(Pid, Idx), V} || {Pid, V} <- maps:to_list(W)],
        P1 = [{maps:get(Pid, Idx), V} || {Pid, V} <- maps:to_list(Pj)],
        #{idx => maps:get(P, Idx), w => W1, p => P1}
    end, Pids),
    file:write_file(FilePath, term_to_binary(Data)),
    io:format("Saved ~p nodes to ~s~n", [length(Data), FilePath]).

load_network(FilePath) ->
    {ok, Bin} = file:read_file(FilePath),
    Data = binary_to_term(Bin),
    AllNodes = gen_node_monitor:get_nodes(),
    Pids = [P || {P, _} <- AllNodes],
    Idx = maps:from_list(lists:zip(lists:seq(0, length(Pids)-1), Pids)),
    lists:foreach(fun(#{idx := I, w := W, p := P}) ->
        Pid = maps:get(I, Idx),
        [neuron:set_w(Pid, maps:get(J, Idx), V) || {J, V} <- W],
        [neuron:set_p(Pid, maps:get(J, Idx), V) || {J, V} <- P]
    end, Data),
    io:format("Loaded ~p nodes from ~s~n", [length(Data), FilePath]).

test_eval(In1, In2, Out) ->
    test_eval(In1, In2, Out, 10).

test_eval(In1, In2, Out, Trials) ->
    io:format("~n=== Evaluating XOR accuracy ===~n"),
    Patterns = [{0,0,0}, {0,1,1}, {1,0,1}, {1,1,0}],
    lists:foreach(fun({X1, X2, Expected}) ->
        neuron:clamp(In1, X1), neuron:clamp(In2, X2),
        timer:sleep(200),
        Counts = [begin
            timer:sleep(50),
            {S, _, _} = neuron:get_state(Out), S
        end || _ <- lists:seq(1, Trials)],
        P = lists:sum(Counts) / length(Counts),
        Icon = case abs(P - Expected) < 0.3 of
            true -> "OK"; false -> "XX"
        end,
        io:format("  ~s In=(~p,~p) ~p P(Out=1)=~.2f (expect ~p)~n",
                  [Icon, X1, X2, hd(Counts), P, Expected])
    end, Patterns),
    ok.
