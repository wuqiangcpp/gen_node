-module(gen_node_ws).
-behaviour(cowboy_websocket).

-export([start/1, start/2, stop/0]).
-export([init/2, websocket_init/1, websocket_handle/2, websocket_info/2]).

-define(DEFAULT_PORT, 8081).

-type handler() :: fun(() -> term()) | {module(), atom(), list()}.
-type handlers() :: #{binary() => handler()}.

-record(ws_state, {handlers :: handlers()}).

start(Handlers) -> start(Handlers, ?DEFAULT_PORT).

start(Handlers, Port) ->
    {ok, _} = application:ensure_all_started(cowboy),
    {ok, Cwd} = file:get_cwd(),
    DocRoot = filename:join(Cwd, "client"),
    Dispatch = cowboy_router:compile([{'_', [
        {"/ws", ?MODULE, #{handlers => Handlers}},
        {"/[...]", cowboy_static, {dir, DocRoot}}
    ]}]),
    {ok, _} = cowboy:start_clear(?MODULE, [{port, Port}], #{
        env => #{dispatch => Dispatch}
    }).

stop() ->
    cowboy:stop_listener(?MODULE).

init(Req, #{handlers := Handlers}) ->
    {cowboy_websocket, Req, #ws_state{handlers = Handlers}, #{
        idle_timeout => 60000
    }}.

websocket_init(State) ->
    {[], State, hibernate}.

websocket_handle({text, Msg}, State = #ws_state{handlers = Hs}) ->
    Result = try
        Map = jsx:decode(Msg, [return_maps]),
        Cmd = maps:get(<<"cmd">>, Map),
        Args = case maps:get(<<"value">>, Map, undefined) of
            undefined -> [];
            V -> [V]
        end,
        case maps:find(Cmd, Hs) of
            {ok, {M, F, A}} -> apply(M, F, A ++ Args);
            {ok, Fun} when is_function(Fun) -> Fun();
            error -> #{error => <<"unknown command: ", Cmd/binary>>}
        end
    catch _:_ ->
        #{error => <<"invalid json">>}
    end,
    {reply, {text, jsx:encode(Result)}, State};
websocket_handle(_Frame, State) ->
    {[], State, hibernate}.

websocket_info(_Info, State) ->
    {[], State, hibernate}.
