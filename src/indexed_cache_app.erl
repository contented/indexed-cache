%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Feb 2016 11:56
%%%-------------------------------------------------------------------
-module(indexed_cache_app).
-author("lol4t0").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    indexed_cache_sup:start_link().


-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok.
