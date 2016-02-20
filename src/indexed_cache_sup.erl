%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Feb 2016 13:06
%%%-------------------------------------------------------------------
-module(indexed_cache_sup).
-author("lol4t0").

-behaviour(supervisor).

%% API
-export([start_link/0, add_connection/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 50,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {ok, {SupFlags, []}}.

add_connection(Pool, FieldsMapping, Params) ->
    ConnectionWorker = {Pool, {indexed_cache_connection, start_link, [Pool, FieldsMapping, Params]},
        permanent, 2000, worker, [indexed_cache_connection]},
    supervisor:start_child(?MODULE, ConnectionWorker).

