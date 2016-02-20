%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Feb 2016 11:55
%%%-------------------------------------------------------------------
-module(indexed_cache).
-author("lol4t0").

-type constrains() :: list(constrain()).
-type constrain() ::
    {Op :: op(), Field :: field_name(), Value :: binary()} |
    {range, Field :: field_name(), Field :: field_name(), Value :: binary()} |
    {in, Field :: field_name(), [Value :: binary()]}.
-type op() :: eq | startswith | endswith | like.
-type sort_order() :: asc | desc.
-type field_name() :: pos_integer().

-type objects() :: list(object()).
-type object() :: term().

-type poolid() ::atom().
-type fields_mapping() :: record(field_type()).
-type field_type() :: boolean | time | float | string.
-type record(_) :: tuple(). %% represents erlang record

-type connection_opts() :: list(connection_ep()).
-type connection_ep() :: {Host :: string(), Port :: pos_integer()}.

-define(DEFAULT_CONNECTION_POOL, [{"localhost", 21212}]).

%% API
-export([start/0, stop/0, update/4, get/6, connect/2, connect/3]).

start() ->
    application:ensure_all_started(?MODULE).

-spec connect(Id :: poolid(), FieldsMapping :: fields_mapping()) -> any().
connect(Id, FieldsMapping) ->
    connect(Id, FieldsMapping, ?DEFAULT_CONNECTION_POOL).

-spec connect(Id :: poolid(), FieldsMapping :: fields_mapping(), Opts ::connection_opts()) -> any().
connect(Id, FieldsMapping, Opts) ->
    indexed_cache_sup:add_connection(Id, FieldsMapping, Opts).

stop() ->
    application:stop(?MODULE).

-spec get(PoolId :: atom(), Constrains :: constrains(), SortField :: field_name(), Order :: sort_order(),
    Offset :: non_neg_integer(), Count :: pos_integer()) -> {ok, Objects :: objects()} | {error, Reason :: term()}.
get(PoolId, Constrains, SortField, Order, Offset, Count) ->
    indexed_cache_request:get(PoolId, Constrains, SortField, Order, Offset, Count).

-spec update(PoolId :: atom(), GroupId :: binary(), Update::objects(), Remove :: constrains()) ->
    true | {error, Reason :: term()}.
update(PoolId, GroupId, Update, Remove) ->
    indexed_cache_request:update(PoolId, GroupId, Update, Remove).
