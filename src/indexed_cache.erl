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
    {Op :: op(), Field :: field_id(), Value :: value_type()} |
    {range, Field :: field_id(), From :: value_type(), To :: value_type()} |
    {in, Field :: field_id(), Values :: list(value_type())}.
-type op() :: eq | startswith | endswith | like.
-type sort_order() :: asc | desc.
-type field_id() :: pos_integer().

-type objects() :: list(object()).
-type object() :: term().

-type poolid() ::atom().
-type field_types() :: record_(field_type()).
-type field_names() ::record_(field_name()).

-type field_type() :: boolean   | time               | float    | string.
-type value_type() :: boolean() | binary_time_type() | number() | iolist().

-type field_name() :: atom().
-type record_(_) :: tuple(). %% represents erlang record

-type binary_time_type() :: binary().

-type connection_opts() :: list(connection_ep()).
-type connection_ep() :: {Host :: string(), Port :: pos_integer()}.

-define(DEFAULT_CONNECTION_POOL, [{"localhost", 21212}]).

%% API
-export([start/0, stop/0, update/4, get/6, connect/3, connect/4]).

start() ->
    application:ensure_all_started(?MODULE).

-spec connect(Id :: poolid(), FieldTypes :: field_types(), FieldNames :: field_names()) -> any().
connect(Id, FieldTypes, FieldNames) ->
    connect(Id, FieldTypes, FieldNames, ?DEFAULT_CONNECTION_POOL).

-spec connect(Id :: poolid(), FieldTypes :: field_types(), FieldNames :: field_names(), Opts ::connection_opts()) -> any().
connect(Id, FieldTypes, FieldNames, Opts) ->
    indexed_cache_sup:add_connection(Id, FieldTypes, FieldNames, Opts).

stop() ->
    application:stop(?MODULE).

-spec get(PoolId :: atom(), Constrains :: constrains(), SortField :: field_id(), Order :: sort_order(),
    Offset :: non_neg_integer(), Count :: pos_integer()) -> {ok, Objects :: objects()} | {error, Reason :: term()}.
get(PoolId, Constrains, SortField, Order, Offset, Count) ->
    indexed_cache_request:get(PoolId, Constrains, SortField, Order, Offset, Count).

-spec update(PoolId :: atom(), GroupId :: binary(), Update::objects(), Remove :: constrains()) ->
    true | {error, Reason :: term()}.
update(PoolId, GroupId, Update, Remove) ->
    indexed_cache_request:update(PoolId, GroupId, Update, Remove).
