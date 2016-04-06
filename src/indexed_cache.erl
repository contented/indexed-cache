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
    {in, Field :: field_id(), Values :: list(value_type())} |
    {'or', Constrains :: constrains() | constrain()}.
-type op() :: eq | lt | lte | gt | gte | startswith | endswith | like.
-type sort_order() :: asc | desc.
-type field_id() :: pos_integer().

-type objects() :: list(object()).
-type object() :: term().

-type poolid() :: atom().
-type table_name() :: binary().
-type field_types() :: record_(field_type()).
-type field_names() ::record_(field_name()).

-type field_type() :: boolean   | date | datetime    | float    | string.
-type value_type() :: boolean() | binary_time_type() | number() | iolist().

-type field_name() :: atom().
-type record_(_Name) :: tuple(). %% represents any erlang record with elements of specidic type

-type binary_time_type() :: binary().

-type connection_opts() :: list(connection_ep()).
-type connection_ep() :: {Host :: string(), Port :: pos_integer()}.

-define(DEFAULT_CONNECTION_POOL, [{"localhost", 21212}]).
-define(DEFAULT_TABLE_NAME, <<"operations">>).

-export_type([field_names/0, field_types/0]).

%% API
-export([start/0, stop/0, update/3, get/7, connect/3, connect/4, connect/5]).

start() ->
    application:ensure_all_started(?MODULE).

-spec connect(Id :: poolid(), FieldTypes :: field_types(), FieldNames :: field_names()) -> any().
connect(Id, FieldTypes, FieldNames) ->
    connect(Id, ?DEFAULT_TABLE_NAME, FieldTypes, FieldNames, ?DEFAULT_CONNECTION_POOL).

-spec connect(Id :: poolid(), FieldTypes :: field_types(), FieldNames :: field_names(), Opts ::connection_opts()) -> any().
connect(Id, FieldTypes, FieldNames, Opts) ->
    connect(Id, ?DEFAULT_TABLE_NAME, FieldTypes, FieldNames, Opts).

-spec connect(Id :: poolid(), TableName :: table_name(), FieldTypes :: field_types(), FieldNames :: field_names(), Opts ::connection_opts()) -> any().
connect(Id, TableName, FieldTypes, FieldNames, Opts) ->
    indexed_cache_sup:add_connection(Id, TableName, FieldTypes, FieldNames, Opts).

stop() ->
    application:stop(?MODULE).

-spec get(PoolId :: atom(), Constrains :: constrain() | constrains(), SortField :: field_id(), Order :: sort_order(),
    Offset :: non_neg_integer(), Count :: pos_integer(), Aggregations :: list(field_id())) ->
    {ok, Objects :: objects(), TotalCount :: non_neg_integer(), Aggs :: object()} | {error, Reason :: term()}.
get(PoolId, Constrain, SortField, Order, Offset, Count, Aggregations) when is_tuple(Constrain) ->
    get(PoolId, [Constrain], SortField, Order, Offset, Count, Aggregations);
get(PoolId, Constrains, SortField, Order, Offset, Count, Aggregations) when is_list(Constrains) ->
    try
        indexed_cache_request:get(PoolId, Constrains, SortField, Order, Offset, Count, Aggregations)
    catch
        Exception ->
            {error, Exception}
    end.

-spec update(PoolId :: atom(), GroupId :: binary(), Update::objects()) ->
    true.
update(PoolId, GroupId, Update) ->
    indexed_cache_request:update(PoolId, GroupId, Update).
