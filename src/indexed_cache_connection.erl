%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Feb 2016 12:52
%%%-------------------------------------------------------------------
-module(indexed_cache_connection).
-author("lol4t0").

-behaviour(gen_server).

-export([start_link/5, init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3, format_status/2,
    table_name/1, field_types/1, field_names/1]).

-record(state, {pool_id, table_name, field_types, field_names}).

start_link(PoolId, TableName, Fieldtypes, FieldNames, ConnectionOpts) ->
    gen_server:start_link({local, PoolId}, ?MODULE, [PoolId, TableName, Fieldtypes, FieldNames, ConnectionOpts], []).

init([PoolId, TableName, FieldTypes, FieldNames, ConnectionOpts]) when is_binary(TableName), is_tuple(FieldTypes), is_tuple(FieldNames) ->
    process_flag(trap_exit, true),
    ok = erlvolt:add_pool(PoolId, ConnectionOpts),
    {ok, #state{pool_id = PoolId, table_name = TableName, field_types = FieldTypes, field_names = FieldNames}}.

field_types(PoolId) ->
    gen_server:call(PoolId, field_types).

field_names(PoolId) ->
    gen_server:call(PoolId, field_names).

table_name(PoolId) ->
    gen_server:call(PoolId, table_name).

terminate(_Reason, #state{pool_id = PoolId}) ->
    erlvolt:close_pool(PoolId).


handle_call(table_name, _From, State = #state{table_name = TableName}) ->
    {reply, TableName, State};
handle_call(field_types, _From, State = #state{field_types = FiledTypes}) ->
    {reply, FiledTypes, State};
handle_call(field_names, _From, State = #state{field_names = FiledNames}) ->
    {reply, FiledNames, State}.

handle_cast(_Request, _State) -> erlang:error(not_implemented).
handle_info(_Info, _State) -> erlang:error(not_implemented).
code_change(_OldVsn, _State, _Extra) -> erlang:error(not_implemented).
format_status(_Opt, _StatusData) -> erlang:error(not_implemented).
