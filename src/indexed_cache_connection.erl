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

-export([start_link/3, init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3, format_status/2,
    fields_mapping/1]).

-record(state, {pool_id, fields_mapping}).

start_link(PoolId, FieldsMapping, ConnectionOpts) ->
    gen_server:start_link({local, PoolId}, ?MODULE, [PoolId, FieldsMapping, ConnectionOpts], []).

init([PoolId, FieldsMapping, ConnectionOpts]) when is_tuple(FieldsMapping) ->
    process_flag(trap_exit, true),
    ok = erlvolt:add_pool(PoolId, ConnectionOpts),
    [_tag | FieldsMapping1] = tuple_to_list(FieldsMapping),
    {ok, #state{pool_id = PoolId, fields_mapping = FieldsMapping1}}.

fields_mapping(PoolId) ->
    gen_server:call(PoolId, fields_mapping).

terminate(_Reason, #state{pool_id = PoolId}) ->
    erlvolt:close_pool(PoolId).


handle_call(fields_mapping, _From, State = #state{fields_mapping = FiledsMapping}) ->
    {reply, FiledsMapping, State}.

handle_cast(_Request, _State) -> erlang:error(not_implemented).
handle_info(_Info, _State) -> erlang:error(not_implemented).
code_change(_OldVsn, _State, _Extra) -> erlang:error(not_implemented).
format_status(_Opt, _StatusData) -> erlang:error(not_implemented).
