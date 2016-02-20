%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Feb 2016 15:38
%%%-------------------------------------------------------------------
-module(indexed_cache_request).
-author("lol4t0").

-include_lib("eunit/include/eunit.hrl").
-include_lib("erlvolt/include/erlvolt_wire.hrl").
-include_lib("erlvolt/include/erlvolt.hrl").

%% API
-export([get/6, update/4]).



get(PoolId, Constrains, SortField, Order, Offset, Count) ->
    ok.

transpose([[]|_]) -> [];
transpose(M) ->
    [lists:map(fun hd/1, M) | transpose(lists:map(fun tl/1, M))].

update(PoolId, GroupId, Update, Remove) ->
    Update1 = [ begin
                    [_tag | Data] = tuple_to_list(Item),
                    Data
                end || Item <- Update
    ],
    Update2 = transpose(Update1),
    FieldsMapping = indexed_cache_connection:fields_mapping(PoolId),
    Update3 = [{?VOLT_ARRAY, preserialize(ItemType, Item)} || {ItemType, Item} <- lists:zip(FieldsMapping, Update2)],
    case erlvolt:call_procedure(PoolId, "UpdateData", [GroupId, {?VOLT_ARRAY, Remove}] ++ Update3) of
        {result, {voltresponse, {0, _, 1, <<>>, 128, <<>>, <<>>, _}, _}} ->
            true;
        ?VOLT_ERROR_MESSAGE(Msg) ->
            {error, Msg}
    end.

%% Possible types are STRING, FLOAT, TIME and BOOL, so this is not gonna be difficult.
%% The only thing we should care is NULL values
preserialize(DataType, Data) ->
    {voltarray, encode_type(DataType), [convert(DataType, Item) || Item <- Data]}.

convert(boolean, true) ->
    1;
convert(boolean, false) ->
    0;
convert(float, Num) when is_number(Num) ->
    Num;
convert(float, undefined) ->
    null;
convert(string, undefined) ->
    null;
convert(string, Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
convert(string, Else) ->
    Else;
convert(time, undefined) ->
    calendar:universal_time();
convert(time, <<Y:4/binary, M:2/binary, D:2/binary>>) ->
    {i_tuple([Y, M, D]), {0,0,0}};
convert(time, <<Y:4/binary, M:2/binary, D:2/binary, H:2/binary, Min:2/binary, Sec:2/binary, _/binary>>) ->
    {i_tuple([Y, M, D]), i_tuple([H, Min, Sec])}.

i_tuple(L) -> list_to_tuple(lists:map(fun erlang:binary_to_integer/1, L)).


encode_type(boolean) -> ?VOLT_INTEGER;
encode_type(float) -> ?VOLT_DECIMAL;
encode_type(string) -> ?VOLT_STRING;
encode_type(time) -> ?VOLT_TIMESTAMP.