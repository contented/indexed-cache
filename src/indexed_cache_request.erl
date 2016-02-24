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
    %% Assuming reading is not so frequent job, will just generate Ad hock queries.
    %% According to the documentation, they are slower mostly because they have to compile before execution
    %% In our case compilation time impact is not critical.
    FieldNames = indexed_cache_connection:field_names(PoolId),
    SortFieldName = atom_to_binary(element(SortField, FieldNames), utf8),
    case erlvolt:call_procedure(PoolId, "GetData", make_query(Constrains, SortFieldName, Order, Offset, Count)) of
        {result, {voltresponse, {0, _, 1, <<>>, 128, <<>>, <<>>, _}, [{volttable,_,_,Rows}]}} ->
            FieldTypes = indexed_cache_connection:field_types(PoolId),
            {ok, deserialize_objects(element(1, FieldNames), FieldTypes, Rows)};
        ?VOLT_ERROR_MESSAGE(T) ->
            {error, T}
    end.

make_query(Constrains, SortField, Order, Offset, Count) ->
    {QueryParts, Substitutions} = make_constrains(Constrains),
    Query = [
        <<"SELECT * FROM rows ">>,
        QueryParts,
        <<"ORDER BY ">>, SortField, <<" ">>,
        if
            Order == asc -> <<"ASC ">>;
            Order == desc -> <<"DESC">>
        end,
        <<"LIMIT ">>, integer_to_binary(Count), <<" ">>,
        <<"OFFSET ">>, integer_to_binary(Offset)
    ],
    [iolist_to_binary(Query) , params_to_stringlist(Substitutions)].

params_to_stringlist(List) ->
    {?VOLT_ARRAY, {voltarray, encode_type(string), lists:map(fun param_to_string/1, List)}}.

param_to_string(true) ->
    <<"1">>;
param_to_string(false) ->
    <<"0">>;
param_to_string(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
param_to_string(Else) ->
    iolist_to_binary(Else).


make_constrains([]) ->
    {[], []};
make_constrains(Constrains) ->
    {[QHead | QRest], [SHead | SRest]} = lists:unzip(lists:map(fun make_constrain/1, Constrains)),
    {[
        <<"WHERE ">>,
        lists:map(fun(QPart) -> [QPart, <<" AND ">>] end, QRest),
        QHead,
        <<" ">>
    ], SRest ++ SHead}. %% DO NOT MESS UP QueryParts and their substitutuions

make_constrain({eq, Field, Value}) ->
    {[Field, <<" == ?">>], [Value]};
make_constrain({startswith, Field, Value}) ->
    {[Field, <<" LIKE ?%">>], [Value]};
make_constrain({endswith, Field, [Value]}) ->
    {[Field, <<" LIKE %?">>], [Value]};
make_constrain({like, Field, Value}) ->
    {[Field, <<" LIKE %?%">>], [Value]};
make_constrain({range, Field, From, To}) ->
    {[Field, <<" BETWEEN ? AND ?">>], [From, To]};
make_constrain({in, Field, Values}) ->
    {[Field, <<" IN ?">>], [Values]}.

deserialize_objects(RecordName, FiledTypes, Rows) ->
    [list_to_tuple([RecordName | deserialize_object(FiledTypes, Row)]) || Row <- Rows].

deserialize_object(FieldTypes, {voltrow, Row}) ->
    [deserialize_type(T, V) || {T, V} <- lists:zip(FieldTypes, Row)].

deserialize_type(_, null) -> undefined;
deserialize_type(boolean, 0) -> false;
deserialize_type(boolean, 1) -> true;
deserialize_type(time, Timestamp) -> calendar:now_to_universal_time(Timestamp);
deserialize_type(_, Value) -> Value.

%%%
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
    FieldTypes = indexed_cache_connection:field_types(PoolId),
    Update3 = [{?VOLT_ARRAY, preserialize(ItemType, Item)} || {ItemType, Item} <- lists:zip(FieldTypes, Update2)],
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