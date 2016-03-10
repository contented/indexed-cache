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
-export([get/7, update/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

get(PoolId, Constrains, SortField, Order, Offset, Count, Aggregations) ->
    %% Assuming reading is not so frequent job, will just generate Ad hock queries.
    %% According to the documentation, they are slower mostly because they have to compile before execution
    %% In our case compilation time impact is not critical.
    FieldNames = indexed_cache_connection:field_names(PoolId),
    FieldTypes = indexed_cache_connection:field_types(PoolId),
    SortFieldName = field_name(FieldNames, SortField),
    Query = make_query(FieldNames, FieldTypes, Constrains, SortFieldName, Order, Offset, Count, Aggregations),
    case erlvolt:call_procedure(PoolId, "GetData", Query) of
        {result, {voltresponse, {0, _, 1, <<>>, 128, <<>>, <<>>, _}, [
            {volttable,_,_,Rows},
            {volttable,_,_,AggregationRes}
        ]}} ->
            {TotalCount, Aggs} = deserialize_aggregations(FieldNames, Aggregations, AggregationRes),
            {ok,
                deserialize_objects(element(1, FieldNames), types_list(FieldTypes), Rows),
                TotalCount,
                Aggs
            };
        {result,{voltresponse,{_,_,_,T,_,_,_,_},[]}} ->
            {error, T}
    end.

deserialize_aggregations(RecordInfo, _, {voltrow, [Count]}) ->
    {Count, make_empty_record(RecordInfo)};
deserialize_aggregations(RecordInfo, FieldIds, [{voltrow, [Count | Data]}]) ->
    Empty = make_empty_record(RecordInfo),
    {Count, lists:foldl(fun({K, V}, Acc) -> setelement(K, Acc, V) end, Empty, lists:zip(FieldIds, Data))}.

make_empty_record(RecordInfo) ->
    erlang:make_tuple(size(RecordInfo), undefined, [{1, element(1, RecordInfo)}]).

types_list(TypesRecord) ->
    tl(tuple_to_list(TypesRecord)).

field_name(FieldNames, FieldId) ->
    atom_to_binary(element(FieldId, FieldNames), utf8).

field_type(FieldTypes, FieldId) when is_tuple(FieldTypes) ->
    element(FieldId, FieldTypes).

make_query(FieldNames, FieldTypes, Constrains, SortField, Order, Offset, Count, Aggregations) ->
    {QueryParts, Substitutions} = make_top_constrains(FieldNames, FieldTypes, Constrains),
    Query = [
        <<"SELECT * FROM rows ">>,
        QueryParts,
        <<"ORDER BY ">>, SortField, <<" ">>,
        if
            Order == asc -> <<"ASC ">>;
            Order == desc -> <<"DESC ">>
        end,
        <<"LIMIT ">>, integer_to_binary(Count), <<" ">>,
        <<"OFFSET ">>, integer_to_binary(Offset)
    ],
    AggsQuery = if
                    Aggregations =/= [] ->
                        [
                            <<"SELECT ">>, make_aggs_query_part(FieldNames, Aggregations), <<" FROM rows ">>,
                            QueryParts
                        ];
                    Aggregations == [] ->
                        [
                            <<"SELECT COUNT(*) FROM rows ">>,
                            QueryParts
                        ]
    end,
    [iolist_to_binary(Query) , iolist_to_binary(AggsQuery), params_to_stringlist(Substitutions)].

make_aggs_query_part(FieldNames, Aggregations) ->
    Sums = [ [<<"SUM(">>, field_name(FieldNames, Field), <<")">>] || Field <- Aggregations],
    [<<"COUNT(*)">>, [[",", E] || E <- Sums]].


params_to_stringlist(List) ->
    {?VOLT_ARRAY, {voltarray, encode_type(string), lists:map(fun param_to_string/1, List)}}.

param_to_string({Type, Value}) ->
    param_to_string(Type, Value).

param_to_string(boolean, true) ->
    <<"1">>;
param_to_string(boolean, false) ->
    <<"0">>;
param_to_string(string, Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
param_to_string(float, Num) when is_integer(Num) ->
    integer_to_binary(Num);
param_to_string(float, Num) when is_float(Num) ->
    float_to_binary(Num, [{decimals, 12}, compact]);
param_to_string(time, BinaryTime) when is_binary(BinaryTime)->
    string_volt_time(convert(time, BinaryTime));
param_to_string(string, Else) ->
    iolist_to_binary(Else).

string_volt_time({Date,Time}) ->
    UnixEpoch = {{1970,1,1},{0,0,0}},
    TS = calendar:datetime_to_gregorian_seconds({Date,Time}) - calendar:datetime_to_gregorian_seconds(UnixEpoch),
    integer_to_binary(TS).


make_top_constrains(_, _, []) ->
    {[], []};
make_top_constrains(FieldNames, FieldTypes, Constrains) ->
    {Qry, Substs} = make_constrains(FieldNames, FieldTypes, Constrains),
    {[<<"WHERE ">>, Qry], Substs}.

make_constrains(FieldNames, FieldTypes, Constrains) ->
    Qry = lists:unzip([make_constrain(FieldNames, FieldTypes, Constrain) || Constrain <- Constrains]),
    join_qry_parts(<<" AND ">>, Qry).

join_qry_parts(_Op, {[], _}) ->
    {[], []};
join_qry_parts(Op, {[QHead | QRest], Substs}) ->
    {[
        QHead,
        lists:map(fun(QPart) -> [Op, QPart] end, QRest),
        <<" ">>
    ], lists:append(Substs)}.

make_constrain(FieldNames, FieldTypes, {Op, Field, Value}) when Op ==eq; Op == lt; Op == lte; Op == gt; Op == gte->
    FieldType = field_type(FieldTypes, Field),
    {[field_name(FieldNames, Field), sym_for_op(Op), mb_cast(FieldType)], [{FieldType, Value}]};
make_constrain(FieldNames, FieldTypes, {startswith, Field, Value}) ->
    FieldType = field_type(FieldTypes, Field),
    {[field_name(FieldNames, Field), <<" LIKE ?">>], [{FieldType, [Value, <<"%">>]}]};
make_constrain(FieldNames, FieldTypes, {endswith, Field, Value}) ->
    FieldType = field_type(FieldTypes, Field),
    {[field_name(FieldNames, Field), <<" LIKE ?">>], [{FieldType, [<<"%">>, Value]}]};
make_constrain(FieldNames, FieldTypes, {like, Field, Value}) ->
    FieldType = field_type(FieldTypes, Field),
    {[field_name(FieldNames, Field), <<" LIKE ?">>], [{FieldType, [<<"%">>, Value, <<"%">>]}]};
make_constrain(FieldNames, FieldTypes, {range, Field, From, To}) ->
    FieldType = field_type(FieldTypes, Field),
    Cast = mb_cast(FieldType),
    {[field_name(FieldNames, Field), <<" BETWEEN ", Cast/binary, " AND ", Cast/binary>>], [{FieldType, From}, {FieldType, To}]};
make_constrain(FieldNames, FieldTypes, {in, Field, Values}) when is_list(Values), length(Values) > 0 ->
    FieldType = field_type(FieldTypes, Field),
    Cast = mb_cast(FieldType),
    Substs = [{FieldType, V} || V <- Values],
    {[field_name(FieldNames, Field), <<" IN (">>, [<<Cast/binary,",">> || _ <- tl(Values)], Cast, <<")">>], Substs};
make_constrain(FieldNames, FieldTypes, {'or', ConstrainsList}) ->
    Qry = lists:unzip(lists:map(fun
        (Constrains) when is_list(Constrains)-> make_constrains(FieldNames, FieldTypes, Constrains); %% Recursive building
        (Constrain) -> make_constrain(FieldNames, FieldTypes, Constrain)
    end, ConstrainsList)),
    {QParts, Substs} = join_qry_parts(<<" OR ">>, Qry),
    {[<<" ( ">>, QParts, <<" ) ">>], Substs};
make_constrain(_FieldNames, _FieldTypes, Constrain) ->
    throw({invalid_constrain, Constrain}).

sym_for_op(Op) ->
    case Op of
        eq ->  <<" = ">>;

        lt ->  <<" < ">>;
        lte -> <<" <= ">>;

        gt ->  <<" > ">>;
        gte -> <<" >= ">>
    end.


mb_cast(string) -> <<"?">>;
mb_cast(boolean) -> <<"CAST(? AS INTEGER)">>;
mb_cast(float) -> <<"CAST(? AS DECIMAL)">>;
mb_cast(time) -> <<"TO_TIMESTAMP(SECOND, CAST(? AS BIGINT))">>.

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

update(PoolId, GroupId, Update) ->
    Update1 = [ begin
                    [_tag | Data] = tuple_to_list(Item),
                    Data
                end || Item <- Update
    ],
    Update2 = transpose(Update1),
    FieldTypes = types_list(indexed_cache_connection:field_types(PoolId)),
    Update3 = [{?VOLT_ARRAY, preserialize(ItemType, Item)} || {ItemType, Item} <- lists:zip(FieldTypes, Update2)],
    case erlvolt:call_procedure(PoolId, "UpdateData", [GroupId] ++ Update3) of
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