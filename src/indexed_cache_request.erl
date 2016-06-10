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
-export([get/8, update/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(is_time(Time), (Time == date orelse Time == datetime)).

get(PoolId, Fields, Constrains, SortField, Order, Offset, Count, Aggregations) ->
    %% Assuming reading is not so frequent job, will just generate Ad hock queries.
    %% According to the documentation, they are slower mostly because they have to compile before execution
    %% In our case compilation time impact is not critical.
    TableName = indexed_cache_connection:table_name(PoolId),
    FieldNames = indexed_cache_connection:field_names(PoolId),
    FieldTypes = indexed_cache_connection:field_types(PoolId),
    Select = binary_join(Fields, <<",">>),
    SortFieldName = field_name(FieldNames, SortField),
    Query = make_query(Select, TableName, FieldNames, FieldTypes, Constrains, SortFieldName, Order, Offset, Count, Aggregations),
    ?debugFmt("~p~n",[Query]),
    case erlvolt:call_procedure(PoolId, "GetData", Query) of
        {result, {voltresponse, {0, _, 1, <<>>, 128, <<>>, <<>>, _}, [
            {volttable,_,_,Rows},
            {volttable,_,_,AggregationRes}
        ]}} ->
            ?debugFmt("~p~n",[Rows]),
            ?debugFmt("~p~n",[AggregationRes]),
            {TotalCount, Aggs} = deserialize_aggregations(FieldNames, Aggregations, AggregationRes),
            {ok,
                ?debugFmt("~p~n",[FieldTypes]),
                ?debugFmt("~p~n",[types_list(FieldTypes)]),
                deserialize_objects(Fields, FieldNames, FieldTypes, Rows),
                TotalCount,
                Aggs
            };
        {result,{voltresponse,{_,_,_,T,_,_,_,_},[]}} ->
            {error, T}
    end.

-spec binary_join([binary()], binary()) -> binary().
binary_join(List, Sep) ->
    lists:foldr(fun (A, B) ->
        if
            bit_size(B) > 0 -> <<A/binary, Sep/binary, B/binary>>;
            true -> A
        end
    end, <<>>, List).

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


make_query(<<>>, TableName, FieldNames, FieldTypes, Constrains, SortField, Order, Offset, Count, Aggregations) ->
    make_query(<<"*">>, TableName, FieldNames, FieldTypes, Constrains, SortField, Order, Offset, Count, Aggregations);
make_query(Fields, TableName, FieldNames, FieldTypes, Constrains, SortField, Order, Offset, Count, Aggregations) ->
    {QueryParts, Substitutions} = make_top_constrains(FieldNames, FieldTypes, Constrains),
    Query = [
        <<"SELECT ">>, Fields, <<" FROM ">>, TableName, <<" ">>,
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
                            <<"SELECT ">>, make_aggs_query_part(FieldNames, Aggregations), <<" FROM ">>, TableName, <<" ">>,
                            QueryParts
                        ];
                    Aggregations == [] ->
                        [
                            <<"SELECT COUNT(*) FROM ">>, TableName, <<" ">>,
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
param_to_string(Time, BinaryTime) when is_binary(BinaryTime) andalso ?is_time(Time) ->
    string_volt_time(convert(Time, BinaryTime));
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
mb_cast(Time) when ?is_time(Time) -> <<"TO_TIMESTAMP(SECOND, CAST(? AS BIGINT))">>;
mb_cast(Else) -> throw({invalid_field_type, Else}).

deserialize_objects([], FieldNames, FiledTypes, Rows) ->
    RecordName = element(1, FieldNames),
    FiledTypes1 = types_list(FiledTypes),
    [list_to_tuple([RecordName | deserialize_object(FiledTypes1, Row)]) || Row <- Rows];
deserialize_objects(Fields, FieldNames, FiledTypes, Rows) ->
    FieldsList = [binary_to_existing_atom(X, latin1) || X <- Fields],
    Zip = lists:zip(tuple_to_list(FieldNames), tuple_to_list(FiledTypes)),
    FiledTypesFiltered = [V || {K,V} <- tl(Zip), lists:member(K, FieldsList)],
    [list_to_tuple(deserialize_object(FiledTypesFiltered, Row)) || Row <- Rows].

deserialize_object(FieldTypes, {voltrow, Row}) ->
    [deserialize_type(T, V) || {T, V} <- lists:zip(FieldTypes, Row)].

deserialize_type(_, null) -> undefined;
deserialize_type(boolean, 0) -> false;
deserialize_type(boolean, 1) -> true;
deserialize_type(date, Timestamp) -> date_to_binary(calendar:now_to_universal_time(Timestamp));
deserialize_type(datetime, Timestamp) -> datetime_to_binary(calendar:now_to_universal_time(Timestamp));
deserialize_type(_, Value) -> Value.

-define(TIME_ELEM_FORMATTER, <<"~2.10.0B">>).
-define(YEAR_FORMATTER, <<"~4.10.0B">>).
date_to_binary({{Y, M, D}, _}) ->
    list_to_binary(io_lib:format(<<?YEAR_FORMATTER/binary, ?TIME_ELEM_FORMATTER/binary,
        ?TIME_ELEM_FORMATTER/binary>>, [Y, M, D])).

datetime_to_binary({{Y, M, D}, {H, Min, S}}) ->
    list_to_binary(io_lib:format(<<?YEAR_FORMATTER/binary, ?TIME_ELEM_FORMATTER/binary, ?TIME_ELEM_FORMATTER/binary,
        ?TIME_ELEM_FORMATTER/binary, ?TIME_ELEM_FORMATTER/binary, ?TIME_ELEM_FORMATTER/binary>>, [Y, M, D, H, Min, S])).

%%%
transpose([]) -> [];
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
    Update3 = case  Update2 of
                  [] ->
                      [{?VOLT_ARRAY, preserialize(ItemType, [])} || ItemType <- FieldTypes];
                  [_|_] ->
                      try
                        [{?VOLT_ARRAY, preserialize(ItemType, Item)} || {ItemType, Item} <- lists:zip(FieldTypes, Update2)]
                      catch
                          {conversion_error, EType, EVal} ->
                              EPos = length(lists:takewhile(fun({Type, Val}) ->
                                  Type /= EType orelse Val /= EVal
                              end, lists:zip(FieldTypes, Update2))),
                              FieldNames = indexed_cache_connection:field_names(PoolId),
                              error({convertion_error, EType, EVal, element(EPos, FieldNames)})
                      end
    end,
    case erlvolt:call_procedure(PoolId, "UpdateData", [GroupId] ++ Update3) of
        {result, {voltresponse, {0, _, 1, <<>>, 128, <<>>, <<>>, _}, _}} ->
            true;
        {result,{voltresponse,{_,_,_,Msg,_,_,_,_},[]}} ->
            error({volt_error, Msg})
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
convert(Time, undefined) when ?is_time(Time)->
    throw(time_field_is_undefined);
convert(date, <<Y:4/binary, M:2/binary, D:2/binary>>) ->
    {i_tuple([Y, M, D]), {0,0,0}};
convert(datetime, <<Y:4/binary, M:2/binary, D:2/binary, H:2/binary, Min:2/binary, Sec:2/binary, _/binary>>) ->
    {i_tuple([Y, M, D]), i_tuple([H, Min, Sec])};
convert(Type, Value) ->
    throw({conversion_error, Type, Value}).

i_tuple(L) -> list_to_tuple(lists:map(fun erlang:binary_to_integer/1, L)).


encode_type(boolean) -> ?VOLT_INTEGER;
encode_type(float) -> ?VOLT_DECIMAL;
encode_type(string) -> ?VOLT_STRING;
encode_type(Time) when ?is_time(Time) -> ?VOLT_TIMESTAMP.