%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Mar 2016 12:30
%%%-------------------------------------------------------------------
-module(indexed_cache_request_tst).
-author("lol4t0").

-compile({parse_transform, indexed_cache_data_transform}).

-include_lib("eunit/include/eunit.hrl").

-record(data, {
    a :: binary(),
    b :: binary(),
    c :: boolean(),
    d :: number()
}).

-indexed_cache_data([data]).

make_constrains_test_() ->
    [
        {"Basic test", fun()->
            Qry = [{eq, #data.a, <<"ya">>}],
            X = indexed_cache_request:make_constrains(
                indexed_cache_data:field_names(data), indexed_cache_data:field_types(data), Qry),
            ?assertMatch({_, [{string, <<"ya">>}]}, X),
            {R, _} = X,
            ?assertEqual(<<"a = ? ">>, iolist_to_binary(R))
        end},
        {"Basic OR test", fun()->
            Qry = [{'or', [{eq, #data.a, <<"ya">>}, {in, #data.b, [<<"be">>, <<"bu">>]}]}],
            X = indexed_cache_request:make_constrains(
                indexed_cache_data:field_names(data), indexed_cache_data:field_types(data), Qry),
            ?assertMatch({_, [{string, <<"ya">>}, {string, <<"be">>}, {string, <<"bu">>}]}, X),
            {R, _} = X,
            ?assertEqual(<<" ( a = ? OR b IN (?,?)  )  ">>, iolist_to_binary(R))
        end},
        {"AND-OR test", fun()->
            Qry = [{range, #data.d, 0.1, 0.6}, {'or', [{eq, #data.a, <<"ya">>}, {in, #data.b, [<<"be">>, <<"bu">>]}]}],
            X = indexed_cache_request:make_constrains(
                indexed_cache_data:field_names(data), indexed_cache_data:field_types(data), Qry),
            ?assertMatch({_, [{float, 0.1}, {float, 0.6}, {string, <<"ya">>}, {string, <<"be">>}, {string, <<"bu">>}]}, X),
            {R, _} = X,
            ?assertEqual(<<"d BETWEEN CAST(? AS DECIMAL) AND CAST(? AS DECIMAL)",
                            " AND  ( a = ? OR b IN (?,?)  )  ">>, iolist_to_binary(R))
        end},
        {"Recursive OR test", fun() ->
            Qry = [{'or', [[{eq, #data.a, <<"ya">>}, {like, #data.b, <<"boo">>}], {in, #data.b, [<<"be">>, <<"bu">>]}]}],
            X = indexed_cache_request:make_constrains(
                indexed_cache_data:field_names(data), indexed_cache_data:field_types(data), Qry),

            %% do not analyze <<"%boo%">> as it can be iolist
            ?assertMatch({_, [{string, <<"ya">>}, {string, _}, {string, <<"be">>}, {string, <<"bu">>}]}, X),

            {R, _} = X,
            ?assertEqual(<<" ( a = ? AND b LIKE ?  OR b IN (?,?)  )  ">>, iolist_to_binary(R))
        end},
        {"Simple ops test", fun() ->
            Qry = [{eq, #data.a, <<"a">>}, {lt, #data.a, <<"b">>}, {lte, #data.a, <<"c">>},
                {gt, #data.b, <<"d">>}, {gte, #data.b, <<"e">>}],
            X = indexed_cache_request:make_constrains(
                indexed_cache_data:field_names(data), indexed_cache_data:field_types(data), Qry),
            ?assertMatch({_, _}, X),
            {R, _} = X,
            ?assertEqual(<<"a = ? AND a < ? AND a <= ? AND b > ? AND b >= ? ">>, iolist_to_binary(R))
        end}
    ].

invalid_request_test_() ->
    {setup, fun setup/0, fun teardown/1, [
        {"Wrong operator", fun() ->
            Qry = {magicmagic, #data.a, 'magic'},
            X = indexed_cache:get(pool, Qry, #data.a, asc, 0, 100, []),
            ?assertEqual({error, {invalid_constrain, Qry}}, X)
        end}
    ]}.

setup() ->
    meck:new(indexed_cache_connection),
    meck:expect(indexed_cache_connection, field_names, 1, indexed_cache_data:field_names(data)),
    meck:expect(indexed_cache_connection, field_types, 1, indexed_cache_data:field_types(data)),
    meck:expect(indexed_cache_connection, table_name, 1, <<"your_table_name">>).

teardown(_) ->
    ok.
