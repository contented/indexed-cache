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

-include_lib("eunit/include/eunit.hrl").
-include("indexed_cache.hrl").

-record(data, {a, b, c, d}).
-define(FIELD_TYPES, #data{a = string, b = string, c = boolean, d = float}).
-define(FIELD_NAMES, ?INDEXED_CACHE_FIELD_NAMES(data)).

make_constrains_test_() ->
    [
        {"Basic test", fun()->
            Qry = [{eq, #data.a, <<"ya">>}],
            X = indexed_cache_request:make_constrains(?FIELD_NAMES, ?FIELD_TYPES, Qry),
            ?assertMatch({_, [{string, <<"ya">>}]}, X),
            {R, _} = X,
            ?assertEqual(<<"a = ? ">>, iolist_to_binary(R))
        end},
        {"Basic OR test", fun()->
            Qry = [{'or', [{eq, #data.a, <<"ya">>}, {in, #data.b, [<<"be">>, <<"bu">>]}]}],
            X = indexed_cache_request:make_constrains(?FIELD_NAMES, ?FIELD_TYPES, Qry),
            ?assertMatch({_, [{string, <<"ya">>}, {string, <<"be">>}, {string, <<"bu">>}]}, X),
            {R, _} = X,
            ?assertEqual(<<" ( a = ? OR b IN (?,?)  )  ">>, iolist_to_binary(R))
        end},
        {"AND-OR test", fun()->
            Qry = [{range, #data.d, 0.1, 0.6}, {'or', [{eq, #data.a, <<"ya">>}, {in, #data.b, [<<"be">>, <<"bu">>]}]}],
            X = indexed_cache_request:make_constrains(?FIELD_NAMES, ?FIELD_TYPES, Qry),
            ?assertMatch({_, [{float, 0.1}, {float, 0.6}, {string, <<"ya">>}, {string, <<"be">>}, {string, <<"bu">>}]}, X),
            {R, _} = X,
            ?assertEqual(<<"d BETWEEN CAST(? AS DECIMAL) AND CAST(? AS DECIMAL)",
                            " AND  ( a = ? OR b IN (?,?)  )  ">>, iolist_to_binary(R))
        end},
        {"Recursive OR test", fun() ->
            Qry = [{'or', [[{eq, #data.a, <<"ya">>}, {like, #data.b, <<"boo">>}], {in, #data.b, [<<"be">>, <<"bu">>]}]}],
            X = indexed_cache_request:make_constrains(?FIELD_NAMES, ?FIELD_TYPES, Qry),

            %% do not analyze <<"%boo%">> as it can be iolist
            ?assertMatch({_, [{string, <<"ya">>}, {string, _}, {string, <<"be">>}, {string, <<"bu">>}]}, X),

            {R, _} = X,
            ?assertEqual(<<" ( a = ? AND b LIKE ?  OR b IN (?,?)  )  ">>, iolist_to_binary(R))
        end},
        {"Simple ops test", fun() ->
            Qry = [{eq, #data.a, <<"a">>}, {lt, #data.a, <<"b">>}, {lte, #data.a, <<"c">>},
                {gt, #data.b, <<"d">>}, {gte, #data.b, <<"e">>}],
            X = indexed_cache_request:make_constrains(?FIELD_NAMES, ?FIELD_TYPES, Qry),
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
    meck:expect(indexed_cache_connection, field_names, 1, ?FIELD_NAMES),
    meck:expect(indexed_cache_connection, field_types, 1, ?FIELD_TYPES).

teardown(_) ->
    ok.
