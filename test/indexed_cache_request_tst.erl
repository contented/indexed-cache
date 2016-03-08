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
        end}
    ].
