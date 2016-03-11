%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Mar 2016 11:28
%%%-------------------------------------------------------------------
-module(indexed_cache_data_transform_tst).
-author("lol4t0").

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, indexed_cache_data_transform}).

-record(some, {
    a :: boolean(),
    b :: integer()
}).

-record(data, {
    a :: binary(),
    b :: indexed_cache_data:date(),
    c :: indexed_cache_data:datetime(),
    d :: float(),
    e = false :: boolean()
}).

-indexed_cache_data([data]).

pt_test_() ->
    [
        ?_assertMatch(#data{a = string, b = date, c = datetime, d = float, e = boolean}, indexed_cache_data:field_types(data)),
        ?_assertMatch(#data{a = a, b = b, c = c, d = d, e = e}, indexed_cache_data:field_names(data))
    ].




