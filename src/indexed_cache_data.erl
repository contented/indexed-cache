%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Mar 2016 11:30
%%%-------------------------------------------------------------------
-module(indexed_cache_data).
-author("lol4t0").

-export_type([date/0, datetime/0]).

-type date() :: <<_:64>>.
-type datetime() :: <<_:112>>.

-type indexed_data() :: tuple().


%% API
-export([field_types/1, field_names/1]).

-spec field_types(Data :: indexed_data()) -> indexed_cache:field_types().
field_types(_Data) ->
    pt_error().

-spec field_names(Data :: indexed_data()) -> indexed_cache:field_names().
field_names(_Data) ->
    pt_error().

pt_error() ->
    error(parse_transform_error, "Use indexed_cache_data_transform parse transform to make this work "
        "& define attribute `-indexed_cache_data([<record names>]).`").


