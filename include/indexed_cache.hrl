%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Feb 2016 17:04
%%%-------------------------------------------------------------------
-author("lol4t0").

-ifndef(INDEXED_CACHE_FIELD_NAMES).
-define(INDEXED_CACHE_FIELD_NAMES(Record), list_to_tuple([Record | record_info(fields,Record)])).
-endif.