%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Mar 2016 11:27
%%%-------------------------------------------------------------------
-module(indexed_cache_data_transform).
-author("lol4t0").

%% API
-export([parse_transform/2]).

-compile({parse_transform, parse_trans_codegen}).

-record(data_info, {id, field_names, field_types}).


parse_transform(Forms, _Options) ->
    Records = lists:append([RecordsList || {attribute, _, indexed_cache_data, RecordsList} <- Forms]),
    if
        Records == [] ->
            Forms;
        true ->
            RecordDefs = lists:map(fun(RecId) ->
                [RecordTypeInfo] = [Info || {attribute, _, type, {{record, ARecId}, Info, _}} <- Forms, ARecId =:= RecId],
                {Names, Types} = lists:unzip(lists:map(fun({typed_record_field, NameInfo, TypeInfo}) ->
                    {_, _, Name} = element(3, NameInfo),
                    Type = case TypeInfo of
                        {type, _, union, TypeList} ->
                            extract_type(TypeList);
                        Else ->
                            extract_type([Else])
                    end,
                    {Name, Type}
                end, RecordTypeInfo)),
                #data_info{id = RecId, field_names = list_to_tuple([RecId | Names]), field_types = list_to_tuple([RecId |Types])}
            end, Records),
            NamesFun = codegen:gen_function('indexed_cache$$field_names',
                [fun({'$var', RecId}) -> {'$var', Names} end || #data_info{id = RecId, field_names = Names} <- RecordDefs]),
            TypesFun = codegen:gen_function('indexed_cache$$field_types',
                [fun({'$var', RecId}) -> {'$var', Types} end || #data_info{id = RecId, field_types = Types} <- RecordDefs]),
            Forms1 = replace_calls(Forms),
            Forms1 ++ [
                NamesFun,
                TypesFun,
                {attribute,-1,compile,{nowarn_unused_function,[{'indexed_cache$$field_names',1}]}},
                {attribute,-1,compile,{nowarn_unused_function,[{'indexed_cache$$field_types',1}]}}
            ]
    end.

replace_calls(Forms) ->
    parse_trans:plain_transform(fun do_transform/1, Forms).

do_transform({call,L,
                {remote,_,
                    {atom,_,indexed_cache_data},
                    {atom,_,field_names}}, Args}) ->
    {call, L, {atom, L, 'indexed_cache$$field_names'}, Args};
do_transform({call,L,
    {remote,_,
        {atom,_,indexed_cache_data},
        {atom,_,field_types}}, Args}) ->
    {call, L, {atom, L, 'indexed_cache$$field_types'}, Args};
do_transform(_) ->
    continue.

extract_type(Items) ->
    [Type] = lists:flatmap(fun
        ({type, _, TypeName, _}) ->
            case guess_type(TypeName) of
                undefined -> [];
                Else -> [Else]
            end;
        ({remote_type, _, [{_,_,indexed_cache_data}, {_, _, Type}, _]}) ->
            [Type];
        (_) ->
            []
    end, Items),
    Type.

guess_type(binary) -> string;
guess_type(string) -> string;
guess_type(nonempty_string) -> string;
guess_type(boolean) -> boolean;
guess_type(number) -> float;
guess_type(float) -> float;
guess_type(integer) -> float;
guess_type(pos_integer) -> float;
guess_type(neg_integer) -> float;
guess_type(non_neg_integer) -> float;
guess_type(_) -> undefined.

