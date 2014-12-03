%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_core_handoff_status).

-include("riak_core_handoff.hrl").

-compile(export_all).

-export([handoff_summary/0,
         handoff_summary/1
        ]).

-define(KNOWN_XFERS_MFA,
        {riak_core_vnode_manager, all_handoffs, []}).
-define(ACTIVE_XFERS_MFA,
        {riak_core_handoff_manager, status, []}).
-define(ORDERED_TRANSFERS_FOR_DISPLAY,
        [ownership_transfer, resize_transfer, hinted_handoff, repair]).


-spec handoff_summary() -> string().
handoff_summary() ->
    handoff_summary(local).

-spec handoff_summary('local'|'all'|node()) -> string().
handoff_summary(local) ->
    riak_core_console_writer:write(node_summary(node(), fun collect_from_local/1));

handoff_summary(all) ->
    riak_core_console_writer:write(node_summary(all, fun collect_from_all/1));

handoff_summary(Node) ->
    CollectFun = fun({M, F, A}) -> collect_from_other(Node, {M, F, A}) end,
    riak_core_console_writer:write(node_summary(Node, CollectFun)).

collect_from_other(Node, {M, F, A}) ->
    case riak_core_util:safe_rpc(Node, M, F, A, 5000) of
        {badrpc, _} -> {[], [Node]};
        Result -> {[{Node, Result}], []}
    end.

collect_from_local({M, F, A}) ->
    {[{node(), apply(M, F, A)}], []}.

collect_from_all({M, F, A}) ->
    riak_core_util:rpc_every_member_ann(M, F, A, 5000).

%% See riak_core_handoff_manager:build_status/1 for details on this structure
-spec collect_active_transfers(fun()) ->
                                      {[{node(),
                                        [{status_v2, list(tuple())}]}],
                                       [node()]}.
collect_active_transfers(CollectFun) ->
    {AllActive, DownNodes} = CollectFun(?ACTIVE_XFERS_MFA),
    Flattened = flatten_transfer_proplist(AllActive),
    {Inbound, Outbound} = lists:partition(fun(T) -> transfer_info(T, direction) =:= inbound end, Flattened),
    InboundWithType = populate_inbound_types(Inbound, Outbound),
    Active = Outbound ++ InboundWithType,
    {Active, DownNodes}.

populate_inbound_types(Inbound, Outbound) ->
    [case find_matching_outbound(Transfer, Outbound) of
             [] -> Transfer;
             [Match | _] ->
                 Type = transfer_info(Match, type),
                 replace_transfer_type(Transfer, Type)
         end || Transfer <- Inbound].

find_matching_outbound({Node, _} = Transfer, Outbound) ->
    TargetPartition = transfer_info(Transfer, target_partition),
    Mod = transfer_info(Transfer, mod),
    lists:filter(fun(OutTrans) ->
        TargetPartition =:= transfer_info(OutTrans, target_partition) andalso
        Mod =:= transfer_info(OutTrans, mod) andalso
        Node =:= transfer_info(OutTrans, target_node)
    end, Outbound).

replace_transfer_type({Node, {status_v2, Status}}, Type) ->
    {Node, {status_v2, lists:keyreplace(type, 1, Status, {type, Type})}}.

transfer_info({_Node, {status_v2, Status}}, Key) ->
    {Key, Value} = lists:keyfind(Key, 1, Status),
    Value.

replace_known_with_ring_transfers(Known, RingTransfers) ->
    Known1 = lists:filter(fun({_Node, {{_Module, _Index},
                                      {ownership_transfer, _Dir, _Data}}}) ->
                                  false;
                             ({_Node, {{_Module, _Index},
                                      {resize_transfer, _Dir, _Data}}}) ->
                                  false;
                             (_) ->
                                  true
                          end,
                          Known),
    coalesce_known_transfers(Known1, RingTransfers).

%% Transform a single tuple with a list of modules (from ring.next) to
%% a list of tuples with one module each
explode_ring_modules(Index, Type, Direction, Source, Dest, Modules) ->
    lists:map(fun(M) -> [{Source, {{M, Index}, {Type, Direction, Dest}}}] end,
              Modules).

parse_ring_into_known(Ring) ->
    OwnershipChanges = riak_core_ring:pending_changes(Ring),

    lists:foldl(fun({Idx, Source, '$resize', Modules, awaiting}, Acc) ->
                        explode_ring_modules(Idx, resize_transfer, outbound,
                                             Source, '$resize', Modules) ++ Acc;
                   ({Idx, Source, Dest, Modules, awaiting}, Acc) ->
                        explode_ring_modules(Idx, ownership_transfer, outbound,
                                             Source, Dest, Modules) ++ Acc;
                    (_, Acc) ->
                       Acc
               end, [], OwnershipChanges).


%% The ring has global visibility into incomplete ownership/resize
%% transfers, so we'll filter those out of what
%% `riak_core_vnode_manager' gives us and replace with the ring data
-spec collect_known_transfers(riak_core_ring:riak_core_ring(), fun()) ->
                                     [{node(),
                                       [{{module(), index()},
                                         {ho_type()|'delete',
                                          'inbound'|'outbound'|'local',
                                          node()|'$resize'|'$delete'}}]}].
collect_known_transfers(Ring, CollectFun) ->
    {Unidirectional1, DownNodes} = CollectFun(?KNOWN_XFERS_MFA),
    Unidirectional2 =
        lists:flatten(replace_known_with_ring_transfers(flatten_transfer_proplist(
                                            Unidirectional1),
                                          parse_ring_into_known(Ring))),
    {coalesce_known_transfers(
       reverse_known_transfers(Ring, Unidirectional2), Unidirectional2),
     DownNodes}.

find_resize_target(Ring, Idx) ->
    riak_core_ring:index_owner(Ring, Idx).

flatten_transfer_proplist(List) ->
    lists:flatten(
      lists:map(fun({Node, NodeTransfers}) ->
                        lists:map(fun(T) -> {Node, T} end, NodeTransfers)
                end, List)).

coalesce_known_transfers(Proplist1, Proplist2) ->
    lists:flatten(Proplist1, Proplist2).

reverse_direction(inbound) ->
    outbound;
reverse_direction(outbound) ->
    inbound.

reverse_known_transfers(Ring, Known) ->
    lists:foldl(fun({_Node1, {{_Module, _Index},
                             {delete, local, '$delete'}}}, Acc) ->
                        %% Don't reverse delete operations
                        Acc;
                   ({Node1, {{Module, Index},
                             {resize_transfer=Type,
                              outbound=Dir,
                              '$resize'}}}, Acc) ->
                        Node2 = find_resize_target(Ring, Index),
                        [{Node2, {{Module, Index},
                                  {Type, reverse_direction(Dir), Node1}}}
                         | Acc];
                   ({Node1, {{Module, Index}, {Type, Dir, Node2}}}, Acc) ->
                        [{Node2, {{Module, Index},
                                  {Type, reverse_direction(Dir), Node1}}}
                         | Acc]
                end,
                [],
                Known).

summary_schema() ->
    ["Node", "Total", "Ownership", "Resize", "Hinted", "Repair"].

add_node_name(Node, Dict) ->
    orddict:update(nodes, fun(Set) -> ordsets:add_element(Node, Set) end,
                   ordsets:from_list([Node]), Dict).

count_active_transfers(Transfers) ->
    lists:foldl(fun({Node, {status_v2, Props}}, Dict) ->
                        Type = proplists:get_value(type, Props),
                        Direction = proplists:get_value(direction, Props),
                        Dict1 = increment_counter(io_lib:format("~ts:~s:~s",
                                                                [Node, Type,
                                                                 Direction]),
                                                  Dict),
                        Dict2 = increment_counter(Type, Dict1),
                        add_node_name(Node, Dict2)
                end,
                orddict:new(),
                Transfers).

%%
%% The ring has a complete view of all known ownership/resize
%% transfers, so rely on that for a tally instead of the data
%% collected from riak_core_vnode_manager.
count_known_transfers(Transfers) ->
    lists:foldl(fun({Node, {{_Module, _Index},
                            {Type, Direction, _Node2}}}, Dict) ->
                        Dict1 = increment_counter(io_lib:format("~ts:~s:~s",
                                                                [Node, Type,
                                                                 Direction]),
                                                  Dict),
                        increment_counter(Type, Dict1)
                end,
                orddict:new(),
                Transfers).

increment_counter(Key, Dict) ->
    orddict:update_counter(Key, 1, Dict).

-spec node_summary(node()|'all', fun()) -> string().
node_summary(NodeOrAll, CollectFun) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),

    %% Known and Active will be proplists with each node name showing
    %% up 0 or more times; the value will be a `status_v2' structure
    %% (see `riak_core_handoff_manager') for active transfers or a
    %% custom handoff shorthand (see the spec for
    %% `collect_known_transfers') for known transfers.
    {Known, _DownNodes} = collect_known_transfers(Ring, CollectFun),
    {Active, DownNodes} = collect_active_transfers(CollectFun),

    KnownStats = count_known_transfers(Known),
    ActiveStats = count_active_transfers(Active),

    %% Each stats structure is an orddict with
    %% "Node:TransferType:Direction" and "TransferType" keys, values
    %% are integers. Also ActiveStats will have a `nodes' key with a
    %% ordset of nodes.

    Schema = summary_schema(),

    Header = {text, "Each cell indicates active transfers and, in parenthesis, the number of all known transfers. The 'Total' column is the sum of the active transfers."},
    AllNodes = riak_core_ring:all_members(Ring),
    Nodes = case NodeOrAll of
        all -> AllNodes -- DownNodes;
        Node -> [Node]
    end,
    Table = {table, Schema,
             [ [ format_node_name(Node) | row_summary(Node, KnownStats, ActiveStats) ] ||
                 Node <- Nodes]},
    case DownNodes of
        [] ->
            [Header, Table];
        _ ->
            NodesDown = {alert, [{column, "(unreachable)", DownNodes}]},
            [Header, Table, NodesDown]
    end.

row_summary(Node, Known, Active) ->
    {Cells, TotalActive} =
        row_summary(Node, Known, Active, ?ORDERED_TRANSFERS_FOR_DISPLAY, [], 0),
    [TotalActive | Cells].

row_summary(_Node, _Known, _Active, [], Accum, Total) ->
    {lists:reverse(Accum), Total};

row_summary(Node, Known, Active, [Type|Tail], Accum, Total) ->
    KeyIn = io_lib:format("~ts:~s:~s", [Node, Type, inbound]),
    KeyOut = io_lib:format("~ts:~s:~s", [Node, Type, outbound]),
    ActiveCount = dict_count_helper(orddict:find(KeyIn, Active)) +
        dict_count_helper(orddict:find(KeyOut, Active)),
    KnownCount= dict_count_helper(orddict:find(KeyIn, Known)) +
        dict_count_helper(orddict:find(KeyOut, Known)),
    CellContents = format_cell_contents(KnownCount, ActiveCount),
    row_summary(Node, Known, Active, Tail,
                [CellContents | Accum], Total + ActiveCount).

format_cell_contents(KnownCount, ActiveCount) ->
    case {KnownCount, ActiveCount} of
        {0, 0} -> " ";
        {0, _} -> io_lib:format("~B (Unknown)", [ActiveCount]);
        _ -> io_lib:format("~B (~B)", [ActiveCount, KnownCount])
    end.

dict_count_helper({ok, Count}) ->
    Count;
dict_count_helper(error) ->
    0.

-spec format_node_name(node()) -> string().
format_node_name(Node) when is_atom(Node) ->
    "  " ++ atom_to_list(Node) ++ "  ".
