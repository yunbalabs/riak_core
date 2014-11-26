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

-export([print_handoff_summary/0,
         print_handoff_summary/1
        ]).

print_handoff_summary() ->
    print_handoff_summary(local).

print_handoff_summary(local) ->
    print_handoff_summary_internal(fun collect_from_local/3);

print_handoff_summary(all) ->
    print_handoff_summary_internal(fun collect_from_all/3);

print_handoff_summary(Node) ->
    CollectFun = fun(M, F, A) -> collect_from_other(Node, M, F, A) end,
    print_handoff_summary_internal(CollectFun).

print_handoff_summary_internal(CollectFun) ->
    Status = transfer_summary(CollectFun),
    Output = riak_core_console_writer:write(Status),
    io:format("~s", [Output]).

collect_from_other(Node, M, F, A) ->
    case riak_core_util:safe_rpc(Node, M, F, A, 5000) of
        {badrpc, _} -> {[], [Node]};
        Result -> {[{Node, Result}], []}
    end.

collect_from_local(M, F, A) ->
    {[{node(), apply(M, F, A)}], []}.

collect_from_all(M, F, A) ->
    riak_core_util:rpc_every_member_ann(M, F, A, 5000).

%% TODO (jwest): remember to take into account different vnode mods
transfer_summary(CollectFun) ->
    %% TODO (jwest): get ring from claimant?
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {OngoingSummary, DownNodes} = ongoing_transfers_summary(CollectFun),
    OutstandingSummary = outstanding_transfers_summary(Ring, CollectFun),
    {Summary, DownNodes} = build_transfer_summary(OngoingSummary, OutstandingSummary, DownNodes),

    %% TODO (jwest): this is better in a macro
    %% TODO (mallen): suppress empty categories?
    Schema = ["Node", "Ownership", "Fallback", "Resize", "Repair"],
    Header = {text, "Key: Ongoing / Outstanding / Total"},
    Footer = {text, "Blanks indicate no transfers."},
    Table = {table, Schema,
             [ [ format_node_name(Node) | format_summary(S) ]
               || {Node, S} <- orddict:to_list(Summary) ]},
    case DownNodes of
        [] ->
            [Header, Table, Footer];
        _ ->
            NodesDown = {alert, [{column, "(unreachable)", DownNodes}]},
            [Header, Table, NodesDown, Footer]
    end.


outstanding_transfers_summary(Ring, CollectFun) ->
    [ outstanding_count(T, Ring, CollectFun)
      || T <- [ownership_transfer, hinted_handoff, resize_transfer, repair] ].

rt(I) ->
    binary_to_list(iolist_to_binary(I)).

format_summary(S) ->
    format_summary(S, default, " ~B / ~B / ~B ").

format_summary(Summary, Fields, OutputFormat) ->
    [ rt(format_summary1(orddict:fetch(T, Summary), Fields, OutputFormat)) ||
        T <- [ownership_transfer, hinted_handoff, resize_transfer, repair] ].

%% The contents of Data
%% Quoting from riak_core_handoff_manager.erl #368
%%  [{mod, Mod},
%%   {src_partition, SrcP},
%%   {target_partition, TargetP},
%%   {src_node, SrcNode},
%%   {target_node, TargetNode},
%%   {direction, Dir},
%%   {status, Status},
%%   {start_ts, StartTS},
%%   {sender_pid, TPid},
%%   {stats, calc_stats(HO)}]

format_summary1({0, 0, 0, _Data}, default, _OutputFormat) ->
    "";

format_summary1({On, Out, Total, _Data}, default, OutputFormat) ->
    io_lib:format(OutputFormat, [On, Out, Total]).

-spec format_node_name(node()) -> string().
format_node_name(Node) when is_atom(Node) ->
    "  " ++ atom_to_list(Node) ++ "  ".

build_transfer_summary(OngoingSummary, OutstandingSummary, DownNodes) ->
    {orddict:map(
       fun(Node, D) -> merge_outstanding(Node, D, OutstandingSummary) end,
       OngoingSummary), DownNodes}.

merge_outstanding(Node, Ongoing, Outstanding) ->
    [O, H, Rz, Rp] = lists:map(
                       fun(L) -> proplists:get_value(Node, L, {0, 0}) end, Outstanding),
    orddict:map(
      fun(K, V) ->
              case K of
                  ownership_transfer ->
                      build_summary_tuple(V, O);
                  hinted_handoff ->
                      build_summary_tuple(V, H);
                  resize_transfer ->
                      build_summary_tuple(V, Rz);
                  repair ->
                      build_summary_tuple(V, Rp)
              end
      end, Ongoing).

build_summary_tuple(Data, {In, Out}) ->
    Total = In + Out,
    C = length(Data),
    O = max(Total - C, C),
    T = max(Total, C),
    {C, O, T, Data}.


%% TODO: collapse into less functions where functional heads match i.e., ownership and resize
-spec outstanding_count(ho_type(), riak_core_ring:riak_core_ring(), fun()) -> [{node(), {non_neg_integer(), non_neg_integer()}}].
outstanding_count(ownership_transfer, Ring, _CollectFun) ->
    OwnershipChanges = riak_core_ring:pending_changes(Ring),

    lists:foldl(fun
                    ({_, _, '$resize', _, _}, Acc) ->
                       Acc;
                    ({_, Source, Dest, _, awaiting}, Acc) ->
                        Acc1 = increment_outbound_tally(Source, Acc),
                        increment_inbound_tally(Dest, Acc1);
                    (_, Acc) ->
                       Acc
               end, [], OwnershipChanges);
outstanding_count(hinted_handoff, Ring, _CollectFun) ->
    [begin
         try
             {_, Sec, _} = riak_core_status:partitions(Node, Ring),
             {Node, {0, length(Sec)}}
         catch _:_ ->
             {Node, {0, 0}}
         end
     end || Node <- riak_core_ring:ready_members(Ring)];

outstanding_count(resize_transfer, Ring, _CollectFun) ->
    Resizes = riak_core_ring:pending_changes(Ring),
    NewRing = riak_core_ring:future_ring(Ring),
    lists:foldl(fun({Idx, Source, '$resize', _, awaiting}, Acc) ->
                        Acc1 = increment_outbound_tally(Source, Acc),
                        increment_inbound_tally(
                          riak_core_ring:index_owner(NewRing, Idx), Acc1);
                   (_, Acc) ->
                        Acc
                end, [], Resizes);
outstanding_count(repair, _Ring, CollectFun) ->
    %% Output should [{Node, Count}]
                                                %apply(riak_core_vnode_manager, all_repairs, []),
                                                %riak_core_util:safe_rpc(OtherNode, ...),
    {Repairs, _Down} = CollectFun(riak_core_vnode_manager, all_repairs, []),
    lists:foldl(fun count_repairs/2, [], Repairs).

count_repairs({Node, RepairRecords}, Acc) ->
    [ {Node, {0, length(RepairRecords)}} | Acc ].

ongoing_transfers_summary(CollectFun) ->
    %% TODO (jwest): have option to use cluster metadata instead of this rpc
    %% TODO (jwest): deal w/ down nodes
    %%
    %% TODO (jwest): is rpc_every_member_ann chatting w/ more nodes than we need to (e.g. only valid, leaving?)
    %% mallen: rpc_every_member_ann calls multirpc_ann which gets its ring state and membership information from the
    %% local ring manager, so its view of the world will be the same as the status code
    {Ongoing, DownNodes} = CollectFun(
                             riak_core_handoff_manager, status,
                             [{direction, outbound}]),

    Summary = lists:foldl(fun build_ongoing_node_summaries/2,
                          orddict:new(), Ongoing),

    %% Summary is an orddict of node -> handoff_type_counts
    %% DownNodes is a list of node() atoms
    {Summary, DownNodes}.

build_ongoing_node_summaries({Node, OutboundTransfers}, Acc) ->
    orddict:store(Node, build_summaries(OutboundTransfers), Acc).

build_summaries(OutboundTransfers) ->
    I = orddict:from_list(
          [
           {ownership_transfer, []},
           {hinted_handoff, []},
           {resize_transfer, []},
           {repair, []}
          ]),
    lists:foldl(fun store_handoff_by_type/2, I, OutboundTransfers).

store_handoff_by_type({status_v2, Status}, D) ->
    {_, Type} = lists:keyfind(type, 1, Status),
    orddict:append(Type, Status, D).

find_tallies(Node, List) ->
    case lists:keyfind(Node, 1, List) of
                    false -> {0, 0};
                    {Node, {_In, _Out}=Counts} -> Counts
    end.

increment_inbound_tally(Node, List) ->
    {In, Out} = find_tallies(Node, List),
    lists:keystore(Node, 1, List, {Node, {In+1, Out}}).

increment_outbound_tally(Node, List) ->
    {In, Out} = find_tallies(Node, List),
    lists:keystore(Node, 1, List, {Node, {In, Out+1}}).
