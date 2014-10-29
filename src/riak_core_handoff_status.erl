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

-compile(export_all).
-include("riak_core_handoff.hrl").

-record(node_transfer_summary, {
          ongoing :: pos_integer(),
          outstanding   :: pos_integer(),
          total :: pos_integer()
         }).

-type node_transfer_summary() :: #node_transfer_summary{}.


-record(transfer_summary,  {
          ownership :: [{node(), node_transfer_summary()}],
          fallback  :: [{node(), node_transfer_summary()}]
         }).

print_handoff_summary() ->
    Status = transfer_summary(),
    Output = riak_core_console_writer:write(Status),
    io:format("~s", [Output]).

%% TODO (jwest): remember to take into account different vnode mods
transfer_summary() ->
    %% TODO (jwest): get ring from claimant?
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    %% TODO (jwest): deal w/ resize/repair
    %% TODO (jwest): splitting this out is stupid because we join back together
    {OngoingOwnership, OngoingFallback, _, _} = ongoing_transfers_summary(),
    AllSummary = build_transfer_summary({OngoingOwnership, OngoingFallback},
                                        {outstanding_count(ownership, Ring), outstanding_count(hinted, Ring)}),
    %% TODO (jwest): this is better in a macro
    %% OwnershipSchema = ["Node"
    %%           , "Ongoing Ownership", "Oustanding Ownership", "Total Ownership"],
    %% FallbackSchema = ["Node"
    %%                  , "Ongoing Fallback", "Outstanding Fallback", "Total Fallback"],
    %% OwnershipTable = {table, OwnershipSchema,
    %%                   [[Node,
    %%                     Summary#node_transfer_summary.ongoing,
    %%                     Summary#node_transfer_summary.outstanding,
    %%                     Summary#node_transfer_summary.total] || {Node, Summary} <- AllSummary#transfer_summary.ownership]},
    %% FallbackTable = {table, FallbackSchema,
    %%                   [[Node,
    %%                     Summary#node_transfer_summary.ongoing,
    %%                     Summary#node_transfer_summary.outstanding,
    %%                     Summary#node_transfer_summary.total] || {Node, Summary} <- AllSummary#transfer_summary.fallback]},
    %%    [OwnershipTable, FallbackTable].
    Schema = ["Node"
              , "Ownership"
              , "Fallback"],
    Header = {text, "Key: (active) pending / total"},
    Table = {table, Schema,
             [begin
                  FSummary = proplists:get_value(Node, AllSummary#transfer_summary.fallback,
                                                 #node_transfer_summary{ongoing = 0, outstanding = 0, total = 0}),

                  [Node,
                   io_lib:format(" ( ~3.. B ) ~3.. B / ~3.. B ",
                                 [Summary#node_transfer_summary.ongoing,
                                  Summary#node_transfer_summary.outstanding,
                                  Summary#node_transfer_summary.total]),
                   io_lib:format(" ( ~3.. B ) ~3.. B / ~3.. B ",
                                 [FSummary#node_transfer_summary.ongoing,
                                  FSummary#node_transfer_summary.outstanding,
                                  FSummary#node_transfer_summary.total])]
              end || {Node, Summary} <- AllSummary#transfer_summary.ownership]},
    [Header, Table].






%% TODO (jwest): this function isn't really necessary right now
build_transfer_summary({OngoingOwnership, OngoingFallback},
                       {OutstandingOwnership, OutstandingFallback}) ->
    #transfer_summary {
       ownership = build_ownership_summary(OngoingOwnership, OutstandingOwnership),
       fallback  = build_fallback_summary(OngoingFallback, OutstandingFallback)
      }.

build_ownership_summary(Ongoing, Outstanding) ->
    lists:foldl(fun({Node, OngoingCount}, Acc) ->
                        %% TODO (jwest): is there ever a time node will be in Ongoing but not Outstanding except when no transers?
                        OutstandingCount = proplists:get_value(Node, Outstanding, 0),
                        NodeSummary = #node_transfer_summary {
                                         ongoing = OngoingCount,
                                         outstanding = OutstandingCount - OngoingCount,
                                         %% TODO (jwest): we reall want to the total in the transition, not total outstanding
                                         total = OutstandingCount
                                        },
                        [{Node, NodeSummary} | Acc]
                end, [], Ongoing).

%% TODO (jwest): lots of duplication w/ fallback summary
build_fallback_summary(Ongoing, Outstanding) ->
    lists:foldl(fun({Node, OngoingCount}, Acc) ->
                        %% TODO (jwest): is there ever a time node will be in Ongoing but not Outstanding except when no transfers?
                        OutstandingCount = proplists:get_value(Node, Outstanding, 0),
                        NodeSummary = #node_transfer_summary {
                                         ongoing = OngoingCount,
                                         outstanding = OutstandingCount - OngoingCount,
                                         total = OutstandingCount
                                        },
                        [{Node, NodeSummary} | Acc]
                end, [], Ongoing).

-spec outstanding_count(ho_type(), riak_core_ring:riak_core_ring()) -> [{node(), pos_integer()}].
outstanding_count(ownership, Ring) ->
    OwnershipChanges = riak_core_ring:pending_changes(Ring),
    lists:foldl(fun({_, Source, _, _, awaiting}, Acc) ->
                        OldCount = case lists:keyfind(Source, 1, Acc) of
                                       false -> 0;
                                       {Source, OC} -> OC
                                   end,
                        lists:keystore(Source, 1, Acc, {Source, OldCount + 1});
                   (_, Acc) ->
                        Acc
                end, [], OwnershipChanges);
outstanding_count(hinted, Ring) ->
    [begin
         {_, Sec, _} = riak_core_status:partitions(Node, Ring),
         {Node, length(Sec)}
     end || Node <- riak_core_ring:ready_members(Ring)].


%outstanding_transfer_summary() ->

ongoing_transfers_summary() ->
    %% TODO (jwest): have option to use cluster metadata instead of this rpc
    %% TODO (jwest): deal w/ down nodes
    %% TODO (jwest): is rpc_every_member_ann chatting w/ more nodes than we need to (e.g. only valid, leaving?)
    {Ongoing, _DownNodes} = riak_core_util:rpc_every_member_ann(riak_core_handoff_manager,
                                                                status,
                                                                [{direction, outbound}],
                                                                5000),
    lists:foldl(fun({Node, Outbounds}, {Ownerships, Fallbacks, Resizes, Repairs}) ->
                        {O, F, R1, R2} = lists:foldl(fun({status_v2, Status}, {Own, Fall, Res, Rep}) ->
                                                             case proplists:get_value(type, Status) of
                                                                 ownership ->
                                                                     {Own + 1, Fall, Res, Rep};
                                                                 hinted ->
                                                                     {Own, Fall + 1, Res, Rep};
                                                                 resize ->
                                                                     {Own, Fall, Res + 1, Rep};
                                                                 repair ->
                                                                     {Own, Fall, Res, Rep + 1}
                                                             end
                                                     end, {0, 0, 0, 0}, Outbounds),
                        {[{Node, O} | Ownerships], [{Node, F} | Fallbacks], [{Node, R1} | Resizes], [{Node, R2} | Repairs]}
                end, {[], [], [], []}, Ongoing).
