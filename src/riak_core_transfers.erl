%% -------------------------------------------------------------------
%%
%% riak_core_handoff_tracking: Functions for updating the current
%% status of handoff tracking (currently in cluster metadata)
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_transfers).
-behaviour(gen_server).

-include("riak_core_handoff.hrl").

%%%%%%
%% Public API
%%%%%%
-export([add_queued_handoffs/1,
         overwrite_claimant_queue/1,
         add_active_handoff/1]).


%%%%%%
%% gen_server callbacks
%%%%%%
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).


%%%%
%% Used exclusively by claimant
%%    * Overwrites queued/claimant/<module> key in CMD
%%    * Increments `md_version' key in orddict
overwrite_claimant_queue(Handoffs) ->
    gen_server:cast(?MODULE, {claimant_queue_overwrite, Handoffs}).

add_queued_handoffs([]) ->
    ok;

add_queued_handoffs([_H|_T]=Handoffs) ->
    gen_server:cast(?MODULE, {add_queued_handoffs,
                              lists:map(fun({Mod, Idx, TargetNode, Pid}) ->
                                                build_status_record(
                                                  Mod, Idx, Idx,
                                                  TargetNode, Pid)
                                        end,
                                        Handoffs)}).

add_active_handoff(Handoff) ->
    gen_server:cast(?MODULE, {active_handoff, Handoff}).

-define(MD_PREFIX(QueuedOrActive), {handoff_tracking, QueuedOrActive}).

%%%%%%
%% gen_server callbacks
%%%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, no_state}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(_, _, _) ->
    {ok, no_state}.

handle_info(_,_) ->
    {ok, no_state}.

handle_cast({claimant_queue_overwrite, Handoffs}, State) ->
    claimant_queue_overwrite_impl(Handoffs),
    {noreply, State};

handle_cast({add_queued_handoffs, Handoffs}, State) ->
    add_queued_handoffs_impl(Handoffs),
    {noreply, State};

handle_cast({active_handoff, HO}, State) ->
    add_active_handoff_impl(HO),
    {noreply, State}.


%%%%%%
%% Implementation
%%%%%%
add_queued_handoffs_impl(Handoffs) ->
    BulkUpdate = lists:map(fun(HO) ->
                                   {Type, _, OldIdx, _} =
                                       extract_key_handoff_data(HO),
                                   {{Type, OldIdx}, status_summary(HO)}
                           end,
                           Handoffs),
    riak_core_metadata:put(?MD_PREFIX(queued),
                           node(),
                           build_append_handoffs_fun(orddict:from_list(BulkUpdate))).

add_active_handoff_impl(HO) ->
    {Type, _Module, OldIdx, _NewIdx} = extract_key_handoff_data(HO),

    riak_core_metadata:put(?MD_PREFIX(active),
                           node(),
                           build_append_handoff_fun(Type, OldIdx,
                                                    status_summary(HO))),
    riak_core_metadata:put(?MD_PREFIX(queued),
                           determine_queued_key(Type),
                           build_delete_handoff_fun(Type, OldIdx)).

claimant_queue_overwrite_impl(Handoffs) ->
    NewVersion = find_next_claimant_version(),
    riak_core_metadata:put(?MD_PREFIX(queued),
                           claimant,
                           build_new_claimant_dict(NewVersion, Handoffs)).


build_status_record(Module, OldIdx, TargetIdx, TargetNode, VNodePid) ->
    Owner = get_owner_by_index(OldIdx),
    build_status_record(Module, OldIdx, TargetIdx, TargetNode, VNodePid,
                        deduce_handoff_type(Owner)).

build_status_record(Module, OldIdx, TargetIdx, TargetNode, VNodePid, Type) ->
    #handoff_status{direction=outbound,
                    timestamp=os:timestamp(),
                    src_node=node(),
                    target_node=TargetNode,
                    mod_src_tgt={Module, OldIdx, TargetIdx},
                    vnode_pid=VNodePid, type=Type}.


get_owner_by_index(OldIdx) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owner = riak_core_ring:index_owner(
              Ring,
              OldIdx),
    Owner.

extract_key_handoff_data(#handoff_status{
                            type=Type,
                            mod_src_tgt={Module,
                                         OldIdx,
                                         NewIdx}}) ->
    {Module, Type, OldIdx, NewIdx};

%% TODO: Do we really care about _ModList here, or not?
extract_key_handoff_data([{Idx, SourceNode,TargetNode,_ModList,_CompletedStatus}]) ->
    {ring, determine_transfer_type(SourceNode, TargetNode), Idx, Idx};
extract_key_handoff_data({Idx, SourceNode,TargetNode,_ModList,_CompletedStatus}) ->
    {ring, determine_transfer_type(SourceNode, TargetNode), Idx, Idx}.

determine_transfer_type(_, '$resize') ->
    resize_transfer;
determine_transfer_type(_Source, _Target) ->
    ownership_transfer.

deduce_handoff_type(Owner) when Owner =:= node() ->
    ownership_transfer;
deduce_handoff_type(_Owner) ->
    hinted_handoff.

status_summary(#handoff_status{
                  transport_pid=Pid,
                  direction=Direction,
                  timestamp=Timestamp,
                  target_node=Target,
                  src_node=Source}) ->
    [{pid, Pid}, {direction, Direction}, {timestamp, Timestamp},
     {target, Target}, {source, Source}];

status_summary({_Idx,SourceNode,TargetNode,_ModList,_CompletedStatus}) ->
    [{pid, undefined}, {direction, outbound}, {timestamp, os:timestamp()},
     {target, TargetNode}, {source, SourceNode}].


%%%%
%% Given a transfer type, determine if the queued key in cluster metadata
%% is `claimant' or the current node
determine_queued_key(resize_transfer) ->
    claimant;
determine_queued_key(ownership_transfer) ->
    claimant;
determine_queued_key(_TransferType) ->
    node().


%% Core metadata handling
%%   Prefix:      handoff_tracking
%%   SubPrefix:   queued|active
%%   Key:         claimant|node()
%%                (claimant is only valid under `queued')
%%   Value:       orddict of handoffs


%%   Orddict Key { type, index }
%%   Value  - proplist

build_new_claimant_dict(Version, Handoffs) ->
    Dict = orddict:from_list(
             lists:map(fun(HO) ->
                               {_Module, Type, OldIdx, _NewIdx} =
                                   extract_key_handoff_data(HO),
                               { {Type, OldIdx}, status_summary(HO) }
                       end,
                       Handoffs)),
    orddict:store(md_version, Version, Dict).

find_next_claimant_version() ->
    case riak_core_metadata:get(?MD_PREFIX(queued), claimant) of
        undefined ->
            1;
        OldDict ->
            case orddict:find(md_version, OldDict) of
                {ok, Version} ->
                    Version + 1;
                error ->
                    1
            end
    end.

build_append_handoff_fun(Type, Index, Proplist) ->
    Key = {Type, Index},
    fun(undefined) ->
            orddict:append(Key, Proplist, orddict:new());
       ([Dict]) ->
            orddict:store(Key, Proplist, Dict)
    end.

build_append_handoffs_fun(NewDict) ->
    fun(undefined) ->
            NewDict;
       ([OldDict]) ->
            orddict:merge(fun(_Key, _OldV, NewV) -> NewV end,
                          OldDict, NewDict)
    end.

%%%%
%% `build_delete_handoff_fun' can experience conflicts when updating the
%% claimant ownership/resize queue
build_delete_handoff_fun(Type, Index) ->
    Key = {Type, Index},
    fun(undefined) ->
            undefined;
       ([Dict]) ->
            orddict:erase(Key, Dict);
       ([_Dict|_T]=List) ->
            %% Conflicts
            resolve_claimant_conflicts(List)
    end.

resolve_claimant_conflicts(Dicts) ->
    Dicts2 = filter_highest_version(Dicts),
    orddict:from_list(ordsets:to_list(ordsets:intersection(
                                        lists:map(fun(D) -> ordsets:from_list(orddict:to_list(D)) end,
                                                  Dicts2)))).

filter_highest_version(Dicts) ->
    Highest =
        lists:max(lists:map(fun(D) -> orddict:fetch(md_version, D) end,
                            Dicts)),
    lists:filter(fun(D) -> orddict:fetch(md_version, D) =:= Highest end,
                 Dicts).