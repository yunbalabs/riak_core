-module(riak_core_console_manager).

-behaviour(gen_server).

%% gen_server api and callbacks
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% API
-export([register_command/5,
         register_config/2,
         run/1]).

-define(cmd_table, riak_core_console_commands).
-define(config_table, riak_core_console_config).
-define(schema_table, riak_core_console_schema).

-record(state, {}).

register_config(Key, Callback) ->
    true = ets:insert(?config_table, {Key, Callback}).

register_command(Cmd, Description, Keys, Flags, Fun) ->
    true = ets:insert(?cmd_table, {Cmd, Description, Keys, Flags, Fun}).

run({error, _}=E) ->
    print_error(E);
run({Fun, Args, Flags}) ->
    Fun(Args, Flags);
run([_Script, "set" | Args]) ->
    run_set(Args);
run([_Script, "show" | Args]) ->
    run_show(Args);
run(Cmd) ->
    M0 = match(Cmd, ?cmd_table),
    M1 = parse(M0),
    M2 = validate(M1),
    run(M2).

run_set(ArgsAndFlags) ->
    M1 = parse(ArgsAndFlags),
    M2 = get_config(M1),
    M3 = set_config(M2),
    case run_callback(M3) of
        {error, _}=E ->
            print_error(E);
        _ ->
            ok
    end.

run_callback({error, _}=E) ->
    E;
run_callback({Args, Flags}) ->
    KVFuns = lists:foldl(fun({K, V}, Acc) ->
                             case ets:lookup(?config_table, K) of
                                 [{K, F}] ->
                                     [{K, V, F} | Acc];
                                 [] ->
                                     Acc
                             end
                         end, [], Args),
    [F(K, V, Flags) || {K, V, F} <- KVFuns].

get_config({error, _}=E) ->
    E;
get_config({Args, Flags0}) ->
    [{schema, Schema}] = ets:lookup(?schema_table, schema),
    Conf = [{[K], V} || {K, V} <- Args],
    AppConfig = cuttlefish_generator:minimal_map(Schema, Conf),
    case validate_flags(config_flags(), Flags0) of
        {error, _}=E ->
            E;
        Flags ->
            {AppConfig, Conf, Flags}
    end.

set_config({error, _}=E) ->
    E;
set_config({AppConfig, Args, Flags}) ->
    case set_app_config(AppConfig, Flags) of
        ok ->
            {Args, Flags};
        E ->
            E
    end.

set_app_config(AppConfig, []) ->
    set_local_app_config(AppConfig);
set_app_config(AppConfig, [{node, Node}]) ->
    set_remote_app_config(AppConfig, Node);
set_app_config(AppConfig, [{all, _}]) ->
    set_remote_app_config(AppConfig);
set_app_config(_AppConfig, _Flags) ->
    Msg = "Cannot use --all(-a) and --node(-n) at the same time",
    io:format("Error: ~p~n", [Msg]),
    {error, {invalid_flag_combination, Msg}}.

set_local_app_config(AppConfig) ->
    [application:set_env(App, Key, Val) || {App, [{Key, Val}]} <- AppConfig],
    ok.

set_remote_app_config(AppConfig, Node) ->
    Fun = set_local_app_config,
    case riak_core_util:safe_rpc(Node, ?MODULE, Fun, [AppConfig]) of
        {badrpc, rpc_process_down} ->
            io:format("Error: Node ~p Down~n", [Node]),
            {error, {badrpc, Node}};
        _ ->
            ok
    end.

set_remote_app_config(AppConfig) ->
    io:format("Setting config across the cluster~n", []),
    {_, Down} = riak_core_util:rpc_every_member_ann(?MODULE,
                                                    set_local_app_config,
                                                    [AppConfig],
                                                    5000),
    (Down == []) orelse io:format("Failed to set config for: ~p~n", [Down]).

run_show(_Args) ->
    not_implemented.

config_flags() ->
    [{node, [{shortname, "n"},
             {longname, "node"},
             {typecast, fun list_to_atom/1},
             {description,
                 "The node to apply the operation on"}]},

     {all, [{shortname, "a"},
            {longname, "all"},
            {description,
                "Apply the operation to all nodes in the cluster"}]}].

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Note that this gen_server only exists to create ets tables and keep them
%% around indefinitely.
init([]) ->
    ets:new(?cmd_table, [public, named_table]),
    ets:new(?config_table, [public, named_table]),
    ets:new(?schema_table, [public, named_table]),
    SchemaFiles = filelib:wildcard(code:lib_dir() ++ "/*.schema"),
    Schema = cuttlefish_schema:files(SchemaFiles),
    true = ets:insert(?schema_table, {schema, Schema}),
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Msg, State) ->
    {noreply, State}.
terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

print_error({error, no_matching_spec}) ->
    io:format("Invalid Command~n");
print_error({error, {invalid_flag, Str}}) ->
    io:format("Invalid Flag: ~p~n", [Str]);
print_error({error, {invalid_action, Str}}) ->
    io:format("Invalid Action: ~p~n", [Str]);
print_error({error, invalid_number_of_args}) ->
    io:format("Invalid number of arguments~n");
print_error({error, {invalid_argument, Str}}) ->
    io:format("Invalid argument: ~p~n", [Str]);
print_error({error, {invalid_flags, Flags}}) ->
    io:format("Invalid Flags: ~p~n", [Flags]);
print_error({error, {invalid_flag_value, {Name, Val}}}) ->
    io:format("Invalid value: ~p for flag: ~p~n", [Val, Name]);
print_error({error, {invalid_value, Val}}) ->
    io:format("Invalid value: ~p~n", [Val]);
print_error({error, {invalid_kv_arg, Arg}}) ->
    io:format("Not a Key/Value argument of format: ~p=<Value>: ~n", [Arg]);
print_error({error, {too_many_equal_signs, Arg}}) ->
    io:format("Too Many Equal Signs in Argument: ~p~n", [Arg]).

-spec match([list()], ets:tid()) -> {tuple(), list()} | {error, no_matching_spec}.
match(Cmd0, Table) ->
    {Cmd, Args} = split_command(Cmd0),
    case ets:lookup(Table, Cmd) of
        [Spec] ->
            {Spec, Args};
        [] ->
            {error, no_matching_spec}
    end.

-spec split_command([list()]) -> {list(), list()}.
split_command(Cmd0) ->
    lists:splitwith(fun(Str) ->
                        is_not_kv_arg(Str) andalso is_not_flag(Str)
                    end, Cmd0).

parse({error, _}=E) ->
    E;
parse({Spec, ArgsAndFlags}) ->
    case parse(ArgsAndFlags) of
        {error, _}=E ->
            E;
        {Args, Flags} ->
            {Spec, Args, Flags}
    end;
parse(ArgsAndFlags) ->
    %% Positional key/value args always come before flags in our cli
    {Args0, Flags0} = lists:splitwith(fun is_not_flag/1, ArgsAndFlags),
    case parse_kv_args(Args0) of
        {error, _}=E ->
            E;
        Args ->
            case parse_flags(Flags0) of
                {error, _}=E ->
                    E;
                Flags ->
                    {Args, Flags}
            end
    end.

parse_kv_args(Args) ->
    parse_kv_args(Args, []).

%% All args must be k/v args!
parse_kv_args([], Acc) ->
    Acc;
parse_kv_args([Arg | Args], Acc) ->
    case string:tokens(Arg, "=") of
        [Key, Val] ->
            parse_kv_args(Args, [{Key, Val} | Acc]);
        [Key] ->
            {error, {invalid_kv_arg, Key}};
        _ ->
            {error, {too_many_equal_signs, Arg}}
    end.


parse_flags(Flags) ->
    parse_flags(Flags, [], []).

-spec parse_flags(list(string()), list(string()), list({string(), string()})) ->
    list({string(), string()}) | {error, {invalid_flag, string()}}.
parse_flags([], [], Acc) ->
    Acc;
parse_flags([], [Flag], Acc) ->
    [{Flag, undefined} | Acc];
parse_flags(["--"++Long | T], [], Acc) ->
    case string:tokens(Long,"=") of
        [Flag, Val] ->
            parse_flags(T, [], [{list_to_atom(Flag), Val} | Acc]);
        [Flag] ->
            parse_flags(T, [list_to_atom(Flag)], Acc)
    end;
parse_flags(["--"++_Long | _T]=Flags, [Flag], Acc) ->
    parse_flags(Flags, [], [{Flag, undefined} | Acc]);
parse_flags([[$-,Short] | T], [], Acc) ->
    parse_flags(T, [Short], Acc);
parse_flags([[$-,Short] | T], [Flag], Acc) ->
    parse_flags(T, [Short], [{Flag, undefined} | Acc]);
parse_flags([[$-,Short | Arg] | T], [], Acc) ->
    parse_flags(T, [], [{Short, Arg} | Acc]);
parse_flags([[$-,Short | Arg] | T], [Flag], Acc) ->
    parse_flags(T, [], [{Short, Arg}, {Flag, undefined} | Acc]);
parse_flags([Val | T], [Flag], Acc) ->
    parse_flags(T, [], [{Flag, Val} | Acc]);
parse_flags([Val | _T], [], _Acc) ->
    {error, {invalid_flag, Val}}.

validate({error, _}=E) ->
    E;
validate({Spec, Args0, Flags0}) ->
    {_Cmd, _Description, KeySpecs, FlagSpecs, Callback} = Spec,
    case validate_args(KeySpecs, Args0) of
        {error, _}=E ->
            E;
        Args ->
            case validate_flags(FlagSpecs, Flags0) of
                {error, _}=E ->
                    E;
                Flags ->
                    {Callback, Args, Flags}
            end
    end.

validate_args(KeySpecs, Args) ->
    convert_args(KeySpecs, Args, []).

convert_args([], [], Acc) ->
    Acc;
convert_args(_KeySpec, [], Acc) ->
    Acc;
convert_args([], Args, _Acc) ->
    {error, {invalid_args, Args}};
convert_args(KeySpecs, [{Key, Val0} | Args], Acc) ->
    case lists:keyfind(Key, 1, KeySpecs) of
        false ->
            {error, {invalid_key, Key}};
        {Key, Spec} ->
            case convert_arg(Spec, Key, Val0) of
                {error, _}=E ->
                    E;
                Val ->
                    convert_args(KeySpecs, Args, [{Key, Val} | Acc])
            end
    end.

convert_arg(Spec, Key, Val) ->
    {typecast, Fun} = lists:keyfind(typecast, 1, Spec),
    try
        Fun(Val)
    catch error:badarg ->
        {error, {invalid_argument, {Key, Val}}}
    end.

validate_flags(FlagSpecs, Flags) ->
    convert_flags(FlagSpecs, Flags, []).

convert_flags([], [], Acc) ->
    Acc;
convert_flags(_Flags, [], Acc) ->
    Acc;
convert_flags([], Provided, _Acc) ->
    Invalid = [Flag || {Flag, _} <- Provided],
    {error, {invalid_flags, Invalid}};
convert_flags(FlagSpecs, [{Key, Val0} | Flags], Acc) ->
    case lists:keyfind(Key, 1, FlagSpecs) of
        false ->
            %% We may have been passed a -short option instead of a --long option
            case find_shortname_key(Key, FlagSpecs) of
                {error, _}=E ->
                    E;
                NewKey ->
                    %% We just want to replace the shortname with a valid key
                    %% (atom of longname) and call the same function again.
                    convert_flags(FlagSpecs, [{NewKey, Val0} | Flags], Acc)
            end;
        {Key, Spec} ->
            case convert_flag(Spec, Key, Val0) of
                {error, _}=E ->
                    E;
                Val ->
                    convert_flags(FlagSpecs, Flags, [{Key, Val} | Acc])
            end
    end.

convert_flag(Spec, Key, Val) ->
    %% Flags don't necessarily have values, in which case Val is undefined here.
    %% Additionally, flag values can also be strings and not have typecast funs.
    %% It's not incorrect, so just return the value in that case.
    case lists:keyfind(typecast, 1, Spec) of
        false ->
            Val;
        {typecast, Fun} ->
            try
                Fun(Val)
            catch error:badarg ->
                {error, {invalid_flag, {Key, Val}}}
            end
    end.

find_shortname_key(ShortVal, FlagSpecs) ->
    %% Make it a string instead of an int
    Short = [ShortVal],
    Error = {error, {invalid_flag, Short}},
    lists:foldl(fun({Key, Props}, Acc) ->
                    case lists:member({shortname, Short}, Props) of
                        true ->
                            Key;
                        false ->
                            Acc
                    end
                end, Error, FlagSpecs).

is_not_kv_arg("-"++_Str) ->
    true;
is_not_kv_arg(Str) ->
    case lists:member($=, Str) of
        true ->
            false;
        false ->
            true
    end.

is_not_flag(Str) ->
    case lists:prefix("-", Str) of
        true ->
            try
                %% negative integers are arguments
                _ = list_to_integer(Str),
                true
            catch error:badarg ->
                false
            end;
        false ->
            true
    end.

%%-spec spaces(integer()) -> list().
%%spaces(Num) ->
%%    string:copies(" ",Num).

