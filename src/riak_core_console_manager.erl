-module(riak_core_console_manager).

-export([run/5]).

-behaviour(gen_server).

%% gen_server api
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
         run/4]).

-type name() :: string().
-type callback() :: fun().
-type str_to_type() :: fun().
-type description() :: string().
-type action() :: {name(), callback(), [str_to_type()], description()}.

-type short() :: string().
-type long() :: string().
-type flag() :: {long(), short(), str_to_type(), description()}.

-record(spec, {script :: string(),
               command :: string(),
               subcommand :: string(),
               actions :: [action()],
               flags :: [flag()]}).

%% This table should exist for the runtime of the VM and is registered in
%% riak_core_console_manager_sup.erl
-define(table, riak_core_console_specs).

-record(state, {table=?table}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_command(Script, Cmd, SubCmd, Actions, Flags) ->
    Msg = {register_command, Script, Cmd, SubCmd, Actions, Flags},
    gen_server:call(?MODULE, Msg, 5000).

run(Script, Cmd, SubCmd, Args) ->
    Msg = {run, Script, Cmd, SubCmd, Args},
    gen_server:cast(?MODULE, Msg).

init([]) ->
    %% TODO: This line should not really exist, but we don't have actual
    %% processes to register things independently yet. Remove it when feasable.
    gen_server:cast(?MODULE, register_commands),
    {ok, #state{}}.

handle_call({register_command, Script, Cmd, SubCmd, Actions, Flags}, _From,
  State=#state{table=Table}) ->
    true = ets:insert(Table, {{Script, Cmd, SubCmd}, Actions, Flags}),
    {reply, ok, State}.

%% TODO: Remove this clause when commands are registered outside this module by
%% their responsible processes.
handle_cast(register_commands, State) ->
    spawn(fun() ->
                riak_core_console:register_handoff_commands()
          end),
    {noreply, State};
handle_cast({run, Script, Cmd, SubCmd, Args}, State=#state{table=Table}) ->
    run(Script, Cmd, SubCmd, Args, Table),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

run(Script, Cmd, SubCmd, [], Table) ->
    case match(Script, Cmd, SubCmd, Table) of
        {error, _}=E ->
            print_error(E);
        Spec ->
            usage(Script, Spec)
    end;
run(Script, Cmd, SubCmd, Args, Table) ->
    M0 = match(Script, Cmd, SubCmd, Table),
    M1 = parse(M0, Args),
    M2 = validate(M1),
    run(M2).

run({error, _}=E) ->
    print_error(E);
run({Spec, [Action | Args], Flags}) ->
    {Action, Fun, _, _} = lists:keyfind(Action, 1, Spec#spec.actions),
    %% TODO: Should probably wrap this in a try/catch
    Fun(Args, Flags).

usage(Script, Spec) ->
    [Action | Actions] = Spec#spec.actions,
    _Flags = Spec#spec.flags,
    CommandStr = command_str(Script, Spec#spec.command, Spec#spec.subcommand),
    Str0 = "Usage:  " ++ CommandStr ++ action_string(Action) ++ "\n",
    Rest = [spaces(8) ++ CommandStr ++ action_string(A) ++ "\n"
        || A <- Actions],
    io:format("~p~p", [Str0, Rest]).

command_str(Script, Command, Subcommand) ->
    Script ++ " " ++ Command ++ " " ++ Subcommand.

action_string({Name, _, Funs, _}) ->
    NumArgs = length(Funs),
    Args = [" <Arg"++integer_to_list(I)++">" || I <- lists:seq(1, NumArgs)],
    [Name | Args].

print_error({error, no_matching_spec}) ->
    io:format("Invalid Command/Subcommand.");
print_error({error, {invalid_flag, Str}}) ->
    io:format("Invalid Flag: ~p.", [Str]);
print_error({error, {invalid_action, Str}}) ->
    io:format("Invalid Action: ~p.", [Str]);
print_error({error, invalid_number_of_args}) ->
    io:format("Invalid number of arguments.");
print_error({error, {invalid_argument, Str}}) ->
    io:format("Invalid argument: ~p.", [Str]);
print_error({error, {invalid_flags, Flags}}) ->
    io:format("Invalid Flags: ~p.", [Flags]);
print_error({error, {invalid_flag_value, {Name, Val}}}) ->
    io:format("Invalid value: ~p for flag: ~p", [Val, Name]);
print_error({error, {invalid_value, Val}}) ->
    io:format("Invalid value: ~p.", [Val]).

match(Script, Cmd, SubCmd, Table) ->
    case ets:lookup(Table, {Script, Cmd, SubCmd}) of
        [{{Script, Cmd, SubCmd}, Actions, Flags}] ->
            #spec{script=Script,
                  command=Cmd,
                  subcommand=SubCmd,
                  actions=Actions,
                  flags=Flags};
        [] ->
            {error, no_matching_spec}
    end.

parse({error, _}=E, _Args) ->
    E;
parse(Spec, ArgsAndFlags) ->
    %%ArgsAndFlags = lists:split(" ", ArgsAndFlags0),
    %% Positional args always come before flags in our cli
    {Args, Flags0} = lists:splitwith(fun is_not_flag/1, ArgsAndFlags),
    case parse_flags(Flags0) of
        {error, _}=E ->
            E;
        Flags ->
            {Spec, Args, Flags}
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
            parse_flags(T, [], [{Flag, Val} | Acc]);
        [Flag] ->
            parse_flags(T, [Flag], Acc)
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
    case validate_args(Spec, Args0) of
        {error, _}=E ->
            E;
        Args ->
            case validate_flags(Spec, Flags0) of
                {error, _}=E ->
                    E;
                Flags ->
                    {Spec, Args, Flags}
            end
    end.

validate_args(#spec{actions=Actions}, [Name | Args]) ->
    case lists:keyfind(Name, 1, Actions) of
        false ->
            {error, {invalid_action, Name}};
        {Name, _, Funs, _} ->
            convert_args(Name, Funs, Args, [])
    end.

convert_args(Name, [], [], Acc) ->
    [Name | lists:reverse(Acc)];
convert_args(_Name, Funs, Args, _Acc) when length(Funs) =/= length(Args) ->
    {error, invalid_number_of_args};
convert_args(Name, [F | Funs], [A | Args], Acc) ->
    try
        Arg = F(A),
        convert_args(Name, Funs, Args, [Arg | Acc])
    catch error:badarg ->
        {error, {invalid_argument, A}}
    end.

validate_flags(#spec{flags=Flags}, UserFlags) ->
    convert_flags(Flags, UserFlags, []).

convert_flags([], [], Acc) ->
    Acc;
convert_flags(_Flags, [], Acc) ->
    Acc;
convert_flags([], Provided, _Acc) ->
    Invalid = [Flag || {Flag, _} <- Provided],
    {error, {invalid_flags, Invalid}};
convert_flags(Flags, [{Name, Val0} | T], Acc) ->
    case lists:keyfind(Name, 1, Flags) of
        false ->
            case lists:keyfind(Name, 2, Flags) of
                false ->
                    {error, {invalid_flag, Name}};
                {_, _, Fun, _} ->
                    try
                        Val = Fun(Val0),
                        convert_flags(Flags, T, [{Name, Val} | Acc])
                    catch error:badarg ->
                        {error, {invalid_flag_value, {Name, Val0}}}
                    end
            end;
        {_, _, Fun, _} ->
            try
                Val = Fun(Val0),
                convert_flags(Flags, T, [{Name, Val} | Acc])
            catch error:badarg ->
                {error, {invalid_flag_value, {Name, Val0}}}
            end
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

-spec spaces(integer()) -> list().
spaces(Num) ->
    string:copies(" ",Num).

