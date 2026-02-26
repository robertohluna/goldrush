%% Copyright (c) 2012, Magnus Klaar <klaar@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


%% @doc Event filter implementation.
%%
%% An event query is constructed using the built in operators exported from
%% this module. The filtering operators are used to specify which events
%% should be included in the output of the query. The default output action
%% is to copy all events matching the input filters associated with a query
%% to the output. This makes it possible to construct and compose multiple
%% queries at runtime.
%%
%% === Examples of built in filters ===
%% ```
%% %% Select all events where 'a' exists and is greater than 0.
%% glc:gt(a, 0).
%% %% Select all events where 'a' exists and is equal to 0.
%% glc:eq(a, 0).
%% %% Select all events where 'a' exists and is not equal to 0.
%% glc:neq(a, 0).
%% %% Select all events where 'a' exists and is less than 0.
%% glc:lt(a, 0).
%% %% Select all events where 'a' exists and is anything.
%% glc:wc(a).
%%
%% %% Select no input events. Used as black hole query.
%% glc:null(false).
%% %% Select all input events. Used as passthrough query.
%% glc:null(true).
%% '''
%%
%% === Examples of combining filters ===
%% ```
%% %% Select all events where both 'a' and 'b' exists and are greater than 0.
%% glc:all([glc:gt(a, 0), glc:gt(b, 0)]).
%% %% Select all events where 'a' or 'b' exists and are greater than 0.
%% glc:any([glc:gt(a, 0), glc:gt(b, 0)]).
%% '''
%%
%% === Handling output events ===
%%
%% Once a query has been composed it is possible to override the output action
%% with an erlang function. The function will be applied to each output event
%% from the query. The return value from the function will be ignored.
%%
%% ```
%% %% Write all input events as info reports to the error logger.
%% glc:with(glc:null(true), fun(E) ->
%%     error_logger:info_report(gre:pairs(E)) end).
%% '''
%%
-module(glc).

-export([
    compile/2,
    compile/3,
    compile/4,
    handle/2,
    handle_many/2,
    get/2,
    delete/1,
    reset_counters/1,
    reset_counters/2,
    snapshot/1,
    explain/1,
    start/0,
    terminate/2
]).

-export([
    lt/2, lte/2,
    eq/2, neq/2,
    gt/2, gte/2,
    wc/1,
    nf/1
]).

-export([
    all/1,
    any/1,
    null/1,
    with/2,
    run/3
]).

-export([
    info/1,
    input/1,
    output/1,
    job_input/1,
    job_run/1,
    job_error/1,
    job_time/1,
    filter/1,
    union/1
]).

-record(module, {
    'query' :: term(),
    tables :: [{atom(), atom()}],
    qtree :: term(),
    store :: term()
}).

-spec lt(atom(), term()) -> glc_ops:op().
lt(Key, Term) ->
    glc_ops:lt(Key, Term).

-spec lte(atom(), term()) -> glc_ops:op().
lte(Key, Term) ->
    glc_ops:lte(Key, Term).

-spec eq(atom(), term()) -> glc_ops:op().
eq(Key, Term) ->
    glc_ops:eq(Key, Term).

-spec neq(atom(), term()) -> glc_ops:op().
neq(Key, Term) ->
    glc_ops:neq(Key, Term).

-spec gt(atom(), term()) -> glc_ops:op().
gt(Key, Term) ->
    glc_ops:gt(Key, Term).

-spec gte(atom(), term()) -> glc_ops:op().
gte(Key, Term) ->
    glc_ops:gte(Key, Term).

-spec wc(atom()) -> glc_ops:op().
wc(Key) ->
    glc_ops:wc(Key).

-spec nf(atom()) -> glc_ops:op().
nf(Key) ->
    glc_ops:nf(Key).

%% @doc Filter the input using multiple filters.
-spec all([glc_ops:op()]) -> glc_ops:op().
all(Filters) ->
    glc_ops:all(Filters).

%% @doc Filter the input using one of multiple filters.
-spec any([glc_ops:op()]) -> glc_ops:op().
any(Filters) ->
    glc_ops:any(Filters).

%% @doc Always return `true' or `false'.
-spec null(boolean()) -> glc_ops:op().
null(Result) ->
    glc_ops:null(Result).

%% @doc Apply a function to each output of a query.
-spec with(glc_ops:op(), fun((gre:event()) -> term())) -> glc_ops:op().
with(Query, Action) ->
    glc_ops:with(Query, Action).

%% @doc Return a union of multiple queries.
-spec union([glc_ops:op()]) -> glc_ops:op().
union(Queries) ->
    glc_ops:union(Queries).


%% @doc Compile a query to a module.
%%
%% On success the module representing the query is returned. The module and
%% data associated with the query must be released using the {@link delete/1}
%% function. The name of the query module is expected to be unique.
%% Counters are reset by default unless Reset is set to false.
-spec compile(atom(), glc_ops:op() | [glc_ops:op()]) -> {ok, atom()}.
compile(Module, Query) ->
    compile(Module, Query, [{statistics, true}]).

-spec compile(atom(), glc_ops:op() | [glc_ops:op()], atom() | list() | boolean()) -> {ok, atom()}.
compile(Module, Query, Store) when not is_boolean(Store) ->
    compile(Module, Query, Store, true);
compile(Module, Query, Reset) when is_boolean(Reset) ->
    compile(Module, Query, undefined, Reset).

compile(Module, Query, Store, Reset) when Store =:= []; Store =:= undefined ->
    compile(Module, Query, [{statistics, true}], Reset);
compile(Module, Query, Store, Reset) when is_list(Store) ->
    case lists:keyfind(statistics, 1, Store) of
        {_, true} ->
            compile(Module, Query, Store, true, Reset);
        _ ->
            compile(Module, Query, Store, false, false)
    end.

compile(Module, Query, Store, Stats, Reset) ->
    {ok, ModuleData} = module_data(Module, Query, Store, Stats),
    case glc_code:compile(Module, ModuleData, Stats) of
        {ok, Module} when Stats =:= true, Reset =:= true ->
            reset_counters(Module),
            {ok, Module};
        {ok, Module} ->
            {ok, Module}
    end.


%% @doc Handle an event using a compiled query.
%%
%% The input event is expected to have been returned from {@link gre:make/2}.
%% Also accepts raw proplists for convenience.
-spec handle(atom(), list({atom(), term()}) | gre:event()) -> ok.
handle(Module, Event) when is_list(Event) ->
    Module:handle(gre:make(Event, [list]));
handle(Module, Event) ->
    Module:handle(Event).

%% @doc Get a stored value from a compiled query module.
get(Module, Key) ->
    Module:get(Key).

%% @doc Execute a timed job through a compiled query module.
%%
%% The function is executed with timing instrumentation. The elapsed
%% time is injected as a `runtime' field into the resulting event,
%% which is then routed through the query filters.
run(Module, Fun, Event) when is_list(Event) ->
    Module:runjob(Fun, gre:make(Event, [list]));
run(Module, Fun, Event) ->
    Module:runjob(Fun, Event).


%% @doc Handle a list of events in one call.
%% More efficient than calling handle/2 in a loop: each event is processed
%% through the compiled filter individually, but avoids per-call overhead.
-spec handle_many(atom(), [gre:event() | [{atom(), term()}]]) -> ok.
handle_many(Module, Events) ->
    lists:foreach(fun(E) ->
        case E of
            E when is_list(E) -> Module:handle(gre:make(E, [list]));
            E -> Module:handle(E)
        end
    end, Events).

%% @doc Return all statistics as an atomic map snapshot.
%% Unlike info/1 which makes separate calls per counter, this reads
%% all counters in a single gen_server call for consistency.
-spec snapshot(atom()) -> #{atom() => non_neg_integer()}.
snapshot(Module) ->
    CountsTable = Module:table(counters),
    case CountsTable of
        undefined ->
            #{input => 0, filter => 0, output => 0,
              job_input => 0, job_run => 0,
              job_time => 0, job_error => 0};
        _ ->
            gr_counter:snapshot(CountsTable)
    end.

%% @doc Return a human-readable representation of the optimized query.
%% Shows the query tree after glc_lib:reduce/1 has been applied, which
%% may differ from the original due to flattening and deduplication.
-spec explain(atom()) -> iolist().
explain(Module) ->
    Tree = Module:explain(),
    glc_lib:pp(Tree).

%% @doc Return all statistics for a query module as a proplist.
info(Module) ->
    Counters = [input, filter, output,
                job_input, job_run,
                job_time, job_error],
    [{C, Module:info(C)} || C <- ['query' | Counters]].

%% @doc The number of input events for this query module.
-spec input(atom()) -> non_neg_integer().
input(Module) ->
    Module:info(input).

%% @doc The number of output events for this query module.
-spec output(atom()) -> non_neg_integer().
output(Module) ->
    Module:info(output).

%% @doc The number of filtered events for this query module.
-spec filter(atom()) -> non_neg_integer().
filter(Module) ->
    Module:info(filter).

%% @doc The number of job runs for this query module.
-spec job_run(atom()) -> non_neg_integer().
job_run(Module) ->
    Module:info(job_run).

%% @doc The number of job errors for this query module.
-spec job_error(atom()) -> non_neg_integer().
job_error(Module) ->
    Module:info(job_error).

%% @doc The number of job inputs for this query module.
-spec job_input(atom()) -> non_neg_integer().
job_input(Module) ->
    Module:info(job_input).

%% @doc The accumulated job time for this query module.
-spec job_time(atom()) -> non_neg_integer().
job_time(Module) ->
    Module:info(job_time).

%% @doc Terminate supervisors for a query module.
-spec terminate(atom(), all | counters | params) -> ok.
terminate(Module, counters) ->
    Counts = counts_name(Module),
    GuardCounts = guard_counts_name(Module),
    _ = [begin
        ok = supervisor:terminate_child(Sup, Name),
        ok = supervisor:delete_child(Sup, Name)
      end || {Sup, Name} <-
        [{gr_guardian_sup, GuardCounts},
         {gr_counter_sup, Counts}]
    ],
    ok;
terminate(Module, params) ->
    Params = params_name(Module),
    GuardParams = guard_params_name(Module),
    _ = [begin
        ok = supervisor:terminate_child(Sup, Name),
        ok = supervisor:delete_child(Sup, Name)
      end || {Sup, Name} <-
        [{gr_guardian_sup, GuardParams},
         {gr_param_sup, Params}]
    ],
    ok;
terminate(Module, all) ->
    catch (terminate(Module, counters)),
    terminate(Module, params).

%% @doc Release a compiled query.
%%
%% This releases all resources allocated by a compiled query. The query name
%% is expected to be associated with an existing query module.
-spec delete(atom()) -> ok.
delete(Module) ->
    ok = terminate(Module, all),
    code:soft_purge(Module),
    code:delete(Module),
    ok.

%% @doc Reset all counters for a compiled query.
-spec reset_counters(atom()) -> ok.
reset_counters(Module) ->
    Module:reset_counters(all).

%% @doc Reset a specific counter for a compiled query.
-spec reset_counters(atom(), atom()) -> ok.
reset_counters(Module, Counter) ->
    Module:reset_counters(Counter).

%% @private Serialize non-abstractable terms for storage in compiled code.
%% PIDs, ports, and references cannot be embedded as literals in generated
%% modules. We serialize them to binary and deserialize at runtime.
serialize_store(Store) when not is_list(Store) -> Store;
serialize_store(Store) ->
    lists:map(fun({K, V}) when is_pid(V); is_port(V); is_reference(V) ->
                    {K, {serialized, binary_to_list(term_to_binary(V))}};
                 ({K, V}) -> {K, V}
              end, Store).

%% @private Map a query to a module data term.
-spec module_data(atom(), term(), term(), boolean()) -> {ok, #module{}}.
module_data(Module, Query, Store, Stats) ->
    Tables = module_tables(Module, Stats),
    Query2 = glc_lib:reduce(Query),
    Store2 = serialize_store(Store),
    {ok, #module{'query'=Query, tables=Tables, qtree=Query2, store=Store2}}.

%% @private Create supervised processes for params and counter tables.
-spec module_tables(atom(), boolean()) -> list().
module_tables(Module, Stats) ->
    Params = params_name(Module),
    Counts = counts_name(Module),
    GuardParams = guard_params_name(Module),
    GuardCounts = guard_counts_name(Module),

    _ = supervisor:start_child(gr_param_sup,
        {Params, {gr_param, start_link, [Params]},
        transient, brutal_kill, worker, [Params]}),
    _ = supervisor:start_child(gr_guardian_sup,
        {GuardParams, {gr_guardian, start_link, [GuardParams, Params, []]},
        transient, brutal_kill, worker, [GuardParams]}),

    Tables = case Stats of
        true ->
            Counters = [{input,0}, {filter,0}, {output,0},
                        {job_input, 0}, {job_run,   0},
                        {job_time,  0}, {job_error, 0}],
            _ = supervisor:start_child(gr_counter_sup,
                {Counts, {gr_counter, start_link, [Counts]},
                transient, brutal_kill, worker, [Counts]}),
            _ = supervisor:start_child(gr_guardian_sup, {GuardCounts,
                {gr_guardian, start_link, [GuardCounts, Counts, Counters]},
                transient, brutal_kill, worker, [GuardCounts]}),
            [{counters, Counts}];
        false ->
            [{counters, undefined}]
     end,
    [{params, Params} | Tables].


%% @private Generate deterministic registered names for per-module processes.
reg_name(Module, Name) ->
    list_to_atom("gr_" ++ atom_to_list(Module) ++ Name).

params_name(Module) -> reg_name(Module, "_params").
counts_name(Module) -> reg_name(Module, "_counters").
guard_params_name(Module) -> reg_name(Module, "_params_grd").
guard_counts_name(Module) -> reg_name(Module, "_counters_grd").


%% @doc Start the goldrush application and its dependencies.
start() ->
    ok = application:start(syntax_tools),
    ok = application:start(compiler),
    ok = application:start(goldrush).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup_query(Module, Query) ->
    setup_query(Module, Query, [{statistics, true}]).

setup_query(Module, Query, Store) ->
    setup_query(Module, Query, Store, true).

setup_query(Module, Query, Store, Reset) ->
    case Reset of
        true  -> ?assertNot(erlang:module_loaded(Module));
        false -> ?assert(erlang:module_loaded(Module))
    end,
    ?assertEqual({ok, Module}, case (catch compile(Module, Query, Store, Reset)) of
        {'EXIT',_}=Error -> ?debugFmt("~p", [Error]), Error; Else -> Else end),
    ?assert(erlang:function_exported(Module, table, 1)),
    ?assert(erlang:function_exported(Module, handle, 1)),
    {compiled, Module}.

events_test_() ->
    {foreach,
        fun() ->
                error_logger:tty(false),
                application:start(syntax_tools),
                application:start(compiler),
                application:start(goldrush)
        end,
        fun(_) ->
                application:stop(goldrush),
                application:stop(compiler),
                application:stop(syntax_tools),
                error_logger:tty(true)
        end,
        [
            {"null query compiles",
                fun() ->
                    {compiled, Mod} = setup_query(testmod1, glc:null(false)),
                    ?assertError(badarg, Mod:table(noexists))
                end
            },
            {"params table exists",
                fun() ->
                    {compiled, Mod} = setup_query(testmod2, glc:null(false)),
                    ?assert(is_atom(Mod:table(params))),
                    ?assertMatch([_|_], gr_param:info(Mod:table(params)))
                end
            },
            {"null query exists",
                fun() ->
                    {compiled, Mod} = setup_query(testmod3, glc:null(false)),
                    ?assert(erlang:function_exported(Mod, info, 1)),
                    ?assertError(badarg, Mod:info(invalid)),
                    ?assertEqual({null, false}, Mod:info('query'))
                end
            },
            {"init counters test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod4, glc:null(false)),
                    ?assertEqual(0, Mod:info(input)),
                    ?assertEqual(0, Mod:info(filter)),
                    ?assertEqual(0, Mod:info(output))
                end
            },
            {"filtered event test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod5, glc:null(false)),
                    glc:handle(Mod, gre:make([], [list])),
                    ?assertEqual(1, Mod:info(input)),
                    ?assertEqual(1, Mod:info(filter)),
                    ?assertEqual(0, Mod:info(output))
                end
            },
            {"nomatch event test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod6, glc:eq('$n', 'noexists@nohost')),
                    glc:handle(Mod, gre:make([{'$n', 'noexists2@nohost'}], [list])),
                    ?assertEqual(1, Mod:info(input)),
                    ?assertEqual(1, Mod:info(filter)),
                    ?assertEqual(0, Mod:info(output))
                end
            },
            {"opfilter eq test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod7, glc:eq('$n', 'noexists@nohost')),
                    glc:handle(Mod, gre:make([{'$n', 'noexists@nohost'}], [list])),
                    ?assertEqual(1, Mod:info(input)),
                    ?assertEqual(0, Mod:info(filter)),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"opfilter gt test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod8, glc:gt(a, 1)),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    ?assertEqual(1, Mod:info(input)),
                    ?assertEqual(0, Mod:info(filter)),
                    glc:handle(Mod, gre:make([{'a', 0}], [list])),
                    ?assertEqual(2, Mod:info(input)),
                    ?assertEqual(1, Mod:info(filter)),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"opfilter lt test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9, glc:lt(a, 1)),
                    glc:handle(Mod, gre:make([{'a', 0}], [list])),
                    ?assertEqual(1, Mod:info(input)),
                    ?assertEqual(0, Mod:info(filter)),
                    ?assertEqual(1, Mod:info(output)),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    ?assertEqual(2, Mod:info(input)),
                    ?assertEqual(1, Mod:info(filter)),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"opfilter lte test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9a, glc:lte(a, 1)),
                    glc:handle(Mod, gre:make([{a, 1}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    glc:handle(Mod, gre:make([{a, 2}], [list])),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"opfilter gte test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9b, glc:gte(a, 2)),
                    glc:handle(Mod, gre:make([{a, 2}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    glc:handle(Mod, gre:make([{a, 1}], [list])),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"opfilter neq test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9c, glc:neq(a, 1)),
                    glc:handle(Mod, gre:make([{a, 2}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    glc:handle(Mod, gre:make([{a, 1}], [list])),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"wildcard op test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9d, glc:wc(a)),
                    glc:handle(Mod, gre:make([{a, anything}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    glc:handle(Mod, gre:make([{b, 1}], [list])),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"notfound op test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9e, glc:nf(a)),
                    glc:handle(Mod, gre:make([{b, 1}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    glc:handle(Mod, gre:make([{a, 1}], [list])),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"allholds op test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod10,
                        glc:all([glc:eq(a, 1), glc:eq(b, 2)])),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    ?assertEqual(2, Mod:info(input)),
                    ?assertEqual(2, Mod:info(filter)),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(4, Mod:info(input)),
                    ?assertEqual(4, Mod:info(filter)),
                    glc:handle(Mod, gre:make([{'a', 1},{'b', 2}], [list])),
                    ?assertEqual(5, Mod:info(input)),
                    ?assertEqual(4, Mod:info(filter)),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"anyholds op test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod11,
                        glc:any([glc:eq(a, 1), glc:eq(b, 2)])),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    ?assertEqual(2, Mod:info(input)),
                    ?assertEqual(2, Mod:info(filter)),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(4, Mod:info(input)),
                    ?assertEqual(2, Mod:info(filter))
                end
            },
            {"with function test",
                fun() ->
                    Self = self(),
                    {compiled, Mod} = setup_query(testmod12,
                        glc:with(glc:eq(a, 1), fun(Event) -> Self ! gre:fetch(a, Event) end)),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    ?assertEqual(1, receive Msg -> Msg after 0 -> notcalled end)
                end
            },
            {"with arity-2 function test",
                fun() ->
                    Self = self(),
                    Store = [{mykey, myval}, {statistics, true}],
                    {compiled, Mod} = setup_query(testmod12b,
                        glc:with(glc:eq(a, 1), fun(Event, EStore) ->
                            Self ! {gre:fetch(a, Event), EStore} end),
                        Store),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(1, Mod:info(output)),
                    Received = receive Msg -> Msg after 100 -> timeout end,
                    ?assertMatch({1, _}, Received)
                end
            },
            {"reset counters test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod13, glc:null(true)),
                    glc:handle(Mod, gre:make([], [list])),
                    ?assertEqual(1, Mod:info(input)),
                    ?assertEqual(1, Mod:info(output)),
                    glc:reset_counters(Mod),
                    ?assertEqual(0, Mod:info(input)),
                    ?assertEqual(0, Mod:info(output))
                end
            },
            {"delete test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod14, glc:null(true)),
                    glc:delete(Mod),
                    ?assertNot(erlang:module_loaded(Mod))
                end
            },
            {"handle raw list test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod14b, glc:eq(a, 1)),
                    glc:handle(Mod, [{a, 1}]),
                    ?assertEqual(1, Mod:info(output))
                end
            },
            {"stored value test",
                fun() ->
                    Store = [{stored, value}, {statistics, true}],
                    {compiled, Mod} = setup_query(testmod15,
                        glc:null(true), Store),
                    ?assertEqual({ok, value}, glc:get(Mod, stored)),
                    ?assertEqual({error, undefined}, glc:get(Mod, nonexist))
                end
            },
            {"no statistics test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod16,
                        glc:null(true), [{statistics, false}]),
                    glc:handle(Mod, gre:make([], [list])),
                    ?assertEqual(0, Mod:info(input)),
                    ?assertEqual(0, Mod:info(output))
                end
            },
            {"delete without statistics test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod17,
                        glc:null(true), [{statistics, false}]),
                    glc:delete(Mod),
                    ?assertNot(erlang:module_loaded(Mod))
                end
            },
            {"run timed job test",
                fun() ->
                    Self = self(),
                    {compiled, Mod} = setup_query(testmod18,
                        glc:with(glc:gte(runtime, 0.0),
                            fun(Event) -> Self ! gre:fetch(runtime, Event) end)),
                    glc:run(Mod, fun(Event, _Store) ->
                        gre:fetch(a, Event) + 1
                    end, gre:make([{a, 1}], [list])),
                    Runtime = receive R -> R after 1000 -> timeout end,
                    ?assert(is_float(Runtime)),
                    ?assert(Runtime >= 0.0)
                end
            },
            {"info aggregate test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod19, glc:null(true)),
                    glc:handle(Mod, gre:make([], [list])),
                    Info = glc:info(Mod),
                    ?assert(is_list(Info)),
                    ?assertEqual(1, proplists:get_value(input, Info)),
                    ?assertEqual(1, proplists:get_value(output, Info))
                end
            },
            {"union error test",
                fun() ->
                    ?assertError(badarg, glc:union([glc:eq(a, 1)]))
                end
            },
            {"snapshot returns consistent map",
                fun() ->
                    {compiled, Mod} = setup_query(testmod20, glc:null(true)),
                    glc:handle(Mod, gre:make([], [list])),
                    glc:handle(Mod, gre:make([], [list])),
                    Snap = glc:snapshot(Mod),
                    ?assert(is_map(Snap)),
                    ?assertEqual(2, maps:get(input, Snap)),
                    ?assertEqual(2, maps:get(output, Snap)),
                    ?assertEqual(0, maps:get(filter, Snap))
                end
            },
            {"handle_many processes batch",
                fun() ->
                    {compiled, Mod} = setup_query(testmod21, glc:eq(a, 1)),
                    Events = [gre:make([{a, 1}], [list]),
                              gre:make([{a, 2}], [list]),
                              [{a, 1}]],
                    glc:handle_many(Mod, Events),
                    ?assertEqual(3, Mod:info(input)),
                    ?assertEqual(2, Mod:info(output)),
                    ?assertEqual(1, Mod:info(filter))
                end
            },
            {"explain returns readable output",
                fun() ->
                    {compiled, Mod} = setup_query(testmod22,
                        glc:all([glc:eq(a, 1), glc:gt(b, 2)])),
                    Result = glc:explain(Mod),
                    ?assert(is_list(Result) orelse is_binary(Result)),
                    Flat = iolist_to_binary(Result),
                    ?assert(byte_size(Flat) > 0),
                    ?assertNotEqual(nomatch, binary:match(Flat, <<"a">>)),
                    ?assertNotEqual(nomatch, binary:match(Flat, <<"b">>))
                end
            }
        ]
    }.

-endif.
