%% @doc Integration tests for the goldrush event stream processing library.
%%
%% These tests exercise the full pipeline: query compilation, event routing,
%% counter tracking, parameter storage, crash recovery, and the evolution
%% features (snapshot, handle_many, explain).
-module(glc_integration_tests).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test fixtures
%%%===================================================================

setup() ->
    error_logger:tty(false),
    application:start(syntax_tools),
    application:start(compiler),
    application:start(goldrush).

teardown(_) ->
    application:stop(goldrush),
    application:stop(compiler),
    application:stop(syntax_tools),
    error_logger:tty(true).

%%%===================================================================
%%% Compilation and lifecycle tests
%%%===================================================================

lifecycle_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"compile and delete round trip",
            fun() ->
                {ok, Mod} = glc:compile(lifecycle_mod1, glc:null(true)),
                ?assert(erlang:module_loaded(Mod)),
                glc:delete(Mod),
                ?assertNot(erlang:module_loaded(Mod))
            end
        },
        {"recompile same module name",
            fun() ->
                {ok, Mod} = glc:compile(lifecycle_mod2, glc:eq(a, 1)),
                glc:handle(Mod, [{a, 1}]),
                ?assertEqual(1, Mod:info(output)),
                glc:delete(Mod),
                {ok, Mod2} = glc:compile(lifecycle_mod2, glc:eq(a, 2)),
                ?assertEqual(Mod, Mod2),
                glc:handle(Mod2, [{a, 2}]),
                ?assertEqual(1, Mod2:info(output))
            end
        }
    ]}.

%%%===================================================================
%%% Operator coverage tests
%%%===================================================================

operator_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"all operators match correctly",
            fun() ->
                %% lt
                {ok, M1} = glc:compile(op_lt, glc:lt(x, 10)),
                glc:handle(M1, [{x, 5}]),
                ?assertEqual(1, M1:info(output)),
                glc:handle(M1, [{x, 15}]),
                ?assertEqual(1, M1:info(output)),

                %% lte
                {ok, M2} = glc:compile(op_lte, glc:lte(x, 10)),
                glc:handle(M2, [{x, 10}]),
                ?assertEqual(1, M2:info(output)),
                glc:handle(M2, [{x, 11}]),
                ?assertEqual(1, M2:info(output)),

                %% eq
                {ok, M3} = glc:compile(op_eq, glc:eq(x, 10)),
                glc:handle(M3, [{x, 10}]),
                ?assertEqual(1, M3:info(output)),
                glc:handle(M3, [{x, 11}]),
                ?assertEqual(1, M3:info(output)),

                %% neq
                {ok, M4} = glc:compile(op_neq, glc:neq(x, 10)),
                glc:handle(M4, [{x, 5}]),
                ?assertEqual(1, M4:info(output)),
                glc:handle(M4, [{x, 10}]),
                ?assertEqual(1, M4:info(output)),

                %% gt
                {ok, M5} = glc:compile(op_gt, glc:gt(x, 10)),
                glc:handle(M5, [{x, 15}]),
                ?assertEqual(1, M5:info(output)),
                glc:handle(M5, [{x, 5}]),
                ?assertEqual(1, M5:info(output)),

                %% gte
                {ok, M6} = glc:compile(op_gte, glc:gte(x, 10)),
                glc:handle(M6, [{x, 10}]),
                ?assertEqual(1, M6:info(output)),
                glc:handle(M6, [{x, 5}]),
                ?assertEqual(1, M6:info(output)),

                %% wc (wildcard - exists)
                {ok, M7} = glc:compile(op_wc, glc:wc(x)),
                glc:handle(M7, [{x, anything}]),
                ?assertEqual(1, M7:info(output)),
                glc:handle(M7, [{y, 1}]),
                ?assertEqual(1, M7:info(output)),

                %% nf (not found)
                {ok, M8} = glc:compile(op_nf, glc:nf(x)),
                glc:handle(M8, [{y, 1}]),
                ?assertEqual(1, M8:info(output)),
                glc:handle(M8, [{x, 1}]),
                ?assertEqual(1, M8:info(output))
            end
        }
    ]}.

%%%===================================================================
%%% Composite query tests
%%%===================================================================

composite_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"all requires every condition",
            fun() ->
                Q = glc:all([glc:eq(a, 1), glc:gt(b, 5)]),
                {ok, Mod} = glc:compile(comp_all, Q),
                glc:handle(Mod, [{a, 1}, {b, 10}]),
                ?assertEqual(1, Mod:info(output)),
                glc:handle(Mod, [{a, 1}, {b, 3}]),
                ?assertEqual(1, Mod:info(output)),
                glc:handle(Mod, [{a, 2}, {b, 10}]),
                ?assertEqual(1, Mod:info(output))
            end
        },
        {"any requires at least one condition",
            fun() ->
                Q = glc:any([glc:eq(a, 1), glc:eq(b, 2)]),
                {ok, Mod} = glc:compile(comp_any, Q),
                glc:handle(Mod, [{a, 1}]),
                ?assertEqual(1, Mod:info(output)),
                glc:handle(Mod, [{b, 2}]),
                ?assertEqual(2, Mod:info(output)),
                glc:handle(Mod, [{c, 3}]),
                ?assertEqual(2, Mod:info(output))
            end
        },
        {"nested all/any composition",
            fun() ->
                Q = glc:all([
                    glc:eq(env, prod),
                    glc:any([glc:eq(level, error), glc:eq(level, critical)])
                ]),
                {ok, Mod} = glc:compile(comp_nested, Q),
                glc:handle(Mod, [{env, prod}, {level, error}]),
                ?assertEqual(1, Mod:info(output)),
                glc:handle(Mod, [{env, prod}, {level, info}]),
                ?assertEqual(1, Mod:info(output)),
                glc:handle(Mod, [{env, dev}, {level, error}]),
                ?assertEqual(1, Mod:info(output))
            end
        }
    ]}.

%%%===================================================================
%%% Counter and statistics tests
%%%===================================================================

counter_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"counter reset works",
            fun() ->
                {ok, Mod} = glc:compile(ctr_reset, glc:null(true)),
                glc:handle(Mod, []),
                glc:handle(Mod, []),
                ?assertEqual(2, Mod:info(input)),
                glc:reset_counters(Mod),
                ?assertEqual(0, Mod:info(input)),
                ?assertEqual(0, Mod:info(output))
            end
        },
        {"selective counter reset",
            fun() ->
                {ok, Mod} = glc:compile(ctr_sel, glc:null(true)),
                glc:handle(Mod, []),
                ?assertEqual(1, Mod:info(input)),
                ?assertEqual(1, Mod:info(output)),
                glc:reset_counters(Mod, input),
                ?assertEqual(0, Mod:info(input)),
                ?assertEqual(1, Mod:info(output))
            end
        },
        {"no statistics mode returns zeros",
            fun() ->
                {ok, Mod} = glc:compile(ctr_nostats,
                    glc:null(true), [{statistics, false}]),
                glc:handle(Mod, []),
                ?assertEqual(0, Mod:info(input)),
                ?assertEqual(0, Mod:info(output))
            end
        }
    ]}.

%%%===================================================================
%%% Evolution feature tests: snapshot
%%%===================================================================

snapshot_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"snapshot returns map with all counters",
            fun() ->
                {ok, Mod} = glc:compile(snap_all, glc:null(true)),
                Snap = glc:snapshot(Mod),
                ?assert(is_map(Snap)),
                ?assert(maps:is_key(input, Snap)),
                ?assert(maps:is_key(filter, Snap)),
                ?assert(maps:is_key(output, Snap)),
                ?assert(maps:is_key(job_input, Snap)),
                ?assert(maps:is_key(job_run, Snap)),
                ?assert(maps:is_key(job_time, Snap)),
                ?assert(maps:is_key(job_error, Snap))
            end
        },
        {"snapshot reflects event processing",
            fun() ->
                {ok, Mod} = glc:compile(snap_proc, glc:eq(a, 1)),
                glc:handle(Mod, [{a, 1}]),
                glc:handle(Mod, [{a, 2}]),
                glc:handle(Mod, [{a, 1}]),
                Snap = glc:snapshot(Mod),
                ?assertEqual(3, maps:get(input, Snap)),
                ?assertEqual(2, maps:get(output, Snap)),
                ?assertEqual(1, maps:get(filter, Snap))
            end
        },
        {"snapshot with no statistics returns zeros",
            fun() ->
                {ok, Mod} = glc:compile(snap_nostat,
                    glc:null(true), [{statistics, false}]),
                Snap = glc:snapshot(Mod),
                ?assert(is_map(Snap)),
                ?assertEqual(0, maps:get(input, Snap))
            end
        }
    ]}.

%%%===================================================================
%%% Evolution feature tests: handle_many
%%%===================================================================

handle_many_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"handle_many processes all events",
            fun() ->
                {ok, Mod} = glc:compile(hmany_basic, glc:null(true)),
                Events = [gre:make([{a, 1}], [list]),
                          gre:make([{b, 2}], [list]),
                          gre:make([{c, 3}], [list])],
                glc:handle_many(Mod, Events),
                ?assertEqual(3, Mod:info(input)),
                ?assertEqual(3, Mod:info(output))
            end
        },
        {"handle_many with mixed formats",
            fun() ->
                {ok, Mod} = glc:compile(hmany_mixed, glc:eq(a, 1)),
                Events = [
                    gre:make([{a, 1}], [list]),   %% gre event, match
                    [{a, 1}],                      %% raw proplist, match
                    [{a, 2}],                      %% raw proplist, no match
                    gre:make([{b, 1}], [list])     %% gre event, no match
                ],
                glc:handle_many(Mod, Events),
                ?assertEqual(4, Mod:info(input)),
                ?assertEqual(2, Mod:info(output)),
                ?assertEqual(2, Mod:info(filter))
            end
        },
        {"handle_many empty list is no-op",
            fun() ->
                {ok, Mod} = glc:compile(hmany_empty, glc:null(true)),
                glc:handle_many(Mod, []),
                ?assertEqual(0, Mod:info(input))
            end
        }
    ]}.

%%%===================================================================
%%% Evolution feature tests: explain
%%%===================================================================

explain_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"explain returns readable iolist",
            fun() ->
                {ok, Mod} = glc:compile(expl_basic, glc:eq(a, 1)),
                Result = glc:explain(Mod),
                Flat = iolist_to_binary(Result),
                ?assert(byte_size(Flat) > 0),
                ?assertNotEqual(nomatch, binary:match(Flat, <<"a">>))
            end
        },
        {"explain shows composite query structure",
            fun() ->
                Q = glc:all([glc:eq(x, 1), glc:gt(y, 5)]),
                {ok, Mod} = glc:compile(expl_comp, Q),
                Result = iolist_to_binary(glc:explain(Mod)),
                ?assertNotEqual(nomatch, binary:match(Result, <<"all(">>)),
                ?assertNotEqual(nomatch, binary:match(Result, <<"x">>)),
                ?assertNotEqual(nomatch, binary:match(Result, <<"y">>))
            end
        },
        {"explain null queries",
            fun() ->
                {ok, Mod1} = glc:compile(expl_true, glc:null(true)),
                R1 = iolist_to_binary(glc:explain(Mod1)),
                ?assertEqual(<<"*">>, R1),
                glc:delete(Mod1),

                {ok, Mod2} = glc:compile(expl_false, glc:null(false)),
                R2 = iolist_to_binary(glc:explain(Mod2)),
                ?assertEqual(<<"(none)">>, R2)
            end
        }
    ]}.

%%%===================================================================
%%% With function and store tests
%%%===================================================================

with_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"with function receives matching events",
            fun() ->
                Self = self(),
                Q = glc:with(glc:eq(msg, hello),
                    fun(E) -> Self ! {got, gre:fetch(msg, E)} end),
                {ok, Mod} = glc:compile(with_fn, Q),
                glc:handle(Mod, [{msg, hello}]),
                ?assertEqual({got, hello},
                    receive M -> M after 500 -> timeout end)
            end
        },
        {"with arity-2 function gets store",
            fun() ->
                Self = self(),
                Store = [{tag, test_value}, {statistics, true}],
                Q = glc:with(glc:null(true),
                    fun(_E, S) -> Self ! {store, S} end),
                {ok, Mod} = glc:compile(with_store, Q, Store),
                glc:handle(Mod, []),
                Received = receive M -> M after 500 -> timeout end,
                ?assertMatch({store, _}, Received)
            end
        }
    ]}.

%%%===================================================================
%%% Job execution tests
%%%===================================================================

job_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"run executes timed job",
            fun() ->
                Self = self(),
                Q = glc:with(glc:gte(runtime, 0.0),
                    fun(E) -> Self ! {runtime, gre:fetch(runtime, E)} end),
                {ok, Mod} = glc:compile(job_timed, Q),
                glc:run(Mod, fun(E, _S) ->
                    gre:fetch(a, E) * 2
                end, gre:make([{a, 5}], [list])),
                Runtime = receive {runtime, R} -> R after 1000 -> timeout end,
                ?assert(is_float(Runtime)),
                ?assert(Runtime >= 0.0)
            end
        }
    ]}.

%%%===================================================================
%%% Query reduction tests
%%%===================================================================

reduction_test_() ->
    [
        {"reduce flattens nested all",
            fun() ->
                Q = glc:all([glc:eq(a, 1), glc:all([glc:eq(b, 2)])]),
                R = glc_lib:reduce(Q),
                ?assertEqual(glc:all([glc:eq(a, 1), glc:eq(b, 2)]), R)
            end
        },
        {"reduce removes duplicates",
            fun() ->
                Q = glc:all([glc:eq(a, 1), glc:eq(a, 1)]),
                R = glc_lib:reduce(Q),
                ?assertEqual(glc:eq(a, 1), R)
            end
        },
        {"reduce flattens singleton",
            fun() ->
                Q = glc:any([glc:eq(a, 1)]),
                R = glc_lib:reduce(Q),
                ?assertEqual(glc:eq(a, 1), R)
            end
        }
    ].

%%%===================================================================
%%% Pretty printer tests
%%%===================================================================

pp_test_() ->
    [
        {"pp formats equality",
            fun() ->
                R = iolist_to_binary(glc_lib:pp(glc:eq(x, 1))),
                ?assertEqual(<<"x==1">>, R)
            end
        },
        {"pp formats comparison ops",
            fun() ->
                ?assertEqual(<<"x>5">>,
                    iolist_to_binary(glc_lib:pp(glc:gt(x, 5)))),
                ?assertEqual(<<"x<5">>,
                    iolist_to_binary(glc_lib:pp(glc:lt(x, 5)))),
                ?assertEqual(<<"x>=5">>,
                    iolist_to_binary(glc_lib:pp(glc:gte(x, 5)))),
                ?assertEqual(<<"x=<5">>,
                    iolist_to_binary(glc_lib:pp(glc:lte(x, 5)))),
                ?assertEqual(<<"x!=5">>,
                    iolist_to_binary(glc_lib:pp(glc:neq(x, 5))))
            end
        },
        {"pp formats wildcard and notfound",
            fun() ->
                ?assertEqual(<<"x:exists">>,
                    iolist_to_binary(glc_lib:pp(glc:wc(x)))),
                ?assertEqual(<<"x:missing">>,
                    iolist_to_binary(glc_lib:pp(glc:nf(x))))
            end
        },
        {"pp formats null",
            fun() ->
                ?assertEqual(<<"*">>,
                    iolist_to_binary(glc_lib:pp(glc:null(true)))),
                ?assertEqual(<<"(none)">>,
                    iolist_to_binary(glc_lib:pp(glc:null(false))))
            end
        },
        {"pp formats composite queries",
            fun() ->
                Q = glc:all([glc:eq(a, 1), glc:gt(b, 2)]),
                R = iolist_to_binary(glc_lib:pp(Q)),
                ?assertNotEqual(nomatch, binary:match(R, <<"all(">>)),
                ?assertNotEqual(nomatch, binary:match(R, <<"a==1">>)),
                ?assertNotEqual(nomatch, binary:match(R, <<"b>2">>))
            end
        }
    ].
