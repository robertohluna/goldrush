%% @doc Tests for the supervised worker processes (counter, param, guardian).
%%
%% Validates crash recovery via the ETS heir pattern, counter operations,
%% and parameter storage.
-module(gr_worker_tests).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Fixtures
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
%%% Counter worker tests
%%%===================================================================

counter_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"counter insert and lookup",
            fun() ->
                {ok, Mod} = glc:compile(cw_test1, glc:null(true)),
                Tab = Mod:table(counters),
                ?assert(Tab =/= undefined),
                %% Counters were initialized during compile
                Val = gr_counter:lookup_element(Tab, input),
                ?assert(is_integer(Val))
            end
        },
        {"counter update via cast",
            fun() ->
                {ok, Mod} = glc:compile(cw_test2, glc:null(true)),
                Tab = Mod:table(counters),
                gr_counter:update_counter(Tab, input, {2, 5}),
                timer:sleep(50),  %% cast is async
                Val = gr_counter:lookup_element(Tab, input),
                ?assertEqual(5, Val)
            end
        },
        {"counter list returns all entries",
            fun() ->
                {ok, Mod} = glc:compile(cw_test3, glc:null(true)),
                Tab = Mod:table(counters),
                List = gr_counter:list(Tab),
                ?assert(is_list(List)),
                Keys = [K || {K, _} <- List],
                ?assert(lists:member(input, Keys)),
                ?assert(lists:member(output, Keys)),
                ?assert(lists:member(filter, Keys))
            end
        },
        {"counter snapshot returns map",
            fun() ->
                {ok, Mod} = glc:compile(cw_test4, glc:null(true)),
                Tab = Mod:table(counters),
                Snap = gr_counter:snapshot(Tab),
                ?assert(is_map(Snap)),
                ?assertEqual(0, maps:get(input, Snap))
            end
        },
        {"counter batch_update applies deltas",
            fun() ->
                {ok, Mod} = glc:compile(cw_test5, glc:null(true)),
                Tab = Mod:table(counters),
                gr_counter:batch_update(Tab, [{input, 3}, {output, 7}]),
                timer:sleep(50),  %% cast is async
                Snap = gr_counter:snapshot(Tab),
                ?assertEqual(3, maps:get(input, Snap)),
                ?assertEqual(7, maps:get(output, Snap))
            end
        }
    ]}.

%%%===================================================================
%%% Parameter worker tests
%%%===================================================================

param_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"param insert and lookup",
            fun() ->
                {ok, Mod} = glc:compile(pw_test1, glc:null(true)),
                Tab = Mod:table(params),
                gr_param:insert(Tab, {mykey, myval}),
                Result = gr_param:lookup(Tab, mykey),
                ?assertEqual([{mykey, myval}], Result)
            end
        },
        {"param info returns table data",
            fun() ->
                {ok, Mod} = glc:compile(pw_test2, glc:null(true)),
                Tab = Mod:table(params),
                Info = gr_param:info(Tab),
                ?assert(is_list(Info))
            end
        }
    ]}.

%%%===================================================================
%%% Guardian crash recovery tests
%%%===================================================================

guardian_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        {"counter survives worker crash",
            fun() ->
                {ok, Mod} = glc:compile(grd_test1, glc:null(true)),
                %% Process some events
                glc:handle(Mod, []),
                glc:handle(Mod, []),
                ?assertEqual(2, Mod:info(input)),

                %% Kill the counter worker
                Tab = Mod:table(counters),
                Pid = whereis(Tab),
                ?assert(is_pid(Pid)),
                exit(Pid, kill),
                timer:sleep(200),  %% wait for restart

                %% Counter data should survive thanks to guardian
                NewPid = whereis(Tab),
                ?assert(is_pid(NewPid)),
                ?assertNotEqual(Pid, NewPid),
                Val = gr_counter:lookup_element(Tab, input),
                ?assertEqual(2, Val)
            end
        }
    ]}.
