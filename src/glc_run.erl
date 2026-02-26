%% @doc Timed execution runtime for job processing.
%%
%% Executes a function with timing instrumentation, capturing both
%% the elapsed time and the result. Uses monotonic time on OTP 18+
%% for clock-skew-resistant measurements, falls back to os:timestamp()
%% on older releases. Errors are caught and returned as tagged tuples.
-module(glc_run).

-export([execute/2]).

-ifdef(erlang18).
-define(time_now(), erlang:monotonic_time()).
-define(time_diff(T1, T2), erlang:convert_time_unit(T2 - T1, native, micro_seconds)).
-else.
-define(time_now(), os:timestamp()).
-define(time_diff(T1, T2), timer:now_diff(T2, T1)).
-endif.

%% @doc Execute a function with timing. Returns {Microseconds, Result}.
-spec execute(fun(), [term()]) -> {non_neg_integer(), term()}.
execute(Fun, [Event, Store]) ->
    T1 = ?time_now(),
    case (catch Fun(Event, Store)) of
        {'EXIT', {Reason, _ST}} ->
            T2 = ?time_now(),
            {?time_diff(T1, T2), {error, Reason}};
        {'EXIT', Reason} ->
            T2 = ?time_now(),
            {?time_diff(T1, T2), {error, Reason}};
        Result ->
            T2 = ?time_now(),
            {?time_diff(T1, T2), Result}
    end.
