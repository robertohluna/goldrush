# Goldrush

**Event stream processing for Erlang/OTP**

Goldrush is a runtime event filtering and processing library that compiles query specifications into optimized Erlang modules. Queries are composed using a small set of filter operators, compiled once, and then applied to streams of events with zero interpretation overhead.

## Features

- **Runtime code generation** — Query trees are compiled into native BEAM modules via `erl_syntax` + `compile:forms/2`, eliminating per-event interpretation cost
- **Composable operators** — `eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `wc` (exists), `nf` (not found) with `all`/`any` combinators
- **Crash-safe ETS** — Counter and parameter tables survive worker crashes via an ETS heir/guardian pattern with supervised process trees
- **Statistics tracking** — Per-query counters for input, filter, output, job runs, errors, and timing with optional compile-time elimination
- **Timed job execution** — Run instrumented functions with automatic timing capture and result routing through query filters
- **Atomic snapshots** — Read all counters in a single consistent operation instead of multiple individual lookups
- **Batch event handling** — Process lists of events in one call to reduce per-call overhead
- **Query explainer** — Human-readable pretty-printing of optimized query trees for debugging and introspection
- **Compile-time value store** — Embed key-value pairs into generated modules, accessible at runtime via `glc:get/2`

## Quick Start

```erlang
%% Start the application
ok = application:start(syntax_tools),
ok = application:start(compiler),
ok = application:start(goldrush).

%% Compile a query that matches events where level = error
{ok, Mod} = glc:compile(my_query, glc:eq(level, error)).

%% Handle events
glc:handle(Mod, [{level, error}, {msg, <<"disk full">>}]).
glc:handle(Mod, [{level, info}, {msg, <<"all good">>}]).

%% Check stats
glc:input(Mod).   %% => 2
glc:output(Mod).  %% => 1
glc:filter(Mod).  %% => 1

%% Clean up
glc:delete(Mod).
```

## Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `glc:eq(K, V)` | Equals | `glc:eq(level, error)` |
| `glc:neq(K, V)` | Not equals | `glc:neq(env, test)` |
| `glc:gt(K, V)` | Greater than | `glc:gt(count, 100)` |
| `glc:gte(K, V)` | Greater or equal | `glc:gte(priority, 3)` |
| `glc:lt(K, V)` | Less than | `glc:lt(latency, 50)` |
| `glc:lte(K, V)` | Less or equal | `glc:lte(retries, 3)` |
| `glc:wc(K)` | Key exists (wildcard) | `glc:wc(trace_id)` |
| `glc:nf(K)` | Key not found | `glc:nf(debug)` |
| `glc:null(Bool)` | Constant pass/reject | `glc:null(true)` |

## Combinators

```erlang
%% All conditions must match
glc:all([glc:eq(env, prod), glc:gt(severity, 3)]).

%% Any condition matches
glc:any([glc:eq(level, error), glc:eq(level, critical)]).

%% Attach an action to matching events
glc:with(glc:eq(level, error), fun(Event) ->
    error_logger:error_report(gre:pairs(Event))
end).

%% Multiple queries as a union
glc:union([Query1, Query2]).
```

## Batch Processing

```erlang
%% Handle multiple events in one call
Events = [
    [{level, error}, {msg, <<"timeout">>}],
    [{level, info}, {msg, <<"started">>}],
    gre:make([{level, warn}], [list])
],
glc:handle_many(Mod, Events).
```

## Atomic Snapshots

```erlang
%% Get all counters as a consistent map in one call
Snap = glc:snapshot(Mod),
%% => #{input => 42, filter => 10, output => 32,
%%      job_input => 0, job_run => 0, job_time => 0, job_error => 0}
```

## Query Explain

```erlang
%% See the optimized query tree
Q = glc:all([glc:eq(a, 1), glc:gt(b, 5)]),
{ok, Mod} = glc:compile(my_q, Q),
io:format("~s~n", [glc:explain(Mod)]).
%% => all(a==1, b>5)
```

## Timed Jobs

```erlang
%% Execute a function with timing instrumentation
{ok, Mod} = glc:compile(timed_q,
    glc:with(glc:gte(runtime, 0.0),
        fun(E) -> io:format("Took ~p sec~n", [gre:fetch(runtime, E)]) end)),

glc:run(Mod, fun(Event, _Store) ->
    expensive_operation(gre:fetch(data, Event))
end, gre:make([{data, payload}], [list])).
```

## Compile-Time Store

```erlang
%% Embed values at compile time, read at runtime
Store = [{api_key, <<"abc123">>}, {statistics, true}],
{ok, Mod} = glc:compile(my_q, glc:null(true), Store),
{ok, <<"abc123">>} = glc:get(Mod, api_key).

%% Access store from within callback
glc:with(glc:null(true), fun(Event, Store) ->
    ApiKey = proplists:get_value(api_key, Store),
    send_event(ApiKey, Event)
end).
```

## Architecture

```
goldrush (application)
├── gr_sup (top supervisor, one_for_one)
│   ├── gr_counter_sup (dynamic supervisor for counter workers)
│   ├── gr_param_sup (dynamic supervisor for param workers)
│   └── gr_guardian_sup (dynamic supervisor for ETS guardians)
│
├── Per-query processes:
│   ├── gr_counter — gen_server owning counter ETS table
│   ├── gr_param — gen_server owning parameter ETS table
│   └── gr_guardian — ETS heir process for crash recovery
│
├── glc — Public API (compile, handle, snapshot, explain, etc.)
├── glc_code — Runtime code generation via erl_syntax
├── glc_lib — Query reduction, optimization, pretty-printing
├── glc_ops — Operator constructors and type definitions
├── glc_run — Timed job execution with OTP 18+ monotonic time
├── gre — Event creation and field access
└── gr_context — Runtime context (node, app, pid, timestamp)
```

## Building

```bash
rebar3 compile
```

## Testing

```bash
rebar3 eunit
```

Runs 116 tests covering operators, composite queries, counter/param workers, guardian crash recovery, snapshots, batch handling, pretty-printing, and query explanation.

## Requirements

- Erlang/OTP 18 or later
- rebar3

## License

ISC License — see source file headers for details.
