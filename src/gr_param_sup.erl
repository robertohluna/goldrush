%% @doc Dynamic supervisor for parameter storage worker processes.
%%
%% Each compiled query module gets its own param worker process
%% that holds the ETS table for runtime parameter lookups.
-module(gr_param_sup).
-behaviour(supervisor).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, { {one_for_one, 50, 10}, [supervisor:child_spec()]} }.
init(_Args) ->
    {ok, { {one_for_one, 50, 10}, []} }.
