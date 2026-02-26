%% @doc Dynamic supervisor for counter worker processes.
%%
%% Each compiled query module gets its own counter worker process
%% registered under this supervisor. Workers are started dynamically
%% when a query is compiled and removed when the query is deleted.
-module(gr_counter_sup).
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
