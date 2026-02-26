%% @doc Dynamic supervisor for ETS table guardian processes.
%%
%% Guardians serve as the heir for ETS tables owned by counter and
%% param workers. If a worker dies, the guardian receives the table
%% via ETS-TRANSFER and hands it back once the worker is restarted.
-module(gr_guardian_sup).
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
