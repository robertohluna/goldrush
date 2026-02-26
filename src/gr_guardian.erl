%% @doc ETS table guardian process for crash-safe table ownership.
%%
%% Implements the OTP ETS heir pattern. The guardian creates an ETS table,
%% populates it with initial data, sets itself as heir, and gives ownership
%% to the designated worker process. If the worker crashes, the table is
%% automatically transferred back to the guardian via ETS-TRANSFER. The
%% guardian then waits for the supervisor to restart the worker and hands
%% the table back, preserving all data across crashes.
-module(gr_guardian).
-behaviour(gen_server).

%% API
-export([start_link/3, await_pid/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    table_id :: ets:tab() | undefined,
    ward     :: atom()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Provision initial ETS data for the ward process.
-spec provision(atom() | pid(), term()) -> ok.
provision(Name, Data) ->
    gen_server:cast(Name, {provision, Data}).

%% @doc Start a guardian linked to a ward (the process that owns the table).
-spec start_link(atom(), atom(), term()) -> {ok, pid()} | {error, term()}.
start_link(Name, Ward, Data) ->
    gen_server:start_link({local, Name}, ?MODULE, [Ward, Data], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Ward, Data]) ->
    process_flag(trap_exit, true),
    provision(self(), Data),
    {ok, #state{ward=Ward}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unhandled_message}, State}.

handle_cast({provision, Data}, #state{ward=Ward}=State) ->
    WardPid = whereis(Ward),
    link(WardPid),
    TableId = ets:new(?MODULE, [set, private]),
    ets:insert(TableId, Data),
    ets:setopts(TableId, {heir, self(), Data}),
    ets:give_away(TableId, WardPid, Data),
    {noreply, State#state{table_id=TableId}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _Pid, _Reason}, State) ->
    %% Ward died; table will arrive via ETS-TRANSFER
    {noreply, State};
handle_info({'ETS-TRANSFER', TableId, _Pid, Data}, #state{ward=Ward}=State) ->
    WardPid = await_pid(Ward),
    link(WardPid),
    ets:give_away(TableId, WardPid, Data),
    {noreply, State#state{table_id=TableId}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Wait for a registered process to become available.
%% Used during crash recovery to wait for the supervisor to restart
%% the ward process before handing back the ETS table.
-spec await_pid(atom()) -> pid().
await_pid(Name) when is_pid(Name) ->
    Name;
await_pid(Name) when is_atom(Name), Name =/= undefined ->
    case whereis(Name) of
        undefined ->
            timer:sleep(1),
            await_pid(Name);
        Pid -> Pid
    end.
