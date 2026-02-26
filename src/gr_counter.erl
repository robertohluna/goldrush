%% @doc Counter worker process for tracking event statistics.
%%
%% Each compiled query module gets a counter worker that owns an ETS table
%% holding event counts (input, filter, output, job_input, etc). The table
%% is protected by a guardian process for crash recovery.
%%
%% Counter updates use gen_server:cast for fire-and-forget performance.
%% Lookups and resets use gen_server:call for synchronous consistency.
-module(gr_counter).
-behaviour(gen_server).

%% API
-export([start_link/1,
         list/1, lookup_element/2,
         insert_counter/3,
         update_counter/3, reset_counters/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {table_id :: ets:tab() | undefined, pending = [] :: list()}).

%%%===================================================================
%%% API
%%%===================================================================

list(Server) ->
    case (catch gen_server:call(Server, list)) of
        {'EXIT', _Reason} ->
            list(gr_guardian:await_pid(Server));
        Else -> Else
    end.

lookup_element(Server, Term) ->
    case (catch gen_server:call(Server, {lookup_element, Term})) of
        {'EXIT', _Reason} ->
            lookup_element(gr_guardian:await_pid(Server), Term);
        Else -> Else
    end.

insert_counter(Server, Counter, Value) when is_atom(Server) ->
    case whereis(Server) of
        undefined ->
            insert_counter(gr_guardian:await_pid(Server), Counter, Value);
        Pid ->
            case erlang:is_process_alive(Pid) of
                true  -> insert_counter(Pid, Counter, Value);
                false -> insert_counter(gr_guardian:await_pid(Server), Counter, Value)
            end
    end;
insert_counter(Server, Counter, Value) when is_pid(Server) ->
    case (catch gen_server:call(Server, {insert_counter, Counter, Value})) of
        {'EXIT', _Reason} ->
            insert_counter(gr_guardian:await_pid(Server), Counter, Value);
        Else -> Else
    end.

update_counter(Server, Counter, Value) when is_atom(Server) ->
    case whereis(Server) of
        undefined ->
            update_counter(gr_guardian:await_pid(Server), Counter, Value);
        Pid ->
            case erlang:is_process_alive(Pid) of
                true  -> update_counter(Pid, Counter, Value);
                false -> update_counter(gr_guardian:await_pid(Server), Counter, Value)
            end
    end;
update_counter(Server, Counter, Value) when is_pid(Server) ->
    gen_server:cast(Server, {update, Counter, Value}).

reset_counters(Server, Counter) ->
    case (catch gen_server:call(Server, {reset_counters, Counter})) of
        {'EXIT', _Reason} ->
            reset_counters(gr_guardian:await_pid(Server), Counter);
        Else -> Else
    end.

%%--------------------------------------------------------------------
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(list=Call, From, State) ->
    TableId = State#state.table_id,
    Pending = State#state.pending,
    case TableId of
        undefined -> {noreply, State#state{pending=[{Call, From}|Pending]}};
        _ -> {reply, lists:sort(do_list(TableId)), State}
    end;
handle_call({lookup_element, Term}=Call, From, State) ->
    TableId = State#state.table_id,
    Pending = State#state.pending,
    case TableId of
        undefined -> {noreply, State#state{pending=[{Call, From}|Pending]}};
        _ -> {reply, do_lookup_element(TableId, Term), State}
    end;
handle_call({insert_counter, Counter, Value}, From, State) ->
    Term = [{Counter, Value}],
    Call = {insert, Term},
    TableId = State#state.table_id,
    Pending = State#state.pending,
    case TableId of
        undefined -> {noreply, State#state{pending=[{Call, From}|Pending]}};
        _ -> {reply, do_insert(TableId, Term), State}
    end;
handle_call({reset_counters, Counter}, From, State) ->
    Term = case Counter of
        _ when is_list(Counter) ->
            [{Item, 0} || Item <- Counter];
        _ when is_atom(Counter) ->
            [{Counter, 0}]
    end,
    Call = {insert, Term},
    TableId = State#state.table_id,
    Pending = State#state.pending,
    case TableId of
        undefined -> {noreply, State#state{pending=[{Call, From}|Pending]}};
        _ -> {reply, do_insert(TableId, Term), State}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unhandled_message}, State}.

handle_cast({update, Counter, Value}=Call, State) ->
    TableId = State#state.table_id,
    Pending = State#state.pending,
    State2 = case TableId of
        undefined -> State#state{pending=[Call|Pending]};
        _ -> _ = do_update_counter(TableId, Counter, Value),
             State
    end,
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'ETS-TRANSFER', TableId, _Pid, _Data}, State) ->
    %% Replay all pending operations from before table was available
    _ = [gen_server:reply(From, dispatch_call(TableId, Call))
         || {Call, From} <- State#state.pending],
    _ = [do_update_counter(TableId, Counter, Value)
         || {update, Counter, Value} <- State#state.pending],
    {noreply, State#state{table_id=TableId, pending=[]}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

dispatch_call(TableId, Call) ->
    case Call of
        list                  -> do_list(TableId);
        {insert, Term}        -> do_insert(TableId, Term);
        {lookup_element, Term} -> do_lookup_element(TableId, Term)
    end.

do_list(TableId) ->
    ets:tab2list(TableId).

do_update_counter(TableId, Counter, Value) ->
    ets:update_counter(TableId, Counter, Value).

do_insert(TableId, Term) ->
    ets:insert(TableId, Term).

do_lookup_element(TableId, Term) ->
    ets:lookup_element(TableId, Term, 2).
