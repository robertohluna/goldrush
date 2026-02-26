%% @doc Parameter storage worker for compiled query modules.
%%
%% Stores runtime parameters (function references, etc) in an ETS table.
%% During code generation, terms that cannot be represented as abstract
%% syntax (like funs) are stored here and looked up at event-handling time.
-module(gr_param).
-behaviour(gen_server).

%% API
-export([start_link/1,
         list/1, insert/2,
         lookup/2, lookup_element/2,
         info/1, info_size/1, transform/1]).

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

info_size(Server) ->
    case (catch gen_server:call(Server, info_size)) of
        {'EXIT', _Reason} ->
            info_size(gr_guardian:await_pid(Server));
        Else -> Else
    end.

insert(Server, Term) ->
    case (catch gen_server:call(Server, {insert, Term})) of
        {'EXIT', _Reason} ->
            insert(gr_guardian:await_pid(Server), Term);
        Else -> Else
    end.

lookup(Server, Term) ->
    case (catch gen_server:call(Server, {lookup, Term})) of
        {'EXIT', _Reason} ->
            lookup(gr_guardian:await_pid(Server), Term);
        Else -> Else
    end.

lookup_element(Server, Term) ->
    case (catch gen_server:call(Server, {lookup_element, Term})) of
        {'EXIT', _Reason} ->
            lookup_element(gr_guardian:await_pid(Server), Term);
        Else -> Else
    end.

info(Server) ->
    case (catch gen_server:call(Server, info)) of
        {'EXIT', _Reason} ->
            info(gr_guardian:await_pid(Server));
        Else -> Else
    end.

%% @doc Transform Term -> Key to Key -> Term
transform(Server) ->
    case (catch gen_server:call(Server, transform)) of
        {'EXIT', _Reason} ->
            transform(gr_guardian:await_pid(Server));
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

handle_call(Call, From, State) when is_atom(Call), Call =:= list;
                                     Call =:= info; Call =:= info_size;
                                     Call =:= transform ->
    TableId = State#state.table_id,
    Pending = State#state.pending,
    case TableId of
        undefined -> {noreply, State#state{pending=[{Call, From}|Pending]}};
        _ when Call =:= list      -> {reply, do_list(TableId), State};
        _ when Call =:= info      -> {reply, do_info(TableId), State};
        _ when Call =:= info_size -> {reply, do_info_size(TableId), State};
        _ when Call =:= transform -> {reply, do_transform(TableId), State}
    end;

handle_call({Call, Term}, From, State) when is_atom(Call), Call =:= insert;
                                              Call =:= lookup;
                                              Call =:= lookup_element ->
    TableId = State#state.table_id,
    Pending = State#state.pending,
    case TableId of
        undefined ->
            {noreply, State#state{pending=[{{Call, Term}, From}|Pending]}};
        _ when Call =:= insert         -> {reply, do_insert(TableId, Term), State};
        _ when Call =:= lookup         -> {reply, do_lookup(TableId, Term), State};
        _ when Call =:= lookup_element -> {reply, do_lookup_element(TableId, Term), State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unhandled_message}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'ETS-TRANSFER', TableId, _Pid, _Data}, State) ->
    _ = [gen_server:reply(From, dispatch_call(TableId, Call))
         || {Call, From} <- State#state.pending],
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
        list            -> do_list(TableId);
        info            -> do_info(TableId);
        info_size       -> do_info_size(TableId);
        transform       -> do_transform(TableId);
        {insert, Term}  -> do_insert(TableId, Term);
        {lookup, Term}  -> do_lookup(TableId, Term);
        {lookup_element, Term} -> do_lookup_element(TableId, Term)
    end.

do_list(TableId) ->
    ets:tab2list(TableId).

do_info(TableId) ->
    ets:info(TableId).

do_info_size(TableId) ->
    ets:info(TableId, size).

do_transform(TableId) ->
    ParamsList = [{K, V} || {V, K} <- ets:tab2list(TableId)],
    ets:delete_all_objects(TableId),
    ets:insert(TableId, ParamsList).

do_insert(TableId, Term) ->
    ets:insert(TableId, Term).

do_lookup(TableId, Term) ->
    ets:lookup(TableId, Term).

do_lookup_element(TableId, Term) ->
    ets:lookup_element(TableId, Term, 2).
