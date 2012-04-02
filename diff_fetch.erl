-module(diff_fetch, [Connection]).
-include_lib("stdlib/include/qlc.hrl").
-behaviour(gen_server).

-export([get_count/0, get_cursor/0, downloaded/0]).
-export([fetch/0, fetch/1, back/0]).

%% gen_server callbacks
-export([id/0, start_link/0, stop/0, init/1, handle_call/3]).
-export([handle_cast/2, handle_info/2, terminate/2, code_change/3]).

id() -> list_to_atom(lists:concat(["diff_fetch_", atom_to_list(Connection)])).

%% @doc start_link/0 doc
start_link() ->
    gen_server:start_link({local, id()}, ?MODULE:new(Connection), [], []).

% stopping the server
stop() ->
	case whereis(id()) of
		undefined -> {error, "not running... use start()"};
		_ -> gen_server:cast(id(), stop)
	end.

get_count() ->
	case whereis(id()) of
		undefined -> {error, "not running..."};
		_ -> gen_server:call(id(), {count})
	end.

get_cursor() ->
	case whereis(id()) of
		undefined -> {error, "not running..."};
		_ -> gen_server:call(id(), {cursor})
	end.

%% default batch size is 1024
fetch() -> fetch(1024).

fetch(Number) when is_integer(Number) ->
	case whereis(id()) of
		undefined -> {error, "not running..."};
		_ -> gen_server:call(id(), {fetch, Number}, infinity)
	end.

back() ->
	case whereis(id()) of
		undefined -> {error, "not running..."};
		_ -> gen_server:call(id(), {back})
	end.

downloaded() ->
	case whereis(id()) of
		undefined -> {error, "not running..."};
		_ -> gen_server:call(id(), {downloaded})
	end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------

init([]) ->
	% some initialization routines here for State
	TS = list_to_atom(lists:concat(["tuplestore_", atom_to_list(id())])),
	TStore = ets:new(TS,[public]),
	S = list_to_atom(lists:concat(["state_", atom_to_list(id())])),
	State = ets:new(S,[public]),
	ets:insert(State, {tuplestore, {TStore}}),
	ets:insert(State, {downloaded, {0}}),
	ets:insert(State, {cursor, {0}}),
    {ok, State}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({count}, _From, State) ->
   	Reply = case get_term(State, tuplestore) of
		[] -> error;
		[{TStore}] ->
   			case ets:info(TStore, size) of
				undefined -> error;
				C -> C
			end;
		_Other -> error
	end,
	{reply, Reply, State};

handle_call({cursor}, _From, State) ->
   	Reply = case get_term(State, cursor) of
		[] -> error;
		[{Cursor}] -> Cursor;
		_Other -> error
	end,
	{reply, Reply, State};

handle_call({back}, _From, State) ->
   	[{Cursor}] = get_term(State, cursor),
	Idx = case Cursor of
		0 -> 0;
		C -> C - 1
	end,
	ets:insert(State, {cursor, {Idx}}),
	{reply, ok, State};

%% downloaded (tuples) stats
handle_call({downloaded}, _From, State) ->
   	[{Downloaded}] = get_term(State, downloaded),
	{reply, Downloaded, State};

handle_call({fetch, Number}, _From, State) when is_integer(Number) ->
   	[{TStore}] = get_term(State, tuplestore),
   	[{Cursor}] = get_term(State, cursor),
   	[{Downloaded}] = get_term(State, downloaded),
   	Count = ets:info(TStore, size),
	Reply = case Cursor < Count of
		true -> % return tuple at position of cursor and advance cursor
   			[{TStore}] = get_term(State, tuplestore),
			ets:insert(State, {cursor, {Cursor + 1}}),
			fetch_at(TStore, Cursor + 1);
		false -> % empty tuplestore and fetch next batch
   			[{TStore}] = get_term(State, tuplestore),
			ets:delete_all_objects(TStore),
			Sql = io_lib:format("FETCH FORWARD ~p FROM mycursor",[Number]),
   			List = case epg:exec(Connection, Sql) of
				{error, R1} -> io:format("epg_error: ~s~n", [R1]), throw(epg_error);
				[{header, _H1},{data,[]}] -> [];
				[{header, _H1},{data,T1}] -> T1
			end,
			populate(TStore, 0, List),
			ets:insert(State, {downloaded, {Downloaded + length(List)}}),
   			NewCount = ets:info(TStore, size),
			case NewCount > 0 of
				true ->
					ets:insert(State, {cursor, {1}}),
					fetch_at(TStore, 1);
				false ->
					[]
			end
	end,
	{reply, Reply, State};

handle_call(_Request, _From, State) ->
	{reply, unexpected_request, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
	{stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
   	case get_term(State, tuplestore) of
		[{TStore}] -> ets:delete(TStore);
		_Other -> ok
	end,
	ets:delete(State),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	ets:insert(State, {changed, true}),
    {ok, State}.

%% private functions -------------------------------------------------

fetch_at(TStore, Idx) ->
	case ets:lookup(TStore, Idx) of
		[] -> [];
		[{Idx, Tuple}] -> Tuple
	end.

populate(_TStore, Num, []) -> Num;
populate(TStore, Num, [H|T]) ->
	NewNum = Num + 1,
	ets:insert(TStore, {NewNum, H}),
	populate(TStore, NewNum, T).

% ETS lookup returning term
get_term(Table,Key) ->
	Q = qlc:q([Val || {Id, Val} <- ets:table(Table), Id == Key]),
	qlc:e(Q).

