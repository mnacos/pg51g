-module(diff,[ReqId]).
%% public API
-export([tables/4, tables/5, tables/6]).
%% needed for parallel sigtbl generation
-export([prepare/5]).
%% needed by foreachlevel func
-export([get_all_pris_from_conn1/8, get_pri_if_different/8, get_op_if_different/8]).
%% needed for fetching batches
-export([fetch_one/1, back_one/1]).
-export([diff_fetch_one/1, diff_back_one/1]).
%% generating output
-export([notify/1]).
%% will be removed
-export([main/0]).

main() ->
	epg:start(db1), epg:start(db2),
	receive after 1000 -> ok end,
	epg:exec(db1, "user=user password=pass host=localhost port=5432 dbname=db1"),
	epg:exec(db2, "user=user password=pass host=localhost port=5432 dbname=db2"),
	tables("WHERE id BETWEEN 10000 AND 20000",db1,db2,"schema.table").

notify(Line) when is_list(Line) ->
	case whereis(diff_http) of
		undefined -> io:format("~s",[Line]);
		_ -> diff_http:write(ReqId, Line)
	end.
	
%% same table on both databases
tables(Restriction,Conn1,Conn2,Tbl) when is_atom(Conn1), is_atom(Conn2) ->
	tables(Restriction, Conn1, Conn2, Tbl, Tbl).

%% use default number of folding levels (4)
tables(Restriction,Conn1,Conn2,Tbl1,Tbl2) when is_atom(Conn1), is_atom(Conn2) ->
	tables(Restriction, Conn1, Conn2, Tbl1, Tbl2, 4).

%% general version of diff:tables function
tables(Restriction, Conn1, Conn2, Tbl1, Tbl2, Levels) when is_atom(Conn1) and is_atom(Conn2) ->
	process_flag(trap_exit, true),
	ParentPid = self(),
	Pid1 = spawn_link(fun() -> prepare(ParentPid, Restriction, Conn1, Tbl1, Levels) end),
	Pid2 = spawn_link(fun() -> prepare(ParentPid, Restriction, Conn2, Tbl2, Levels) end),
	notify(io_lib:format("Preparing signature tables on ~p and ~p...~n",[Conn1, Conn2])),
	receive_loop(Pid1, Pid2),
	try 
		lists:flatten([dodiff(Conn1, Conn2, Levels)])
	catch
		throw:no_pris -> []
	end.

%% convenience function introduced because we're trapping exits
receive_loop([], []) -> ok;
receive_loop(Pid1, Pid2) ->
	receive
		{Pid1, TmpTbl1} -> notify(io_lib:format("Table ~s is ready~n", [TmpTbl1])), receive_loop(Pid1, Pid2);
		{Pid2, TmpTbl2} -> notify(io_lib:format("Table ~s is ready~n", [TmpTbl2])), receive_loop(Pid1, Pid2);
		{'EXIT', MyPid, normal} ->
			case MyPid of
				Pid1 -> receive_loop([], Pid2);
				Pid2 -> receive_loop(Pid1, [])
			end;
		{'EXIT', MyPid, _R} ->
			case MyPid of
				Pid1 -> notify(io_lib:format("Error from Pid1~n", [])), throw(prepare_exit);
				Pid2 -> notify(io_lib:format("Error from Pid2~n", [])), throw(prepare_exit)
			end
	end.
	
%% generating a temp signature table using pg51g.tmp(...)
prepare(ParentPid, Restriction, Conn, Tbl, Levels) when is_pid(ParentPid), is_atom(Conn) ->
	SLevel = io_lib:format("~p",[Levels]),
	Sql = lists:concat([
			"SELECT pg51g.tmp('", Tbl, "','tmp_", atom_to_list(Conn), "','", Restriction, "',", SLevel, ");"
	]),
	case epg:exec(Conn, Sql) of
		[{header, _H},{data,[{TmpTbl}]}] ->
			case epg:exec(Conn, lists:concat(["ANALYZE ", "tmp_", atom_to_list(Conn), ";"])) of
				{error, AReason} -> notify(io_lib:format("epg_error: ~s~n", [AReason])), throw(epg_error);
				_AnythingElse -> ParentPid!{self(), TmpTbl}
			end;
		{error, Reason} -> notify(io_lib:format("epg_error: ~s~n", [Reason])), throw(epg_error);
		Other -> notify(io_lib:format("~p got: ~p~n", [Conn, Other]))
	end.

%% data comparison
dodiff(Conn1, Conn2, Levels) ->
	%% integer to string
	SLevel = io_lib:format("~p",[Levels]),
	%% tmp table names
	TmpTbl1 = "tmp_"++atom_to_list(Conn1), TmpTbl2 = "tmp_"++atom_to_list(Conn2),
	%% toplevel diff
	Sql1 = lists:concat(["SELECT level, pri, key, val, grp FROM ", TmpTbl1, " WHERE level = '", SLevel, "';"]),
	Sql2 = lists:concat(["SELECT level, pri, key, val, grp FROM ", TmpTbl2, " WHERE level = '", SLevel, "';"]),
	case epg:exec(Conn1, Sql1) of
		{error, R1} -> notify(io_lib:format("epg_error: ~s~n", [R1])), throw(epg_error);
		[{header, _H1},{data,[{_L, P1, K1, V1, _G1}]}] ->
			case epg:exec(Conn2, Sql2) of
				{error, R2} -> notify(io_lib:format("epg_error: ~s~n", [R2])), throw(epg_error);
				[{header, _H2},{data,[{_L, P2, K2, V2, _G2}]}] ->
					case {P1, K1, V1} == {P2, K2, V2} of
						true -> [];
						false ->
							%% the 'pri' value is at the highest level is always zeroes
							foreachlevel(Levels, Conn1, Conn2, Levels-1, [P1])
					end;
				[{header, _H2},{data,[]}] -> notify(io_lib:format("toplevel Sql2: no_rows~n", [])), throw(epg_error)
			end;
		[{header, _H1},{data,[]}] -> notify(io_lib:format("toplevel Sql1: no_rows~n", [])), throw(epg_error)
	end.

%% processing each level
foreachlevel(_Levels, _Conn1, _Conn2, Level, []) ->
	notify(io_lib:format("no_pris -- Level: ~p~n", [Level])), throw(no_pris);
foreachlevel(Levels, Conn1, Conn2, Level, Pris) when Level == 0 ->
	%% integer to string
	SLevels = io_lib:format("~p",[Levels]),
	%% getting mask for previous level
	SLevelUp = io_lib:format("~p",[Level+1]),
	MSql = lists:concat(["SELECT pg51g.mask4level('", SLevels, "','", SLevelUp, "');"]),
	Mask = case epg:exec(Conn1, MSql) of
		[{header, _H},{data,[{MyMask}]}] -> MyMask;
		{error, Reason} -> notify(io_lib:format("epg_error: ~s~n", [Reason])), throw(epg_error);
		Other -> notify(io_lib:format("~p got: ~p~n", [Conn1, Other])), throw(epg_error)
	end,
	notify(io_lib:format("...scanning Level ~p rows, Mask: ~s, previous Level's number of differences: ~p~n",[0, Mask, length(Pris)])),
	%% what's different here?
	Ops = foreachpri(Level, Mask, Conn1, Conn2, fun get_op_if_different/8, Pris, []),
	notify(io_lib:format("Number of differences: ~p~n",[length(Ops)])),
	Ops; 
foreachlevel(Levels, Conn1, Conn2, Level, Pris) ->
	%% integer to string
	SLevels = io_lib:format("~p",[Levels]),
	%% getting mask for previous level
	SLevelUp = io_lib:format("~p",[Level+1]),
	MSql = lists:concat(["SELECT pg51g.mask4level('", SLevels, "','", SLevelUp, "');"]),
	Mask = case epg:exec(Conn1, MSql) of
		[{header, _H},{data,[{MyMask}]}] -> MyMask;
		{error, Reason} -> notify(io_lib:format("epg_error: ~s~n", [Reason])), throw(epg_error);
		Other -> notify(io_lib:format("~p got: ~p~n", [Conn1, Other])), throw(epg_error)
	end,
	notify(io_lib:format("...scanning Level ~p rows, Mask: ~s, previous Level's number of differences: ~p~n",[Level, Mask, length(Pris)])),
	%% process each pri value
	NewPris = foreachpri(Level, Mask, Conn1, Conn2, fun get_pri_if_different/8, Pris, []),
	%% descend to next level
	foreachlevel(Levels, Conn1, Conn2, Level-1, NewPris).

%% processing each pri
foreachpri(_Level, _Mask, _Conn1, _Conn2, _Diff, [], NewPris) -> NewPris;
foreachpri(Level, Mask, Conn1, Conn2, Diff, [H|T], Pris) ->
	TmpTbl1 = "tmp_"++atom_to_list(Conn1), TmpTbl2 = "tmp_"++atom_to_list(Conn2),
	SLevel = io_lib:format("~p",[Level]),
	Sql1 = lists:concat(["SELECT * FROM ", TmpTbl1, " WHERE level = '", SLevel, "' AND grp = '", H, "' ORDER BY pri, key, val;"]),
	Sql2 = lists:concat(["SELECT * FROM ", TmpTbl2, " WHERE level = '", SLevel, "' AND grp = '", H, "' ORDER BY pri, key, val;"]),
	NewPris = mergejoin_diff(Conn1, Sql1, Conn2, Sql2, SLevel, TmpTbl1, TmpTbl2, Diff),
	%% debugging
	% notify(io_lib:format("Conn1: ~p Sql:~s~n",[Conn1, Sql1])),
	% notify(io_lib:format("Conn2: ~p Sql:~s~n",[Conn2, Sql2])),
	%% using lists:append/1 to make avoid deeper levels per pri
	foreachpri(Level, Mask, Conn1, Conn2, Diff, T, lists:append([Pris,NewPris])).

%% the actual comparison (per level, per pri); returns list of diff records
mergejoin_diff(Conn1, Sql1, Conn2, Sql2, SLevel, TmpTbl1, TmpTbl2, Diff) ->
	%% begin transaction and declare cursor
	epg_start_diff(Conn1, Sql1),
	epg_start_diff(Conn2, Sql2),
	%% init diff_fetch batching servers for each connection
	C1 = diff_fetch:new(Conn1), C2 = diff_fetch:new(Conn2),
	C1:start_link(), C2:start_link(),
	%% define Fetch() and Back() callbacks
	Fetch = fun diff_fetch_one/1, % Fetch = fun fetch_one/1,
	Back = fun diff_back_one/1, % Back = fun back_one/1,
	%% diff rows one by one by calling Diff()
	Pris = Diff(Conn1,Conn2,Fetch,Back,SLevel,TmpTbl1,TmpTbl2,[]),
	%% terminate diff_fetch batching servers
	C1:stop(), C2:stop(),
	%% cleanup transaction
	epg_stop_diff(Conn1),
	epg_stop_diff(Conn2),
	Pris.

%% using this Diff function returns all rows from Conn1 -- no diff-ing involved
get_all_pris_from_conn1(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,Pris) ->
	T1 = Fetch(Conn1),
	case T1 of
		[] -> lists:reverse(Pris);
		{_L1, P1, _K1, _V1, _G1} -> get_all_pris_from_conn1(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[P1|Pris]);
		R1 -> notify(io_lib:format("epg_error: ~s~n", [R1])), throw(epg_error)
	end.

%% using this Diff function returns only the pris of rows which are different
get_pri_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,Pris) ->
	%% fetch tuple from Conn1
	MyT1 = Fetch(Conn1),
	%% fetch tuple from Conn2
	MyT2 = Fetch(Conn2),
	%% compare the two
	case MyT1 of
		{L1, P1, K1, V1, G1} ->
			case MyT2 of
				%% if the tuples are identical, move on to the next pair
				{L1, P1, K1, V1, G1} -> get_pri_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,Pris);
				%% if the tuples share pri but differ, append pri and move on to next pair
				{L1, P1, K1,_V2,_G2} -> get_pri_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[[[P1]]|Pris]);
				%% if the tuples do not even have same pri, look for both pris in both sets
				{L1, P2, K2, V2, G2} ->
					Sql2 = lists:concat(["SELECT * FROM ",TmpTbl2," WHERE level = '",Level,"' AND pri = '",P1,"';"]),
					Sql1 = lists:concat(["SELECT * FROM ",TmpTbl1," WHERE level = '",Level,"' AND pri = '",P2,"';"]),
					TmpPris = case epg:exec(Conn2, Sql2) of
						{error, R3} -> notify(io_lib:format("epg_error: ~s~n", [R3])), throw(epg_error);
						%% if P1 does not exist in Conn2, append it to list of TmpPris after looking at P2
						[{header, _H3},{data,[]}] ->
							%% does P2 exist in Conn1?
							case epg:exec(Conn1, Sql1) of
								{error, R5} -> notify(io_lib:format("epg_error: ~s~n", [R5])), throw(epg_error);
								%% if P2 exists in Conn2 but not in Conn1, append it to the list of TmpPris
								[{header, _H4},{data,[]}] -> [[P1],[P2]];
								%% if P2 exists in Conn1, backtrack Conn2 so that it's fetched again, return P1
								[{header, _H6},{data,_Rows6}] ->
									ok = Back(Conn2),
									[[P1]]
							end;
						%% if P1 exists in Conn2, backtrack Conn1 after looking at P2
						[{header, _H3},{data,[Rows2]}] ->
							case epg:exec(Conn1, Sql1) of
								{error, R5} -> notify(io_lib:format("epg_error: ~s~n", [R5])), throw(epg_error);
								%% if P2 does not exist in Conn1, backtrack Conn1 and append P2
								[{header, _H4},{data,[]}] ->
									ok = Back(Conn1),
									[[P2]];
								%% we should never have reached this point -- paradox!
								[{header, _H8},{data,[Rows1]}] ->
									notify(io_lib:format("[get_pri_if_different/8] paradox: ~p vs. ~p~nRows2: ~p~n~p: ~s~n~p: ~s~n",
													[{L1,P2,K2,V2,G2}, Rows1, Rows2, Conn1, Sql1, Conn2, Sql2])),
									throw(paradox)
							end
					end,
					get_pri_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[TmpPris|Pris]);
				%% if we have run out of tuples from Conn2, append P1 and carry on
				[] -> get_pri_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[[[P1]]|Pris])
			end;
		[] ->
			case MyT2 of
				%% we have run out of tuples from Conn1, append P2 and carry on
				{_L2,P2,_K2,_V2,_G2} -> get_pri_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[[[P2]]|Pris]);
				%% we have finished comparing, return the results
				[] -> lists:reverse(Pris)
			end
	end.

%% using this Diff function returns the full op for rows which are different
get_op_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,Ops) ->
	%% fetch tuple from Conn1
	MyT1 = Fetch(Conn1),
	%% fetch tuple from Conn2
	MyT2 = Fetch(Conn2),
	%% compare the two
	case MyT1 of
		{L1, P1, K1, V1, G1} ->
			case MyT2 of
				%% if the tuples are identical, move on to the next pair
				{L1, P1, K1, V1, G1} -> get_op_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,Ops);
				%% if the tuples share pri but differ, append pri and move on to next pair
				{L1, P1, K1,_V2,_G2} -> get_op_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[[{update,P1}]|Ops]);
				%% if the tuples do not even have same pri, look for both pris in both sets
				{L1, P2, K2, V2, G2} ->
					Sql2 = lists:concat(["SELECT * FROM ",TmpTbl2," WHERE level = '",Level,"' AND pri = '",P1,"';"]),
					Sql1 = lists:concat(["SELECT * FROM ",TmpTbl1," WHERE level = '",Level,"' AND pri = '",P2,"';"]),
					TmpOps = case epg:exec(Conn2, Sql2) of
						{error, R3} -> notify(io_lib:format("epg_error: ~s~n", [R3])), throw(epg_error);
						%% if P1 does not exist in Conn2, append it to list of TmpOps after looking at P2
						[{header, _H3},{data,[]}] ->
							%% does P2 exist in Conn1?
							case epg:exec(Conn1, Sql1) of
								{error, R5} -> notify(io_lib:format("epg_error: ~s~n", [R5])), throw(epg_error);
								%% if P2 exists in Conn2 but not in Conn1, append it to the list of TmpOps
								[{header, _H4},{data,[]}] -> [{delete,P1},{insert,P2}];
								%% if P2 exists in Conn1, backtrack Conn2 so that it's fetched again, return P1
								[{header, _H6},{data,_Rows6}] ->
									ok = Back(Conn2),
									[{delete,P1}]
							end;
						%% if P1 exists in Conn2, backtrack Conn1 after looking at P2
						[{header, _H3},{data,[Rows2]}] ->
							case epg:exec(Conn1, Sql1) of
								{error, R5} -> notify(io_lib:format("epg_error: ~s~n", [R5])), throw(epg_error);
								%% if P2 does not exist in Conn1, backtrack Conn1 and append P2
								[{header, _H4},{data,[]}] ->
									ok = Back(Conn1),
									[{insert,P2}];
								%% we should never have reached this point -- paradox!
								[{header, _H8},{data,[Rows1]}] ->
									notify(io_lib:format("[get_op_if_different/8] paradox: ~p vs. ~p~nRows2: ~p~n",[{L1,P2,K2,V2,G2}, Rows1, Rows2])),
									throw(paradox)
							end
					end,
					get_op_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[TmpOps|Ops]);
				%% if we have run out of tuples from Conn2, append P1 and carry on
				[] -> get_op_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[[{delete,P1}]|Ops])
			end;
		[] ->
			case MyT2 of
				%% we have run out of tuples from Conn1, append P2 and carry on
				{_L2,P2,_K2,_V2,_G2} -> get_op_if_different(Conn1,Conn2,Fetch,Back,Level,TmpTbl1,TmpTbl2,[[{insert,P2}]|Ops]);
				%% we have finished comparing, return the results
				[] -> lists:reverse(Ops)
			end
	end.

%% helper functions
epg_start_diff(Conn, Sql) ->
	case epg:exec(Conn, "START TRANSACTION;") of
		{error, R1} -> notify(io_lib:format("epg_error: ~s~n", [R1])), throw(epg_error);
		_O1 -> ok
	end,
	case epg:exec(Conn, "DECLARE mycursor NO SCROLL CURSOR FOR "++Sql) of
		{error, R2} -> notify(io_lib:format("epg_error: ~s~n", [R2])), throw(epg_error);
		_O2 -> ok
	end.

epg_stop_diff(Conn) ->
	case epg:exec(Conn, "CLOSE mycursor;") of
		{error, R} -> notify(io_lib:format("epg_error: ~s~n", [R])), throw(epg_error);
		_O -> ok
	end,
	case epg:exec(Conn, "COMMIT;") of
		{error, Reason} -> notify(io_lib:format("epg_error: ~s~n", [Reason])), throw(epg_error);
		_Other -> ok
	end.

%% fetches next tuple from a connection --------------------------------------------------
%% returns tuple or empty list or throws an epg_error
fetch_one(Conn) ->
	Sql = "FETCH NEXT FROM mycursor",
	%% fetch tuple from Conn1
	case epg:exec(Conn, Sql) of
		{error, R1} -> notify(io_lib:format("epg_error: ~s~n", [R1])), throw(epg_error);
		[{header, _H1},{data,[]}] -> [];
		[{header, _H1},{data,[T1]}] -> T1
	end.

%% backtracks cursor so that the same tuple is returned next time forward_one() is called
%% returns 'ok' or throws an epg_error
back_one(Conn) ->
	Back = "MOVE BACKWARD 1 FROM mycursor",
	case epg:exec(Conn, Back) of
		{error, R1} -> notify(io_lib:format("epg_error: ~s~n", [R1])), throw(epg_error);
		_O1 -> ok
	end.

%% connection-accepting wrappers for their diff_fetch counterparts
diff_fetch_one(Conn) -> C = diff_fetch:new(Conn), C:fetch().
diff_back_one(Conn) -> C = diff_fetch:new(Conn), C:back().

