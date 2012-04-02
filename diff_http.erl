-module(diff_http).
-export([start/1,dispatch_requests/1,buffer_loop/0,read/1,write/2]).

% API ---------------------------------------------------------------------------------
start(Port) ->
	code:add_path("deps"),
	application:start(crypto), application:start(mochiweb),
	Loop = fun (Req) -> ?MODULE:dispatch_requests(Req) end,
	% register self in the process registry
	register(?MODULE,self()),
	% connection strings
	ets:new(conn_strings, [public, named_table]),
	ets:insert(conn_strings, {db1, "user=user password=pass host=localhost port=5432 dbname=db1"}),
	ets:insert(conn_strings, {db2, "user=user password=pass host=localhost port=5432 dbname=db2"}),
	ets:insert(conn_strings, {db3, "user=user password=pass host=localhost port=5432 dbname=db3"}),
	ets:insert(conn_strings, {db4, "user=user password=pass host=localhost port=5432 dbname=db4"}),
	ets:insert(conn_strings, {db5, "user=user password=pass host=localhost port=5432 dbname=db5"}),
	% starting mochiweb
	SPort = lists:flatten(io_lib:format("~p", [Port])),
	Name = list_to_atom("mochiweb_"++SPort),
	Options = [{name, Name}, {port, Port}, {loop, Loop}],
	Mochi = mochiweb_http:start(Options),
	% supporting per-request buffers
	ets:new(diff_http_buffer, [public, named_table]),
	loop(Mochi).

loop(_Mochi) ->
	Buffers = spawn_link(fun buffer_loop/0),
	register(diff_http_buffers_proc,Buffers),
	receive
		stop -> ok
	end.

buffer_loop() ->
	receive
		{From, Handle, read} ->
			Reply = case ets:lookup(diff_http_buffer, Handle) of
				[] -> [];
				[{Handle, Buffer}] -> Buffer
			end,
			ets:insert(diff_http_buffer, {Handle, []}),
			From!{buffer, Reply}, buffer_loop();
		{data, Handle, Line} when is_list(Line) ->
			Old = case ets:lookup(diff_http_buffer, Handle) of
				[] -> [];
				[{Handle, Buffer}] -> Buffer
			end,
			ets:insert(diff_http_buffer, {Handle, lists:append([Old,Line])}),
			buffer_loop();
		stop -> ok
	end.

%% flushes and returns entire output Buffer, returns Buffer or 'timeout'
read(Handle) when is_atom(Handle) ->
	whereis(diff_http_buffers_proc)!{self(), Handle, read},
	receive
		{buffer, Buffer} -> Buffer
		after 10000 -> timeout
	end.

%% appends string to the output Buffer
write(Handle, Data) when is_atom(Handle), is_list(Data) ->
	whereis(diff_http_buffers_proc)!{data, Handle, Data}.

%% ------------------------------------------------------------------------------------

%% CONTROLLER -------------------------------------------------------------------------
dispatch_requests(Req) ->
	Path = Req:get(path),
	Action = clean_path(Path),
	handle(Action, Req).
%% ------------------------------------------------------------------------------------

%% URLs -------------------------------------------------------------------------------
%
%  Summaries (they all produce HTML fragments):
%  ----------------------------------------------------------------------------
%  /                    ->   basic form
%  /diff                ->   obtain diff results
%  /buffer              ->   flush buffer for ReqId
%
%% ------------------------------------------------------------------------------------

%% HANDLERS ---------------------------------------------------------------------------
handle(Action, Req) when Action == "/" ->
	Req:respond({200, [{"Content-Type", "text/html"}],
		list_to_binary(
			sgte:render_str(template(Action), [{id, key_gen()}])
		)
	});
handle(Action, Req) when Action == "/diff" ->
	Pairs = Req:parse_post(),
	%% Input = re:split(get_lines(Pairs,""),"\r\n",[{return,list}]),
	ReqId = list_to_atom(get_value("id",Pairs,"")),
	Table = re:replace(get_value("table",Pairs,""),"'","",[global,{return,list}]),
	Restriction = case get_value("restriction",Pairs,"") of
		[] -> "";
		R -> "WHERE "++re:replace(R,"'","",[global,{return,list}])
	end,
	From = get_value("from",Pairs,""),
	To = get_value("to",Pairs,""),
	Id = lists:concat([re:replace(Table,"\\.","_",[global,{return,list}]), "_", From, "_", To]),
	epg:start(list_to_atom("from_"++Id)),
	epg:start(list_to_atom("to_"++Id)),
	receive after 2000 -> ok end,
	[{_A1, Conn1}] = ets:lookup(conn_strings, list_to_atom(From)),
	[{_A2, Conn2}] = ets:lookup(conn_strings, list_to_atom(To)),
	{_A3, C1} = epg:exec(list_to_atom("from_"++Id), Conn1),
	{_A4, C2} = epg:exec(list_to_atom("to_"++Id), Conn2),
	%% Diffing!
	MyDiff = diff:new(ReqId),
	{Count, Diff} = try
		case MyDiff:tables(Restriction,list_to_atom("from_"++Id),list_to_atom("to_"++Id),Table) of
			[] -> {0,"identical!"};
			D -> {length(D),lists:concat([io_lib:format("~p,~s~n",[Op,Pri]) || {Op,Pri} <- D])}
		end
	catch
		A:B -> {null,io_lib:format("~p:~p",[A,B])}
	end,
	%% Response Code
	Code = case Count of
		null -> 404;
		_Else -> 200
	end,
	%% ...done
	epg:stop(list_to_atom("from_"++Id)),
	epg:stop(list_to_atom("to_"++Id)),
	Req:respond({Code, [{"Content-Type", "text/plain"}],
		list_to_binary(
			sgte:render_str(template(Action), [
				{from, From},
				{to, To},
				{table, Table},
				{restriction, Restriction},
				{conn1, re:replace(C1,"\n"," ",[global,{return,list}])},
				{conn2, re:replace(C2,"\n"," ",[global,{return,list}])},
				{count, io_lib:format("~p",[Count])},
				{diff,Diff}
			])
		)
	});

handle(Action, Req) when Action == "/buffer" ->
	Pairs = Req:parse_post(),
	ReqId = list_to_atom(get_value("id",Pairs,"")),
	Buffer = read(ReqId),
	Req:respond({200, [{"Content-Type", "text/plain"}], list_to_binary(Buffer)});

handle(Action, Req) when Action == "/jquery.js" ->
	case file:read_file("./jquery.js") of
		{ok, Binary} -> send_text(Req, 200, "text/plain", Binary);
		{error, _} -> send_text(Req, 404, "text/plain", list_to_binary("File not found!\n"))
	end;

handle(_, Req) ->
	Req:respond({404, [{"Content-Type", "text/plain"}], <<"error">>}).
%% ------------------------------------------------------------------------------------

%% TEMPLATES --------------------------------------------------------------------------
template("/") ->
	{ok, Compiled} = sgte:compile("<html>
	<head>
		<title>pg51g http service</title>
		<script type=\"text/javascript\" src=\"/jquery.js\"></script>          
		<script type=\"text/javascript\">                                   
			arg = function () {
				$.post(\"/buffer\", { id: \"$id$\" }, 
					function(data) {
						$('#progress').html($('#progress').text()+data);
					}
				);
			}
		 	$(document).ready(function() {
		    // do stuff when DOM is ready
         		window.setInterval(arg,5000);
			});
		</script>
	</head>
	<body bgcolor=\"white\">
		<div style=\"position: absolute; top: 10px; left: 10px; width: 100%; font-family: sans-serif; font-size: 9pt;\">
			<div style=\"float:left;\">
				<h2>Diff It!</h2>
			</div>
				<br/>
				<p>(very) alpha</p>
			<br/>
			<form action=\"/diff\" method=\"POST\">
				<input type=\"hidden\" name=\"id\" value=\"$id$\"/>
				Table: <input type=\"text\" name=\"table\"/>
				Restriction: <input type=\"text\" name=\"restriction\"/>
				From: <select name=\"from\">
					<option>db1</option>
					<option>db2</option>
					<option>db3</option>
					<option>db4</option>
					<option>db5</option>
				</select>
				To: <select name=\"to\">
					<option>db1</option>
					<option>db2</option>
					<option>db3</option>
					<option>db4</option>
					<option>db5</option>
				</select>
				<br/><br/>
				<input type=\"submit\" name=\"submit\" value=\"diff now!\"/>
			</form>
			<p>Example: \"schema.table\", \"id BETWEEN 10000 AND 20000\", db1, db2 (no quotes, please)</p>
			<br/>
			<pre id=\"progress\" style=\"width: 90%; color: white; background: grey\"></pre>
		</div>
	</body>
</html>
"),
	Compiled;
template("/diff") ->
	{ok, Compiled} = sgte:compile("table: $table$
restriction: $restriction$
from: $from$
to: $to$

Connections: ------------------------------------------------------------
conn1: $conn1$
conn2: $conn2$
-------------------------------------------------------------------------
Number of differences found: $count$

$diff$

"),
	Compiled.
%% -----------------------------------------------------------------------------------

%% HELPER FUNCTIONS -------------------------------------------------------------------
clean_path(Path) ->
	case string:str(Path, "?") of
		0 -> Path;
		N -> string:substr(Path, 1, string:len(Path) - (N + 1))
	end.

get_value(_Key,[],Value) -> Value;
get_value(Key,[H | T],Value) -> 
	case H of
		{Key,Lines} -> get_value(Key,T,Lines);
		_ -> get_value(Key,T,Value)
	end.

send_file(Req, Binary) ->
	Req:respond({	200,
					[
						{"Content-Type", "application/octet-stream"},
						{"Content-Transfer-Encoding", "base64"}
					],
					Binary
				}).

send_text(Req, Code, Type, Binary) ->
	Req:respond({	Code,
					[
						{"Content-Type", Type}
					],
					Binary
				}).

% we need to set the seed here, otherwise key is predictable
key_gen() ->
	{A1,A2,A3} = now(), random:seed(A1, A2, A3),
	List = random_str(32,
		{"q","w","e","r","t","y","Q","W","E","R","T","Y","1","2","3","4","5","6","7","8","9","0"}
	),
	lists:flatten(List).

random_str(0, _Chars) -> [];
random_str(Len, Chars) -> [random_char(Chars)|random_str(Len-1, Chars)].
random_char(Chars) -> element(random:uniform(tuple_size(Chars)), Chars).

%% ------------------------------------------------------------------------------------

