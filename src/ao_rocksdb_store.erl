%%%-----------------------------------------------------------------------------
%%% @doc A process wrapper over rocksdb storage. Replicates functionality of the
%%%      ao_fs_store module.
%%%
%%%      1. All the data is stored in the same rocksdb column family, making the
%%%         structure of the data flat;
%%%      2. The data is stored in the binary format
%%%      3. The concept of link is implemented with a data prefixed with a `link-`
%%%         word, so for example {<<my_key>>, <<"link-my_key2">>} means link to
%%%         my_key2
%%%      4. Read function tries to follow links and extract the data after the
%%%         full traversal
%%%      5. Entry type is identified with the help of the manifest. If entry has
%%%         a manifest it's composite, otherwise it's simple
%%%      6. Read automatically tries to get item entry
%%%      7. Composite items are store with the -item postfix (same as in the FS)
%%% @end
%%%-----------------------------------------------------------------------------
-module(ao_rocksdb_store).

-behaviour(gen_server).

-author("Oleg Tarasenko").

-behaviour(ao_store).

-define(TIMEOUT, 5000).

-include_lib("eunit/include/eunit.hrl").

% Behaviour based callbacks
-export([start/1, stop/1]).
-export([read/2, write/3]).
-export([list/2]).
-export([reset/1, make_link/3]).
-export([make_group/2]).
-export([type/2]).
-export([add_path/3, path/2]).
-export([resolve/2]).
% Starting/stopping process
-export([start_link/1]).
% Gen server callbacks
-export([init/1, terminate/2]).
-export([handle_cast/2, handle_info/2, handle_call/3]).
-export([code_change/3]).

-type key() :: binary().
-type value() :: binary().

-spec start_link(#{dir := term()}) -> ignore | {ok, pid()}.
start_link(#{dir := _Dir} = RocksDBOpts) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, RocksDBOpts, []);
start_link(_Opts) ->
	ignore.

-spec start(#{dir := term()}) -> ignore | {ok, pid()}.
start(Opts) ->
	start_link(Opts).

-spec stop(any()) -> ok.
stop(_Opts) ->
	gen_server:stop(?MODULE).

-spec reset([]) -> ok | no_return().
reset(_Opts) ->
	gen_server:call(?MODULE, reset, ?TIMEOUT).

%%------------------------------------------------------------------------------
%% @doc Read data by the key
%%
%% Recursively follows messages with values defined as <<"link-NewKey">>
%% @end
%%------------------------------------------------------------------------------
-spec read(Opts, Key) -> Result when
	Opts :: map(),
	Key :: key() | list(),
	Result :: {ok, value()} | not_found | {error, {corruption, string()}} | {error, any()}.
read(Opts, RawKey) ->
	Key = join(RawKey),
	case meta(Key) of
		not_found ->
			case resolve(Opts, Key) of
				not_found -> not_found;
				ResolvedPath -> gen_server:call(?MODULE, {read, join(ResolvedPath)}, ?TIMEOUT)
			end;
		_Result ->
			gen_server:call(?MODULE, {read, Key}, ?TIMEOUT)
	end.

%%------------------------------------------------------------------------------
%% @doc Write given Key and Value to the database
%%
%% @end
%%------------------------------------------------------------------------------
-spec write(Opts, Key, Value) -> Result when
	Opts :: map(),
	Key :: key() | list(),
	Value :: value() | list(),
	Result :: ok | {error, any()}.
write(_Opts, RawKey, Value) ->
	Key = join(RawKey),
	gen_server:call(?MODULE, {write, Key, Value}, ?TIMEOUT).

meta(Key) ->
	gen_server:call(?MODULE, {meta, join(Key)}, ?TIMEOUT).

%%------------------------------------------------------------------------------
%% @doc List key/values stored in the storage so far.
%%      *Note*: This function is slow, and probably should not be used on
%%      production
%% @end
%%------------------------------------------------------------------------------
-spec list(Opts, Path) -> Result when
	Opts :: any(),
	Path :: any(),
	Result :: [{key(), value()}].
list(_Opts, _Path) ->
	% Lists all items under given path. Looks useful for composite items
	Result = gen_server:call(?MODULE, list, ?TIMEOUT),
	lists:map(
		fun(Key) ->
			Res =
				case meta(Key) of
					{ok, <<"link">>} ->
						% {ok, Value} = read(#{}, Key),
						<<"link">>;
					{ok, Type} ->
						Type
				end,
			{Key, Res}
		end,
		Result
	).

%% @doc Replace links in a path with the target of the link.
%% [<<"computed-q-T3q22d3Ef62yR8hPK07iylm9Ed6tzMxIcvydIAd0M">>,
% ["slot","1"]]
-spec resolve(Opts, Path) -> Result when
	Opts :: any(),
	Path :: binary() | list(),
	Result :: not_found | binary() | [binary() | list()].
resolve(_Opts, RawKey) ->
	?debugFmt("Trying to resolve ~p", [RawKey]),
	Key = ao_fs_store:join(RawKey),
	Path = filename:split(Key),

	case myresolve("", Path) of
		not_found -> not_found;
		% converting back to list, so ao_cache can remove common
		BinResult -> binary_to_list(BinResult)
	end.

myresolve(CurrPath, []) ->
	CurrPath;
myresolve(CurrPath, ["messages", Key | Rest]) ->
	myresolve(ao_fs_store:join([CurrPath, "messages", Key]), Rest);
myresolve(CurrPath, ["computed", Key | Rest]) ->
	myresolve(ao_fs_store:join([CurrPath, "computed", Key]), Rest);
myresolve(CurrPath, [LookupKey | Rest]) ->
	LookupPath = ao_fs_store:join([CurrPath, LookupKey]),
	NewCurrentPath =
		case meta(LookupPath) of
			{ok, <<"link">>} ->
				item_path(LookupPath);
			{ok, <<"raw">>} ->
				{ok, LookupPath};
			_Other ->
				not_found
		end,
	case NewCurrentPath of
		not_found -> not_found;
		{ok, Path} -> myresolve(Path, Rest)
	end.

item_path(Key) ->
	% Maybe I don't need it, as it just does get no follow
	gen_server:call(?MODULE, {item_path, join(Key)}, ?TIMEOUT).

-spec type(Opts, Key) -> Result when
	Opts :: map(),
	Key :: binary(),
	Result :: composite | simple | not_found.

type(_Opts, not_found) ->
	not_found;
type(_Opts, RawKey) ->
	Key = join(RawKey),
	% TODO: Rewrite doc!
	% The fs adapter looks on the FS. If it's a directory it says 'comosite'
	% composite item is an item which has manifest!
	?debugFmt("[type] Identifying the type by key: ~p --> ~p", [Key, meta(Key)]),
	case meta(Key) of
		not_found ->
			not_found;
		{ok, <<"composite">>} ->
			composite;
		{ok, <<"group">>} ->
			composite;
		{ok, <<"link">>} ->
			simple;
		{ok, _} ->
			simple
	end.

%%------------------------------------------------------------------------------
%% @doc TODO WRite doc
%% @end
%%------------------------------------------------------------------------------
make_group(_Opts, Path) ->
	BinPath = join(Path),
	gen_server:call(?MODULE, {make_group, BinPath}, ?TIMEOUT).

-spec make_link(any(), key(), key()) -> ok.
make_link(_, Key1, Key1) ->
	ok;
make_link(Opts, Key1, Key2) when is_list(Key1), is_list(Key2) ->
	Key1Bin = join(Key1),
	Key2Bin = join(Key2),
	make_link(Opts, Key1Bin, Key2Bin);
make_link(_Opts, Key1, Key2) ->
	gen_server:call(?MODULE, {make_link, Key1, Key2}, ?TIMEOUT).

%% Flatten and convert items to binary.

%% @doc Add two path components together. // is not used
add_path(_Opts, Path1, Path2) ->
	Path1 ++ Path2.

%%%=============================================================================
%%% Gen server callbacks
%%%=============================================================================
init(_Store = #{dir := Dir}) ->
	{ok, DBHandle, [DefaultH, MetaH]} = open_rockdb(Dir),
	State = #{
		db_handle => DBHandle,
		dir => Dir,
		data_family => DefaultH,
		meta_family => MetaH
	},
	{ok, State}.

handle_cast(_Request, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

handle_call({write, Key, Value}, _From, State) ->
	?debugFmt("[info][info] Writing the key ~p [info][info]", [Key]),
	ok = write_meta(State, Key, <<"raw">>, #{}),
	Result = write_data(State, Key, Value, #{}),
	{reply, Result, State};
handle_call({make_group, Key}, _From, State) ->
	ok = write_meta(State, Key, <<"group">>, #{}),
	Result = write_data(State, Key, <<"group">>, #{}),
	{reply, Result, State};
handle_call({make_link, Key1, Key2}, _From, State) ->
	ok = write_meta(State, Key2, <<"link">>, #{}),
	Result = write_data(State, Key2, Key1, #{}),

	{reply, Result, State};
handle_call({read, Key}, _From, DBInfo) ->
	Result = get(DBInfo, Key, #{}),
	{reply, Result, DBInfo};
handle_call({meta, Key}, _From, State) ->
	Result = get_meta(State, Key, #{}),
	{reply, Result, State};
handle_call({item_path, BaseKey}, _From, State) ->
	Result = get_data(State, BaseKey, #{}),
	{reply, Result, State};
handle_call(reset, _From, #{db_handle := DBHandle, dir := Dir}) ->
	ok = rocksdb:close(DBHandle),
	ok = rocksdb:destroy(Dir, []),

	{ok, NewDBHandle, [DefaultH, MetaH]} = open_rockdb(Dir),
	NewState = #{
		db_handle => NewDBHandle,
		dir => Dir,
		data_family => DefaultH,
		meta_family => MetaH
	},

	{reply, ok, NewState};
handle_call(list, _From, State = #{db_handle := DBHandle}) ->
	{ok, Iterator} = rocksdb:iterator(DBHandle, []),
	Items = collect(Iterator),
	{reply, Items, State};
handle_call(_Request, _From, State) ->
	{reply, handle_call_unrecognized_message, State}.

open_rockdb(Dir) ->
	ColumnFamilies = [{"default", []}, {"meta", []}],
	Options = [{create_if_missing, true}, {create_missing_column_families, true}],
	rocksdb:open_with_cf(Dir, Options, ColumnFamilies).

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%=============================================================================
%%% Private
%%%=============================================================================
get_data(#{data_family := F, db_handle := Handle}, Key, Opts) ->
	rocksdb:get(Handle, F, Key, Opts).

get_meta(#{meta_family := F, db_handle := Handle}, Key, Opts) ->
	rocksdb:get(Handle, F, Key, Opts).

write_meta(DBInfo, Key, Value, Opts) ->
	?debugFmt("writing meta: ~p", [{Key, Value}]),
	#{meta_family := ColumnFamily, db_handle := Handle} = DBInfo,
	rocksdb:put(Handle, ColumnFamily, Key, Value, Opts).

write_data(DBInfo, Key, Value, Opts) ->
	?debugFmt("writing data: ~p", [Key]),
	#{data_family := ColumnFamily, db_handle := Handle} = DBInfo,
	% ?debugFmt("String data: ~p: ~p", [Key, Value]),
	rocksdb:put(Handle, ColumnFamily, Key, Value, Opts).

% get(_DBInfo, not_found, _Opts, _Path) ->
%     not_found;
-include_lib("eunit/include/eunit.hrl").
get(DBInfo, Key, Opts) ->
	?debugFmt("[priv_get]Get meta: ~p", [{Key, get_meta(DBInfo, Key, Opts)}]),
	case get_data(DBInfo, Key, Opts) of
		{ok, Value} ->
			case get_meta(DBInfo, Key, Opts) of
				{ok, <<"link">>} ->
					% Automatically follow the link
					get(DBInfo, Value, Opts);
				{ok, <<"raw">>} ->
					{ok, Value};
				{ok, <<"group">>} ->
					% extract the group item
					get(DBInfo, ao_fs_store:join([Key, <<"item">>]), Opts);
				_OtherMeta ->
					{ok, Value}
			end;
		not_found ->
			not_found
	end.

collect(Iterator) ->
	{ok, Key, Value} = rocksdb:iterator_move(Iterator, <<>>),
	% collect(Iterator, [{Key, Value}]).
	collect(Iterator, [Key]).

collect(Iterator, Acc) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, Value} ->
			% Continue iterating, accumulating the key-value pair in the list
			% collect(Iterator, [{Key, Value} | Acc]);
			collect(Iterator, [Key | Acc]);
		{error, invalid_iterator} ->
			% Reached the end of the iterator, return the accumulated list
			lists:reverse(Acc)
	end.

maybe_convert_to_binary(Value) when is_list(Value) ->
	list_to_binary(Value);
maybe_convert_to_binary(Value) when is_binary(Value) ->
	Value.

%%%=============================================================================
%%% Tests
%%%=============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

get_or_start_server() ->
	Opts = #{dir => "TEST-data/rocksdb"},
	case start_link(Opts) of
		{ok, Pid} ->
			Pid;
		{error, {already_started, Pid}} ->
			Pid
	end.

% utils_test_() ->
%     {foreach, fun() -> ignored end, [
%         {"is_charlist/1 returns true for charlists", fun() -> ?assertEqual(true, is_charlist("My Charlist")) end},
%         {"is_charlist/2 returns false for regular lists", fun() -> ?assertEqual(false, is_charlist(["My Charlist"])) end},
%         {"is_charlist/2 returns false for non lists", fun() -> ?assertEqual(false, is_charlist(<<"Binary">>)) end},
%         {"is_charlist/2 returns false non charlists", fun() -> ?assertEqual(false, is_charlist([-1, 2, 12])) end}
%     ]}.

% new_path(_Opts, [Folder, Rest]) ->
% 	FolderJoined =
% 		case is_charlist(Folder) of
% 	  		true  -> Folder;
% 			false -> lists:join("-", Folder)
% 		end,
% 	FolderBin = erlang:list_to_binary(FolderJoined),

% 	RestJoined = lists:join("-", Rest),
% 	RestBin = erlang:list_to_binary(RestJoined),
% 	<<FolderBin/binary, "-", RestBin/binary>>.

path(_Opts, Path) ->
	Path.

join(Key) when is_list(Key) ->
	KeyList = ao_fs_store:join(Key),
	maybe_convert_to_binary(KeyList);
join(Key) when is_binary(Key) -> Key.

path_test_() ->
	{foreach, fun() -> ignored_setup end, [
		{"basic path construction", fun() ->
			?assertEqual(<<"messages-MyID">>, path([], [["messages"], "MyID"])),
			?assertEqual(<<"messages-MyID-item">>, path([], [["messages"], "MyID", "item"])),
			?assertEqual(<<"messages-MyID">>, path([], ["messages", "MyID"]))
		% ao_store:path(Store, [DirBase, fmt_id(ID), Subpath])  // lookup
		% write(Store, ao_store:path(Store, ["messages"]), Item).

		% ao_store:path(Store, ["assignments", fmt_id(ProcID), integer_to_list(Slot)
		% RawMessagePath = ao_store:path(Store, ["messages", UnsignedID]),
		% ProcMessagePath = ao_store:path(Store, ["computed", fmt_id(ProcID), UnsignedID]),
		% ProcSlotPath = ao_store:path(Store, ["computed", fmt_id(ProcID), "slot", integer_to_list(Slot)]),
		% ao_store:path(Store, [DirBase, fmt_id(ID), Subpath])
		end}
		% {"lookup keys are returned with the base path", fun() ->
		%     Opts = #{},
		%     Path = ["part1", "part2", ["lookupkey1", "lookupkey2"]],
		%     Result = path(Opts, Path),

		%     ?assertEqual([<<"part1-part2">>, ["lookupkey1", "lookupkey2"]], Result)
		% end}
	]}.

% write_read_test_() ->
%     {foreach,
%         fun() ->
%             Pid = get_or_start_server(),
%             unlink(Pid)
%         end,
%         fun(_) -> ao_rocksdb_store:reset([]) end, [
%             {"can read/write data", fun() ->
%                 ok = write(#{}, <<"test_key">>, <<"test_value">>),
%                 {ok, Value} = read(ignored_options, <<"test_key">>),

%                 ?assertEqual(<<"test_value">>, Value)
%             end},
%             {"returns not_found for non existing keys", fun() ->
%                 Value = read(#{}, <<"non_existing">>),

%                 ?assertEqual(not_found, Value)
%             end},
%             {"follows links", fun() ->
%                 ok = write(#{}, <<"test_key2">>, <<"value_under_linked_key">>),
%                 ok = make_link(#{}, <<"test_key2">>, <<"test_key">>),
%                 {ok, Value} = read(#{}, <<"test_key">>),

%                 ?assertEqual(<<"value_under_linked_key">>, Value)
%             end}
%             % TTODO FIXME
%             % {"automatically extracts items", fun() ->
%             %     ok = write(#{}, <<"test_key-item">>, <<"item_data">>),
%             %     {ok, Value} = read(#{}, <<"test_key">>),

%             %     ?assertEqual(<<"item_data">>, Value)
%             % end}
%         ]}.

% api_test_() ->
%     {foreach,
%         fun() ->
%             Pid = get_or_start_server(),
%             unlink(Pid)
%         end,
%         fun(_) -> reset([]) end, [
%             {"make_link/3 creates a link to actual data", fun() ->
%                 ok = write(ignored_options, <<"key1">>, <<"test_value">>),
%                 ok = make_link([], <<"key1">>, <<"key2">>),
%                 {ok, Value} = read([], <<"key2">>),

%                 ?assertEqual(<<"test_value">>, Value)
%             end},
%             {"make_link/3 does not create links if keys are same", fun() ->
%                 ok = make_link([], <<"key1">>, <<"key1">>),
%                 ?assertEqual(not_found, read(#{}, <<"key1">>))
%             end},
%             {"reset cleans up the database", fun() ->
%                 ok = write(ignored_options, <<"test_key">>, <<"test_value">>),

%                 ok = reset([]),
%                 ?assertEqual(not_found, read(ignored_options, <<"test_key">>))
%             end}
%             % {"item_path/1 can return the full path of a given item (all links)", fun() ->
%     ok = write(#{}, <<"key">>, <<"link-key1">>),
%     ok = write(#{}, <<"key1">>, <<"link-key2">>),
%     ok = write(#{}, <<"key2">>, <<"link-key3">>),
%     ok = write(#{}, <<"key3">>, <<"final value">>),

%     {ok, Path} = item_path(<<"key">>),
%     ?assertEqual([<<"key3">>, <<"key2">>, <<"key1">>, <<"key">>], Path)
% end},
% {
%     "resolve/2 returns the final key of the element (after following "
%     "links)",
%     fun() ->
%         ok = write(#{}, <<"key">>, <<"link-messages-key1">>),
%         ok = write(#{}, <<"messages-key1">>, <<"Data">>),

%         ?assertEqual(<<"key1">>, resolve(#{}, <<"key">>))
%     end
% }
% ]}.

-endif.
