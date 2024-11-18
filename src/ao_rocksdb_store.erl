%%%-----------------------------------------------------------------------------
%%% @doc A process wrapper over rocksdb storage. Replicates functionality of the
%%%      ao_fs_store module.
%%%
%%%      The data is stored in two Column Families:
%%%      1. Default - for raw data (e.g. TX records)
%%%      2. Meta - for meta information
%%%         (<<"raw">>/<<"link">>/<<"composite">> or <<"group">>)
%%% @end
%%%-----------------------------------------------------------------------------
-module(ao_rocksdb_store).
-include_lib("eunit/include/eunit.hrl").
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

path(_Opts, Path) ->
	Path.

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
				not_found ->
					not_found;
				ResolvedPath ->
					?debugFmt("Read auto resolved: ~p", [ResolvedPath]),
					gen_server:call(?MODULE, {read, join(ResolvedPath)}, ?TIMEOUT)
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
	Key = ao_fs_store:join(RawKey),
	Path = filename:split(Key),

	case do_resolve("", Path) of
		not_found -> not_found;
		<<"">> -> "";
		% converting back to list, so ao_cache can remove common
		BinResult -> binary_to_list(BinResult)
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
make_link(Opts, Existing, New) when is_list(Existing), is_list(New) ->
	ExistingBin = join(Existing),
	NewBin = join(New),
	make_link(Opts, ExistingBin, NewBin);
make_link(_Opts, Existing, New) ->
	gen_server:call(?MODULE, {make_link, Existing, New}, ?TIMEOUT).

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

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%=============================================================================
%%% Private
%%%=============================================================================
open_rockdb(Dir) ->
	ColumnFamilies = [{"default", []}, {"meta", []}],
	Options = [{create_if_missing, true}, {create_missing_column_families, true}],
	rocksdb:open_with_cf(Dir, Options, ColumnFamilies).

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

get(DBInfo, Key, Opts) ->
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

do_resolve(CurrPath, []) ->
	CurrPath;
do_resolve(CurrPath, ["messages", Key | Rest]) ->
	do_resolve(ao_fs_store:join([CurrPath, "messages", Key]), Rest);
do_resolve(CurrPath, ["computed", Key | Rest]) ->
	?debugFmt("Resolving computed: ~p", [[Key | Rest]]),
	do_resolve(ao_fs_store:join([CurrPath, "computed", Key]), Rest);
do_resolve(CurrPath, [LookupKey | Rest]) ->
	LookupPath = ao_fs_store:join([CurrPath, LookupKey]),
	?debugFmt("Lookup on ~p", [LookupPath]),
	NewCurrentPath =
		case meta(LookupPath) of
			{ok, <<"link">>} ->
				item_path(LookupPath);
			{ok, <<"raw">>} ->
				{ok, LookupPath};
			not_found ->
				do_resolve(LookupPath, Rest)
		end,
	case NewCurrentPath of
		not_found ->
			?debugFmt("Current path :~p", [list_to_binary(CurrPath)]),
			list_to_binary(CurrPath);
		Result when is_binary(Result) ->
			Result;
		{ok, Path} ->
			do_resolve(Path, Rest)
	end.
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

join(Key) when is_list(Key) ->
	KeyList = ao_fs_store:join(Key),
	maybe_convert_to_binary(KeyList);
join(Key) when is_binary(Key) -> Key.

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
%             end},
%             {"automatically extracts composite items", fun() ->
% 				% Composite items are supposed to be stored under a given group
% 				ok = make_group(#{}, <<"test_key">>),
% 				ok = write(#{}, <<"test_key/item">>, <<"item_data">>),

% 				{ok, Value} = read(#{}, <<"test_key">>),
%                 ?assertEqual(<<"item_data">>, Value)
%             end}
%         ]}.

api_test_() ->
	{foreach,
		fun() ->
			Pid = get_or_start_server(),
			unlink(Pid)
		end,
		fun(_) -> reset([]) end, [
			{"make_link/3 creates a link to actual data", fun() ->
				ok = write(ignored_options, <<"key1">>, <<"test_value">>),
				ok = make_link([], <<"key1">>, <<"key2">>),
				{ok, Value} = read([], <<"key2">>),

				?assertEqual(<<"test_value">>, Value)
			end},
			{"make_link/3 does not create links if keys are same", fun() ->
				ok = make_link([], <<"key1">>, <<"key1">>),
				?assertEqual(not_found, read(#{}, <<"key1">>))
			end},
			{"reset cleans up the database", fun() ->
				ok = write(ignored_options, <<"test_key">>, <<"test_value">>),

				ok = reset([]),
				?assertEqual(not_found, read(ignored_options, <<"test_key">>))
			end},
			{
				"resolve/2 returns the final key of the element (after following "
				"links)",
				fun() ->
					ok = write(#{}, <<"messages-real-key">>, <<"Data">>),
					ok = make_link(#{}, "real-key", "link-to"),

					?assertEqual("real-key", resolve(#{}, "link-to"))
				end
			}
		]}.

-endif.
