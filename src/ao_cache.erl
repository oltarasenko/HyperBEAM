-module(ao_cache).
-compile(export_all).
-export([
	read_output/3,
	write/2,
	write_output/4,
	write_assignment/2,
	outputs/2,
	assignments/2,
	latest/2, latest/3, latest/4,
	read/2,
	lookup/2,
	read_assignment/3,
	read_message/2
]).
-include("src/include/ao.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_DATA_DIR, "data").
-define(TEST_DIR, "test-cache").
-define(TEST_STORE_MODULE, ao_fs_store).
-define(TEST_STORE, [{?TEST_STORE_MODULE, #{prefix => ?TEST_DIR}, #{scope => local}}]).
-define(COMPUTE_CACHE_DIR, "computed").
-define(ASSIGNMENTS_DIR, "assignments").

%%% A cache of AO messages and compute results.
%%%
%%% In AO, every message is a combinator: The message itself represents a
%%% 'processor' that can be applied to a new message, yielding a result.
%%% As a consequence, a simple way of understanding AO's computation model is to
%%% think of it as a dictionary: Every message is a key, yielding its computed value.
%%%
%%% Each message itself can be raw data with an associated header (containing metadata),
%%% or a bundle of other messages (its children). These children are expressed as
%%% either maps or list of other messages.
%%%
%%% We store each of the messages in a cache on disk. The cache is a simple
%%% wrapper that allows us to look up either the direct key (a message's ID --
%%% either signed or unsigned) or a 'subpath'. We also store a cache of the linkages
%%% between messages as symlinks. In the backend, we store each message as either a
%%% directory -- if it contains further data items inside -- or as a file, if it is
%%% a simple value.
%%%
%%% The file structure of the store is as follows:
%%%
%%% Root: ?DEFAULT_DATA_DIR
%%% Messages: ?DEFAULT_DATA_DIR/messages
%%% Computed outputs: ?DEFAULT_DATA_DIR/computed
%%%
%%% Outputs by process: ?DEFAULT_DATA_DIR/computed/ProcessID
%%% Outputs by slot on process: ?DEFAULT_DATA_DIR/computed/ProcessID/slot/[n]
%%% Outputs by message on process: ?DEFAULT_DATA_DIR/computed/ProcessID/MessageID[/Subpath]
%%%
%%% Outputs are stored as symlinks to the actual file or directory containing the message.
%%% Messages that are composite are represented as directories containing their childen
%%% (by ID and by subpath), as well as their base message stored at `.base`.

latest(Store, ProcID) ->
	latest(Store, ProcID, undefined).
latest(Store, ProcID, Limit) ->
	latest(Store, ProcID, Limit, []).
latest(Store, ProcID, Limit, Path) ->
	case outputs(Store, ProcID) of
		[] ->
			not_found;
		AllOutputSlots ->
			?c(
				{searching_for_latest_slot, {proc_id, fmt_id(ProcID)}, {limit, Limit}, {path, Path},
					{all_output_slots, AllOutputSlots}}
			),
			case
				first_slot_with_path(
					Store,
					fmt_id(ProcID),
					lists:reverse(lists:sort(AllOutputSlots)),
					Limit,
					Path
				)
			of
				not_found ->
					not_found;
				Slot ->
					ResolvedPath =
						ao_store:resolve(
							Store,
							ao_store:path(Store, ["computed", fmt_id(ProcID), "slot", integer_to_list(Slot)])
						),
					?c({resolved_path, ResolvedPath}),
					Msg = read(Store, ResolvedPath),
					?c(got_message),
					{Slot, Msg}
			end
	end.

first_slot_with_path(_Store, _ProcID, [], _Limit, _Path) ->
	not_found;
first_slot_with_path(Store, ProcID, [AfterLimit | Rest], Limit, Path) when AfterLimit > Limit ->
	first_slot_with_path(Store, ProcID, Rest, Limit, Path);
first_slot_with_path(Store, ProcID, [LatestSlot | Rest], Limit, Path) ->
	?c({trying_slot, LatestSlot, Path}),
	RawPath =
		build_path(
			["computed", process, "slot", slot] ++ Path,
			#{slot => integer_to_list(LatestSlot), process => fmt_id(ProcID)}
		),
	ResolvedPath = ao_store:resolve(Store, RawPath),
	case ao_store:type(Store, ResolvedPath) of
		not_found -> first_slot_with_path(Store, ProcID, Rest, Limit, Path);
		_ -> LatestSlot
	end.

build_path(PathList, Map) ->
	lists:map(
		fun
			(Ref) when is_atom(Ref) -> maps:get(Ref, Map);
			(Other) -> Other
		end,
		PathList
	).

outputs(Store, ProcID) ->
	slots(Store, [?COMPUTE_CACHE_DIR, fmt_id(ProcID), "slot"]).

assignments(Store, ProcID) ->
	slots(Store, [?ASSIGNMENTS_DIR, fmt_id(ProcID)]).

slots(Store, Path) ->
	SlotDir = ao_store:path(Store, Path),
	case ao_store:list(Store, SlotDir) of
		{ok, Names} -> [list_to_integer(Name) || Name <- Names];
		{error, _} -> []
	end.

%% Write a full message to the cache.
write_output(Store, ProcID, Slot, Item) ->
	% Write the message into the main cache
	ok = write(Store, Item),
	UnsignedID = fmt_id(Item, unsigned),
	SignedID = fmt_id(Item, signed),
	% Create symlinks from the message on the process and the slot on the process
	% to the underlying data.
	RawMessagePath = ao_store:path(Store, ["messages", UnsignedID]),
	ProcMessagePath = ao_store:path(Store, ["computed", fmt_id(ProcID), UnsignedID]),
	ProcSlotPath = ao_store:path(Store, ["computed", fmt_id(ProcID), "slot", integer_to_list(Slot)]),
	ao_store:make_link(Store, RawMessagePath, ProcMessagePath),
	ao_store:make_link(Store, RawMessagePath, ProcSlotPath),
	case ar_bundles:is_signed(Item) of
		true ->
			ok = ao_store:make_link(
				Store,
				RawMessagePath,
				ao_store:path(Store, ["computed", fmt_id(ProcID), SignedID])
			);
		false ->
			already_exists
	end,
	ok.

%% Write a full message to the cache.
write_assignment(Store, Assignment) ->
	% Write the message into the main cache
	{_, ProcID} = lists:keyfind(<<"Process">>, 1, Assignment#tx.tags),
	{_, Slot} = lists:keyfind(<<"Slot">>, 1, Assignment#tx.tags),
	ok = write(Store, Assignment),
	UnsignedID = fmt_id(Assignment, unsigned),
	SignedID = fmt_id(Assignment, signed),
	% Create symlinks from the message on the process and the
	% slot on the process to the underlying data.
	RawMessagePath =
		ao_store:path(Store, [
			"messages",
			UnsignedID
		]),
	AssignmentPathByNum =
		ao_store:path(Store, [
			"assignments",
			fmt_id(ProcID),
			binary_to_list(Slot)
		]),
	AssignmentPathByID =
		ao_store:path(Store, [
			"assignments",
			fmt_id(ProcID),
			SignedID
		]),
	AssignmentPathByUnsignedID =
		ao_store:path(Store, [
			"assignments",
			fmt_id(ProcID),
			UnsignedID
		]),
	ao_store:make_link(Store, RawMessagePath, AssignmentPathByNum),
	ao_store:make_link(Store, RawMessagePath, AssignmentPathByID),
	ao_store:make_link(Store, RawMessagePath, AssignmentPathByUnsignedID),
	ok.

write(Store, Item) ->
	write(Store, ao_store:path(Store, ["messages"]), Item).

write(Store, Path, Item) when not is_record(Item, tx) ->
	?c(writing_non_tx_item),
	write(Store, Path, ar_bundles:normalize(Item));
write(Store, Path, Item = #tx{unsigned_id = ?DEFAULT_ID}) ->
	?c(writing_tx_item_with_default_id),
	?debugMsg("unsigned_id here. That's the problem. Looping"),
	write(Store, Path, ar_bundles:normalize(Item));
write(Store, Path, Item) ->
	case ar_bundles:type(Item) of
		binary ->
			% The item is a raw binary. Write it into the store and make a
			% link for the signed ID if it is different.
			UnsignedID = ar_bundles:id(Item, unsigned),
			SignedID = ar_bundles:id(Item, signed),
			UnsignedPath = ao_store:path(Store, [Path, fmt_id(UnsignedID)]),
			?c({writing_item, UnsignedPath}),
			ok = ao_store:write(Store, UnsignedPath, ar_bundles:serialize(Item)),
			if
				SignedID =/= not_signed ->
					SignedPath = ao_store:path(Store, [Path, fmt_id(SignedID)]),
					?c({linking_item, SignedPath}),
					ao_store:make_link(Store, UnsignedPath, SignedPath);
				true ->
					link_unnecessary
			end;
		CompositeType ->
			write_composite(Store, Path, CompositeType, Item)
	end,
	ok.

write_composite(Store, Path, map, Item) ->
	% The item is a map. We should:
	% 1. Write the header item into the store without its children.
	%    The header item is the normal item, but with the manifest placed
	%    in the data field. This is unpacked in `read/2`.
	% 2. Write each child into the store.
	% 3. Make links from the keys in the map to the corresponding messages.
	% This process will recurse as necessary to write grandchild messages.
	UnsignedHeaderID = ar_bundles:id(Item, unsigned),
	?c({starting_composite_write, fmt_id(UnsignedHeaderID)}),
	ok = ao_store:make_group(Store, Dir = ao_store:path(Store, [Path, fmt_id(UnsignedHeaderID)])),
	SignedHeaderID = ar_bundles:id(Item, signed),
	?c({writing_composite_header, {unsigned, fmt_id(UnsignedHeaderID)}, {signed, fmt_id(SignedHeaderID)}}),
	ao_store:make_link(
		Store,
		ao_store:path(Store, [Path, fmt_id(UnsignedHeaderID)]),
		ao_store:path(Store, [Path, fmt_id(SignedHeaderID)])
	),
	ao_store:write(
		Store,
		ao_store:path(Store, [Path, fmt_id(UnsignedHeaderID), "item"]),
		ar_bundles:serialize(
			Item#tx{
				data = #{<<"manifest">> => ar_bundles:manifest_item(Item)}
			}
		)
	),
	maps:map(
		fun(Key, Subitem) ->
			% Note: _Not_ relative to the Path! All messages are stored at the
			% same root of the store.
			ok = write(Store, Subitem),
			ao_store:make_link(
				Store,
				ao_store:path(Store, [Path, fmt_id(Subitem)]),
				ao_store:path(Store, [Dir, Key])
			)
		end,
		ar_bundles:map(Item)
	),
	ok;
write_composite(Store, Path, list, Item) ->
	write_composite(Store, Path, map, ar_bundles:normalize(Item)).

read_message(Store, MessageID) ->
	lookup(Store, ["messages", MessageID]).

read_output(Store, ProcID, undefined) ->
	element(2, latest(Store, ProcID));
read_output(Store, ProcID, Slot) when is_integer(Slot) ->
	read_output(Store, ProcID, ["slot", integer_to_list(Slot)]);
read_output(Store, ProcID, MessageID) when is_binary(MessageID) andalso byte_size(MessageID) == 32 ->
	read_output(Store, fmt_id(ProcID), fmt_id(MessageID));
read_output(Store, ProcID, SlotBin) when is_binary(SlotBin) ->
	read_output(Store, ProcID, ["slot", binary_to_list(SlotBin)]);
read_output(Store, ProcID, SlotRef) ->
	?c({reading_computed_result, ProcID, SlotRef}),
	ResolvedPath =
		P2 = ao_store:resolve(
			Store,
			P1 = ao_store:path(Store, [?COMPUTE_CACHE_DIR, ProcID, SlotRef])
		),
	?c({resolved_path, {p1, P1}, {p2, P2}, {resolved, ResolvedPath}}),
	case ao_store:type(Store, ResolvedPath) of
		not_found -> not_found;
		_ -> read(Store, ResolvedPath)
	end.

read_assignment(Store, ProcID, Slot) when is_integer(Slot) ->
	read_assignment(Store, ProcID, integer_to_list(Slot));
read_assignment(Store, ProcID, Slot) ->
	ResolvedPath =
		P2 = ao_store:resolve(
			Store,
			P1 = ao_store:path(Store, [
				"assignments",
				fmt_id(ProcID),
				Slot
			])
		),
	?c({resolved_path, {p1, P1}, {p2, P2}, {resolved, ResolvedPath}}),
	read(Store, ResolvedPath).

lookup(Store, PathPart) when not is_list(PathPart) ->
	lookup(Store, [PathPart]);
lookup(Store, Path) ->
	?c({looking_up, Path}),
	ResolvedPath =
		P2 = ao_store:resolve(
			Store,
			P1 = ao_store:path(Store, lists:map(fun fmt_id/1, Path))
		),
	?c({resolved_path, P1, P2, ResolvedPath}),
	read(Store, ResolvedPath).

read(Store, RawPath) ->
	?c({reading_message, RawPath, Store}),
	MessagePath = ao_store:path(Store, RawPath),
	case ao_store:type(Store, MessagePath) of
		composite ->
			?c({reading_composite_message, MessagePath}),
			% The message is a bundle and we want the whole item.
			% Read the root and reconstruct it.
			RootPath = ao_store:path(Store, [MessagePath, "item"]),
			Root = read_simple_message(Store, RootPath),
			% The bundle is a map of its children by ID. Reconstruct
			% the bundle by reading each child.
			?no_prod("Reconstructing bundle unnecessarily serializes and deserializes"),
			%% This serialization/deserialization normalizes away the manifest wrapping
			%% tags and object, added by `write_composite/3`. We do this so that we can
			%% separate the bundle items from the main object in storage, but if ar_bundles
			%% exposed a function to rebuild the TX object's manifest we could avoid this.
			ar_bundles:deserialize(
				ar_bundles:normalize(Root#tx{
					data = maps:map(
						fun(_, Key) -> read(Store, ["messages", fmt_id(Key)]) end,
						ar_bundles:parse_manifest(
							maps:get(<<"manifest">>, Root#tx.data)
						)
					)
				})
			);
		simple ->
			read_simple_message(Store, MessagePath);
		not_found ->
			not_found
	end.

read_simple_message(Store, Path) ->
	{ok, Bin} = ao_store:read(Store, Path),
	logger:error("Binary after read: ~p", [Bin]),
	hb_tx_operations:deserialize(Bin).
% ar_bundles:deserialize(Bin).

fmt_id(ID) -> fmt_id(ID, unsigned).
fmt_id(ID, Type) when is_record(ID, tx) -> fmt_id(ar_bundles:id(ID, Type));
fmt_id(ID, _) when is_list(ID) andalso length(ID) == 43 -> ID;
fmt_id(ID, _) when is_binary(ID) andalso byte_size(ID) == 43 -> ID;
fmt_id(ID, _Type) when is_binary(ID) andalso byte_size(ID) == 32 ->
	binary_to_list(ar_util:id(ID));
fmt_id(ID, _Type) ->
	ID.

%%% Tests

%% Helpers
test_cache() ->
	ao_store:reset(?TEST_STORE),
	?TEST_STORE.

create_unsigned_tx(Data) ->
	% normalize(Item) -> reset_ids(normalize_data(Item)).
	%
	Item0 = hb_tx:from_map(
		#{
			format => ans104,
			tags => [{<<"Example">>, <<"Tag">>}],
			data => Data
		}
	),
	Item1 = hb_tx:maybe_update_unsigned_id(hb_tx_operations:build_signature_data(Item0), Item0),
	% Item = Item0,
	% Item = ar_bundles:normalize(Item0),
	% Tx = ar_bundles:normalize(Item),
	% Item.
	% 25
	% 1. calculate data size with ar_bundles:normalize(Item)
	% ?debugFmt("Created TX: ~p", [hb_tx:data_size(Item)]),

	% 2. calculate unsigned_id with signature_for_unsigned_item
	% Item1 = hb_tx:owner(?DEFAULT_OWNER, Item0),

	% SignatureData = hb_tx_operations:build_signature_data(Item1),
	% Item2 = hb_tx:unsigned_id(SignatureData, Item1),
	case hb_tx:has_default_signature(Item1) of
		true -> hb_tx:set_default_id(Item1);
		false -> hb_tx:id(crypto:hash(sha256, hb_tx:signature(Item1)))
	end.

% ORIGINAL
% ar_bundles:normalize(
%     #tx{
%         format = ans104,
%         tags = [{<<"Example">>, <<"Tag">>}],
%         data = Data
%     }
% ).
my_test() ->
	OLD_TX =
		ar_bundles:normalize(
			#tx{
				format = ans104,
				tags = [{<<"Example">>, <<"Tag">>}],
				data = <<"Sample">>
			}
		),
	?debugFmt("\n Old good transaction ~p", [ar_bundles:serialize(OLD_TX)]),
	?debugFmt("\n New good? transaction ~p", [hb_tx_operations:serialize(OLD_TX)]),
	?assertEqual(ar_bundles:serialize(OLD_TX), hb_tx_operations:serialize(OLD_TX)),

	NEW_TX = create_unsigned_tx(<<"Sample">>),
	?debugFmt("\n New transaction ~p", [ar_bundles:serialize(ar_bundles:reset_ids(NEW_TX))]),
	?assertEqual(ar_bundles:serialize(NEW_TX), ar_bundles:serialize(OLD_TX)),

	OLD_TX_AFTERWORDS = hb_tx_operations:deserialize(hb_tx_operations:serialize(OLD_TX)),
	lists:foreach(
		fun(Field) ->
			% logger:error("\n Field: ~p   Item: ~p | Retrieved Item: ~p\n", [Field, hb_tx:Field(Item), hb_tx:Field(RetrievedItem)])
			?assertEqual(hb_tx:Field(OLD_TX), hb_tx:Field(OLD_TX_AFTERWORDS), Field)
		end,
		hb_tx:fields_of_tx()
	),
	?assertEqual(OLD_TX, OLD_TX_AFTERWORDS).

%% Helper function to create signed #tx items.
%% # Item = create_unsigned_tx(<<"Simple unsigned data item">>),
create_signed_tx(Data) ->
	ar_bundles:sign_item(create_unsigned_tx(Data), ar_wallet:new()).

%% Test path resolution dynamics.
simple_path_resolution_test() ->
	ao_store:write(TestStore = test_cache(), "test-file", <<"test-data">>),
	ao_store:make_link(TestStore, "test-file", "test-link"),
	?assertEqual({ok, <<"test-data">>}, ao_store:read(TestStore, "test-link")).

resursive_path_resolution_test() ->
	ao_store:write(TestStore = test_cache(), "test-file", <<"test-data">>),
	ao_store:make_link(TestStore, "test-file", "test-link"),
	ao_store:make_link(TestStore, "test-link", "test-link2"),
	?assertEqual({ok, <<"test-data">>}, ao_store:read(TestStore, "test-link2")).

hierarchical_path_resolution_test() ->
	TestStore = test_cache(),
	ao_store:make_group(TestStore, "test-dir1"),
	ao_store:write(TestStore, ["test-dir1", "test-file"], <<"test-data">>),
	ao_store:make_link(TestStore, ["test-dir1"], "test-link"),
	?assertEqual({ok, <<"test-data">>}, ao_store:read(TestStore, ["test-link", "test-file"])).

%% Test storing and retrieving a simple unsigned item
store_simple_unsigned_item_test() ->
	Item = create_unsigned_tx(<<"Simple unsigned data item">>),
	?debugFmt("[info] Item in test: ~p \n", [hb_tx:unsigned_id(Item)]),
	%% Write the simple unsigned item
	ok = write(TestStore = test_cache(), Item),
	%% Read the item back
	RetrievedItem = read(TestStore, ["messages", fmt_id(Item)]),
	lists:foreach(
		fun(Field) ->
			?debugFmt("\n Field: ~p   Item: ~p | Retrieved Item: ~p\n", [Field, hb_tx:Field(Item), hb_tx:Field(RetrievedItem)]),
			?assertEqual(hb_tx:Field(Item), hb_tx:Field(RetrievedItem), Field)
		end,
		hb_tx:fields_of_tx()
	).
% ?assertEqual(Item, RetrievedItem).
%
old_create_unsigned_tx(Data) ->
	% ORIGINAL
	ar_bundles:normalize(
		#tx{
			format = ans104,
			tags = [{<<"Example">>, <<"Tag">>}],
			data = Data
		}
	).

new_test() ->
	ItemData = #{
		<<"key1">> => create_unsigned_tx(<<"value1">>),
		<<"key2">> => create_unsigned_tx(<<"value2">>)
	},
	?debugFmt("\nItemData: ~p\n", [maps:get(<<"key1">>, ItemData)]),
	UnsignedTX = create_unsigned_tx(ItemData),

	ItemDataOld = #{
		<<"key1">> => old_create_unsigned_tx(<<"value1">>),
		<<"key2">> => old_create_unsigned_tx(<<"value2">>)
	},
	UnsignedTXOld = old_create_unsigned_tx(ItemDataOld),
	?debugFmt("Tx in old format. Data: ~p", [hb_tx:data(UnsignedTXOld)]),
	?debugFmt("Tx in old format. Manifest!: ~p", [hb_tx:manifest(UnsignedTXOld)]),
	?debugFmt("Tx in new format. Data: ~p", [hb_tx:data(UnsignedTX)]),
	?assertEqual(UnsignedTX, UnsignedTXOld).

%% Test storing and retrieving a simple signed item
simple_signed_item_test() ->
	Item = create_signed_tx(<<"Simple signed data item">>),
	%% Write the simple signed item
	ok = write(TestStore = test_cache(), Item),
	%% Read the item back
	RetrievedItemUnsigned = read_message(TestStore, ar_bundles:id(Item, unsigned)),
	RetrievedItemSigned = read_message(TestStore, ar_bundles:id(Item, signed)),
	lists:foreach(
		fun(Field) ->
			?debugFmt("\n Field: ~p   Item: ~p | Retrieved Item: ~p\n", [
				Field, hb_tx:Field(Item), hb_tx:Field(RetrievedItemUnsigned)
			]),
			?assertEqual(hb_tx:Field(Item), hb_tx:Field(RetrievedItemUnsigned), Field)
		end,
		hb_tx:fields_of_tx()
	),
	%% Assert that the retrieved item matches the original and verifies
	% ?assertEqual(Item, RetrievedItemUnsigned),
	?assertEqual(Item, RetrievedItemSigned),
	?assertEqual(true, ar_bundles:verify_item(RetrievedItemSigned)).

%% Test storing and retrieving a composite unsigned item
composite_unsigned_item_test() ->
	ItemData = #{
		<<"key1">> => create_unsigned_tx(<<"value1">>),
		<<"key2">> => create_unsigned_tx(<<"value2">>)
	},
	?debugFmt("\nItemData: ~p\n", [maps:get(<<"key1">>, ItemData)]),
	UnsignedTX = create_unsigned_tx(ItemData),
	?debugMsg("Composite item data done"),
	Item = ar_bundles:deserialize(UnsignedTX),
	?debugMsg("Deserialize composite item done"),
	ok = write(TestStore = test_cache(), UnsignedTX),
	?debugMsg("Write item done"),
	RetrievedItem = read_message(TestStore, ar_bundles:id(UnsignedTX)),
	% ?debugMsg("Read item done"),

	Item1 = maps:get(<<"key1">>, RetrievedItem#tx.data),
	?debugFmt("Item1: ~p", [Item1]),
	% Item1Upd = ar_bundles:update_ids(Item1),
	% ?debugVal(ar_bundles:id(Item1Upd)),

	% ?debugMsg("UnsignedTX item data: \n"),
	% ?debugVal(UnsignedTX#tx.data),
	% ?debugVal(ar_bundles:id(maps:get(<<"key1">>, UnsignedTX#tx.data))).
	?debugVal(RetrievedItem#tx.manifest),
	?assertEqual(
		ar_bundles:id((maps:get(<<"key1">>, Item#tx.data)), unsigned),
		ar_bundles:id((maps:get(<<"key1">>, RetrievedItem#tx.data)), unsigned)
	).
% ?assertEqual(
%     ar_bundles:id((maps:get(<<"key2">>, Item#tx.data)), unsigned),
%     ar_bundles:id((maps:get(<<"key2">>, binary_to_term(RetrievedItem#tx.data))), unsigned)
% ),
% ?assertEqual(
%     ar_bundles:id(Item, unsigned),
%     ar_bundles:id(RetrievedItem, unsigned)
% ).

%% Test storing and retrieving a composite signed item
composite_signed_item_test() ->
	ItemData = #{
		<<"key1">> => create_signed_tx(<<"value1">>),
		<<"key2">> => create_signed_tx(<<"value2">>)
	},
	Item = ar_bundles:deserialize(create_signed_tx(ItemData)),
	ok = write(TestStore = test_cache(), Item),
	RetrievedItem = read_message(TestStore, ar_bundles:id(Item, signed)),
	?assertEqual(
		ar_bundles:id((maps:get(<<"key1">>, Item#tx.data)), unsigned),
		ar_bundles:id((maps:get(<<"key1">>, RetrievedItem#tx.data)), unsigned)
	),
	?assertEqual(
		ar_bundles:id((maps:get(<<"key2">>, Item#tx.data)), signed),
		ar_bundles:id((maps:get(<<"key2">>, RetrievedItem#tx.data)), signed)
	),
	?assertEqual(ar_bundles:id(Item, unsigned), ar_bundles:id(RetrievedItem, unsigned)),
	?assertEqual(ar_bundles:id(Item, signed), ar_bundles:id(RetrievedItem, signed)),
	?assertEqual(true, ar_bundles:verify_item(Item)),
	?assertEqual(true, ar_bundles:verify_item(RetrievedItem)).

%% Test deeply nested item storage and retrieval
deeply_nested_item_test() ->
	%% Create nested data
	DeepValueTx = create_signed_tx(<<"deep_value">>),
	Level3Tx = create_unsigned_tx(#{
		<<"level3_key">> => DeepValueTx
	}),
	Level2Tx = create_signed_tx(#{
		<<"level2_key">> => Level3Tx
	}),
	Level1Tx = create_unsigned_tx(#{
		<<"level1_key">> => Level2Tx
	}),
	%% Write the nested item
	ok = write(TestStore = test_cache(), Level1Tx),
	%% Read the deep value back using subpath
	RetrievedItem = read_message(TestStore, [fmt_id(Level1Tx), "level1_key", "level2_key", "level3_key"]),
	%% Assert that the retrieved item matches the original deep value
	?assertEqual(<<"deep_value">>, RetrievedItem#tx.data),
	?assertEqual(
		ar_bundles:id(DeepValueTx, unsigned),
		ar_bundles:id(RetrievedItem, unsigned)
	).

write_and_read_output_test() ->
	Store = test_cache(),
	Proc = create_signed_tx(#{<<"test-item">> => create_unsigned_tx(<<"test-body-data">>)}),
	Item1 = create_signed_tx(<<"Simple signed output #1">>),
	Item2 = create_signed_tx(<<"Simple signed output #2">>),
	ok = write_output(Store, fmt_id(Proc, signed), 0, Item1),
	ok = write_output(Store, fmt_id(Proc, signed), 1, Item2),
	?assertEqual(Item1, read_message(Store, ar_bundles:id(Item1, unsigned))),
	?assertEqual(Item2, read_message(Store, ar_bundles:id(Item2, unsigned))),
	?assertEqual(Item2, read_output(Store, fmt_id(Proc, signed), 1)),
	?assertEqual(Item1, read_output(Store, fmt_id(Proc, signed), ar_bundles:id(Item1, unsigned))).

latest_output_retrieval_test_broken() ->
	Store = test_cache(),
	Proc = create_signed_tx(#{<<"test-item">> => create_unsigned_tx(<<"test-body-data">>)}),
	Item1 = create_signed_tx(<<"Simple signed output #1">>),
	Item2 = create_signed_tx(<<"Simple signed output #2">>),
	ok = write_output(Store, fmt_id(Proc, signed), 0, Item1),
	ok = write_output(Store, fmt_id(Proc, signed), 1, Item2),
	?assertEqual({1, Item2}, latest(Store, fmt_id(Proc, signed))),
	?assertEqual({0, Item1}, latest(Store, fmt_id(Proc, signed), 1)).
