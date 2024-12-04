-module(hb_tx).

-define(DEFAULT_SIG, <<0:4096>>).
-define(DEFAULT_ID, <<0:256>>).
-define(DEFAULT_OWNER, <<0:4096>>).

-export([new/0, from_map/1, fields_of_tx/0]).

%% Exported Functions
-export([
	format/1, format/2,
	id/1, id/2,
	unsigned_id/1, unsigned_id/2,
	last_tx/1, last_tx/2,
	owner/1, owner/2,
	tags/1, tags/2,
	target/1, target/2,
	quantity/1, quantity/2,
	data/1, data/2,
	manifest/1, manifest/2,
	data_size/1, data_size/2,
	data_tree/1, data_tree/2,
	data_root/1, data_root/2,
	signature/1, signature/2,
	reward/1, reward/2,
	denomination/1, denomination/2,
	signature_type/1, signature_type/2
]).

-export([set_default_id/1]).
-export([has_default_signature/1]).
-export([maybe_update_unsigned_id/2]).

%% @doc A transaction.
-record(tx, {
	%% 1 or 2 or ans104.
	format = ans104,
	%% The transaction identifier.
	id = ?DEFAULT_ID,
	unsigned_id = ?DEFAULT_ID,
	%% Either the identifier of the previous transaction from
	%% the same wallet or the identifier of one of the
	%% last ?MAX_TX_ANCHOR_DEPTH blocks.
	last_tx = <<>>,
	%% The public key the transaction is signed with.
	owner = ?DEFAULT_OWNER,
	%% A list of arbitrary key-value pairs. Keys and values are binaries.
	tags = [],
	%% The address of the recipient, if any. The SHA2-256 hash of the public key.
	target = <<>>,
	%% The amount of Winstons to send to the recipient, if any.
	quantity = 0,
	%% The data to upload, if any. For v2 transactions, the field is optional - a fee
	%% is charged based on the "data_size" field, data itself may be uploaded any time
	%% later in chunks.
	data = <<>>,
	manifest = undefined,
	%% Size in bytes of the transaction data.
	data_size = 0,
	%% Deprecated. Not used, not gossiped.
	data_tree = [],
	%% The Merkle root of the Merkle tree of data chunks.
	data_root = <<>>,
	%% The signature.
	signature = ?DEFAULT_SIG,
	%% The fee in Winstons.
	reward = 0,

	%% The code for the denomination of AR in base units.
	%%
	%% 1 corresponds to the original denomination of 1^12 base units.
	%% Every time the available supply falls below ?REDENOMINATION_THRESHOLD,
	%% the denomination is multiplied by 1000, the code is incremented.
	%%
	%% 0 is the default denomination code. It is treated as the denomination code of the
	%% current block. We do NOT default to 1 because we want to distinguish between the
	%% transactions with the explicitly assigned denomination (the denomination then becomes
	%% a part of the signature preimage) and transactions signed the way they were signed
	%% before the upgrade. The motivation is to keep supporting legacy client libraries after
	%% redenominations and at the same time protect users from an attack where
	%% a post-redenomination transaction is included in a pre-redenomination block. The attack
	%% is prevented by forbidding inclusion of transactions with denomination=0 in the 100
	%% blocks preceding the redenomination block.
	%%
	%% Transaction denomination code must not exceed the block's denomination code.
	denomination = 0,

	%% The type of signature this transaction was signed with. A system field,
	%% not used by the protocol yet.
	signature_type = {rsa, 65537}
}).

from_map(Map) ->
	maps:fold(
		fun
			(id, Value, TX) -> id(Value, TX);
			(unsigned_id, Value, TX) -> unsigned_id(Value, TX);
			(format, Value, TX) -> format(Value, TX);
			(last_tx, Value, TX) -> last_tx(Value, TX);
			(owner, Value, TX) -> owner(Value, TX);
			(tags, Value, TX) -> tags(Value, TX);
			(target, Value, TX) -> target(Value, TX);
			(quantity, Value, TX) -> quantity(Value, TX);
			(data, Value, TX) -> data(Value, TX);
			(manifest, Value, TX) -> manifest(Value, TX);
			(data_size, Value, TX) -> data_size(Value, TX);
			(data_tree, Value, TX) -> data_tree(Value, TX);
			(data_root, Value, TX) -> data_root(Value, TX);
			(signature, Value, TX) -> signature(Value, TX);
			(reward, Value, TX) -> reward(Value, TX);
			(denomination, Value, TX) -> denomination(Value, TX);
			(signature_type, Value, TX) -> signature_type(Value, TX)
		end,
		new(),
		Map
	).

new() -> #tx{}.
%% ID
id(TX) -> TX#tx.id.
id(Value, TX) -> TX#tx{id = Value}.

set_default_id(TX) -> id(?DEFAULT_ID, TX).
has_default_signature(TX) -> signature(TX) == ?DEFAULT_SIG.

%% Unsigned ID
unsigned_id(TX) -> TX#tx.unsigned_id.
unsigned_id(Value, TX) -> TX#tx{unsigned_id = Value}.

%% Last TX
last_tx(TX) -> TX#tx.last_tx.
last_tx(Value, TX) -> TX#tx{last_tx = Value}.

%% Owner
owner(TX) -> TX#tx.owner.
owner(Value, TX) -> TX#tx{owner = Value}.

%% Tags
tags(TX) -> TX#tx.tags.
tags(Value, TX) -> TX#tx{tags = Value}.

%% Target
target(TX) -> TX#tx.target.
target(Value, TX) -> TX#tx{target = Value}.

%% Quantity
quantity(TX) -> TX#tx.quantity.
quantity(Value, TX) -> TX#tx{quantity = Value}.

%% Data
data(TX) -> TX#tx.data.
data(Value, TX) when is_binary(Value) ->
	TX#tx{data = Value, data_size = byte_size(Value)};
data(Value, TX) ->
	TX#tx{data = Value}.

%% Manifest
manifest(TX) -> TX#tx.manifest.
manifest(Value, TX) -> TX#tx{manifest = Value}.

%% Data Size
data_size(TX) -> TX#tx.data_size.
data_size(Value, TX) -> TX#tx{data_size = Value}.

%% Data Tree
data_tree(TX) -> TX#tx.data_tree.
data_tree(Value, TX) -> TX#tx{data_tree = Value}.

%% Data Root
data_root(TX) -> TX#tx.data_root.
data_root(Value, TX) -> TX#tx{data_root = Value}.

%% Signature
signature(TX) -> TX#tx.signature.
signature(Value, TX) -> TX#tx{signature = Value}.

%% Reward
reward(TX) -> TX#tx.reward.
reward(Value, TX) -> TX#tx{reward = Value}.

%% Denomination
denomination(TX) -> TX#tx.denomination.
denomination(Value, TX) -> TX#tx{denomination = Value}.

%% Signature Type
signature_type(TX) -> TX#tx.signature_type.
signature_type(Value, TX) -> TX#tx{signature_type = Value}.

format(TX) -> TX#tx.format.
format(Value, TX) -> TX#tx{format = Value}.

maybe_update_unsigned_id(Value, TX) ->
	case unsigned_id(TX) == ?DEFAULT_ID of
		true -> unsigned_id(Value, TX);
		false -> TX
	end.

fields_of_tx() ->
	record_info(fields, tx).
