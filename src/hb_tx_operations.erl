-module(hb_tx_operations).
-compile(export_all).

serialize(TX) ->
	Data = hb_tx:data(TX),
	EncodedTags = encode_tags(hb_tx:tags(TX)),
	<<
		(encode_signature_type(hb_tx:signature_type(TX)))/binary,
		(hb_tx:signature(TX))/binary,
		(hb_tx:owner(TX))/binary,
		(encode_optional_field(hb_tx:target(TX)))/binary,
		(encode_optional_field(hb_tx:last_tx(TX)))/binary,
		(encode_tags_size(hb_tx:tags(TX), EncodedTags))/binary,
		EncodedTags/binary,
		Data/binary
	>>.

deserialize(Binary) ->
	{SignatureType, Signature, Owner, Rest} = decode_signature(Binary),
	{Target, Rest2} = decode_optional_field(Rest),
	{Anchor, Rest3} = decode_optional_field(Rest2),
	{Tags, Data} = decode_tags(Rest3),
	TX0 = hb_tx:from_map(#{
		format => ans104,
		signature_type => SignatureType,
		signature => Signature,
		owner => Owner,
		target => Target,
		last_tx => Anchor,
		tags => Tags,
		data => Data,
		data_size => byte_size(Data)
	}),
	% Maybe set unsigned id (if it's default use the build function)
	TX1 = hb_tx:unsigned_id(build_signature_data(TX0), TX0),
	% Maybe set id based on the signature ?DEFAULT or crypto:hash(sha256, Sig)
	case hb_tx:has_default_signature(TX1) of
		true -> hb_tx:set_default_id(TX1);
		false -> hb_tx:id(crypto:hash(sha256, hb_tx:signature(TX1)), TX1)
	end.

%% @doc Decode tags from a binary format using Apache Avro.
decode_tags(<<0:64/little-integer, 0:64/little-integer, Rest/binary>>) ->
	{[], Rest};
decode_tags(<<_TagCount:64/little-integer, _TagSize:64/little-integer, Binary/binary>>) ->
	{Count, BlocksBinary} = decode_zigzag(Binary),
	{Tags, Rest} = decode_avro_tags(BlocksBinary, Count),
	%% Pull out the terminating zero
	{0, Rest2} = decode_zigzag(Rest),
	{Tags, Rest2}.

%% @doc Decode Avro blocks (for tags) from binary.
decode_avro_tags(<<>>, _) ->
	{[], <<>>};
decode_avro_tags(Binary, Count) when Count =:= 0 ->
	{[], Binary};
decode_avro_tags(Binary, Count) ->
	{NameSize, Rest} = decode_zigzag(Binary),
	decode_avro_name(NameSize, Rest, Count).

decode_avro_name(0, Rest, _) ->
	{[], Rest};
decode_avro_name(NameSize, Rest, Count) ->
	<<Name:NameSize/binary, Rest2/binary>> = Rest,
	{ValueSize, Rest3} = decode_zigzag(Rest2),
	decode_avro_value(ValueSize, Name, Rest3, Count).

decode_avro_value(0, _, Rest, _) ->
	{[], Rest};
decode_avro_value(ValueSize, Name, Rest, Count) ->
	<<Value:ValueSize/binary, Rest2/binary>> = Rest,
	{DecodedTags, NonAvroRest} = decode_avro_tags(Rest2, Count - 1),
	{[{Name, Value} | DecodedTags], NonAvroRest}.

%% @doc Decode a VInt encoded ZigZag integer from binary.
decode_zigzag(Binary) ->
	{ZigZag, Rest} = decode_vint(Binary, 0, 0),
	case ZigZag band 1 of
		1 -> {-(ZigZag bsr 1) - 1, Rest};
		0 -> {ZigZag bsr 1, Rest}
	end.

decode_vint(<<>>, Result, _Shift) ->
	{Result, <<>>};
decode_vint(<<Byte, Rest/binary>>, Result, Shift) ->
	VIntPart = Byte band 16#7F,
	NewResult = Result bor (VIntPart bsl Shift),
	case Byte band 16#80 of
		0 -> {NewResult, Rest};
		_ -> decode_vint(Rest, NewResult, Shift + 7)
	end.

decode_optional_field(<<0, Rest/binary>>) ->
	{<<>>, Rest};
decode_optional_field(<<1:8/integer, Field:32/binary, Rest/binary>>) ->
	{Field, Rest}.

%% @doc Decode the signature from a binary format. Only RSA 4096 is currently supported.
%% Note: the signature type '1' corresponds to RSA 4096 - but it is is written in
%% little-endian format which is why we match on <<1, 0>>.
decode_signature(<<1, 0, Signature:512/binary, Owner:512/binary, Rest/binary>>) ->
	{{rsa, 65537}, Signature, Owner, Rest};
decode_signature(Other) ->
	unsupported_tx_format.

build_signature_data(TX) ->
	SignatureData = ar_deep_hash:hash([
		utf8_encoded("dataitem"),
		utf8_encoded("1"),
		%% Only SignatureType 1 is supported for now (RSA 4096)
		utf8_encoded("1"),
		hb_tx:owner(TX),
		hb_tx:target(TX),
		hb_tx:last_tx(TX),
		% Probably we can just use term_to_binary here, instead of
		% the complex tags encodings..
		encode_tags((hb_tx:tags(TX))),
		maybe_convert_to_binary(hb_tx:data(TX))
	]),
	crypto:hash(sha256, SignatureData).

encode_tags_size([], <<>>) ->
	<<0:64/little-integer, 0:64/little-integer>>;
encode_tags_size(Tags, EncodedTags) ->
	<<(length(Tags)):64/little-integer, (byte_size(EncodedTags)):64/little-integer>>.

%% @doc Only RSA 4096 is currently supported.
%% Note: the signature type '1' corresponds to RSA 4096 -- but it is is written in
%% little-endian format which is why we encode to <<1, 0>>.
encode_signature_type({rsa, 65537}) ->
	<<1, 0>>;
encode_signature_type(_) ->
	unsupported_tx_format.

%% @doc Encode an optional field (target, anchor) with a presence byte.
encode_optional_field(<<>>) ->
	<<0>>;
encode_optional_field(Field) ->
	<<1:8/integer, Field/binary>>.

%% @doc Encode a UTF-8 string to binary.
utf8_encoded(String) ->
	unicode:characters_to_binary(String, utf8).

maybe_convert_to_binary(Term) when is_binary(Term) -> Term;
maybe_convert_to_binary(Term) -> term_to_binary(Term).

%% @doc Encode tags into a binary format using Apache Avro.
encode_tags([]) ->
	<<>>;
encode_tags(Tags) ->
	EncodedBlocks = lists:flatmap(
		fun({Name, Value}) ->
			EncName = encode_avro_string(Name),
			EncValue = encode_avro_string(Value),
			[EncName, EncValue]
		end,
		Tags
	),
	TagCount = length(Tags),
	ZigZagCount = encode_zigzag(TagCount),
	<<ZigZagCount/binary, (list_to_binary(EncodedBlocks))/binary, 0>>.

encode_avro_string(String) ->
	StringBytes = unicode:characters_to_binary(String, utf8),
	Length = byte_size(StringBytes),
	<<(encode_zigzag(Length))/binary, StringBytes/binary>>.

%% @doc Encode an integer using ZigZag encoding.
encode_zigzag(Int) when Int >= 0 ->
	encode_vint(Int bsl 1);
encode_zigzag(Int) ->
	encode_vint(Int bsl 1, -1).

%% @doc Encode a ZigZag integer to VInt binary format.
encode_vint(ZigZag) ->
	encode_vint(ZigZag, []).

encode_vint(0, Acc) ->
	list_to_binary(lists:reverse(Acc));
encode_vint(ZigZag, Acc) ->
	VIntByte = ZigZag band 16#7F,
	ZigZagShifted = ZigZag bsr 7,
	case ZigZagShifted of
		0 -> encode_vint(0, [VIntByte | Acc]);
		_ -> encode_vint(ZigZagShifted, [VIntByte bor 16#80 | Acc])
	end.
