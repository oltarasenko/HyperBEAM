-module(ao_store).
-export([behavior_info/1]).
-export([start/1, stop/1, reset/1]).
-export([type/2, read/2, write/3, list/2]).
-export([path/2, add_path/3]).
-export([make_group/2, make_link/3, resolve/2]).
-include_lib("eunit/include/eunit.hrl").
%%% A simple abstraction layer for AO key value store operations.
%%% This interface allows us to swap out the underlying store
%%% implementation(s) later, or even at the configuration level if desired.
%%% It takes a list of modules and their options, and calls the appropriate
%%% function on the first module that succeeds. If all modules fail, it returns
%%% {error, no_viable_store}.

behavior_info(callbacks) ->
	[
		{start, 1},
		{stop, 1},
		{reset, 1},
		{make_group, 2},
		{make_link, 3},
		{type, 2},
		{read, 2},
		{write, 3},
		{list, 2},
		{path, 2},
		{add_path, 3}
	].

%%% Library wrapper implementations.

start(Modules) -> call_all(Modules, start, []).

stop(Modules) -> call_function(Modules, stop, []).

read(Modules, Key) -> call_function(Modules, read, [Key]).

write(Modules, Key, Value) -> call_function(Modules, write, [Key, Value]).

make_group(Modules, Path) -> call_function(Modules, make_group, [Path]).

make_link(Modules, Existing, New) -> call_function(Modules, make_link, [Existing, New]).

reset(Modules) -> call_function(Modules, reset, []).

type(Modules, Path) -> call_function(Modules, type, [Path]).

path(Modules, Path) -> call_function(Modules, path, [Path]).

add_path(Modules, Path1, Path2) -> call_function(Modules, add_path, [Path1, Path2]).

resolve(Modules, Path) -> call_function(Modules, resolve, [Path]).

list(Modules, Path) -> call_function(Modules, list, [Path]).

%% @doc Call a function on the first module that succeeds.
call_function(X, _Function, _Args) when not is_list(X) ->
	call_function([X], _Function, _Args);
call_function([], _Function, _Args) ->
	not_found;
call_function([{Mod, Opts} | Rest], Function, Args) ->
	try apply(Mod, Function, [Opts | Args]) of
		not_found ->
			logger:error("Not found: ~p", [{Mod, Function, Opts, Args}]),
			call_function(Rest, Function, Args);
		Result ->
			Result
	catch
		Error:Reason:Bt ->
			logger:error("~p", [{Error, Reason, Bt}]),
			% ?debugFmt("Error: ~p", [{Error, Reason, Bt}]),
			call_function(Rest, Function, Args)
	end.

%% @doc Call a function on all given modules.
call_all(X, _Function, _Args) when not is_list(X) ->
	call_all([X], _Function, _Args);
call_all([], _Function, _Args) ->
	ok;
call_all([{Mod, Opts} | Rest], Function, Args) ->
	try
		apply(Mod, Function, [Opts | Args])
	catch
		Error:Reason:Bt ->
			logger:error("~p", [{Error, Reason, Bt}]),
			ok
	end,
	call_all(Rest, Function, Args).
