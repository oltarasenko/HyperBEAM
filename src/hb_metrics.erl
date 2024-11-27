-module(hb_metrics).

-export([event/1]).

event({Metric, Label}) ->
	% The operation below takes something like 30 microseconds, so it's included
	% as it is to metrics evaluation. Otherwise a better approach is to declare
	% metrics once only using some sort of setup.
	prometheus_counter:declare([{name, Metric}, {help, ""}, {labels, [operation]}]),
	prometheus_counter:inc(Metric, [Label]).
