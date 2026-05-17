{ok, _} = application:ensure_all_started(reckon_db),
io:format("--- nif loaded? ---~n"),
io:format("hash:     ~p~n", [reckon_db_hash_nif:implementation()]),
io:format("crypto:   ~p~n", [reckon_db_crypto_nif:implementation()]),
io:format("archive:  ~p~n", [reckon_db_archive_nif:implementation()]),
io:format("aggregate:~p~n", [reckon_db_aggregate_nif:implementation()]),
io:format("filter:   ~p~n", [reckon_db_filter_nif:implementation()]),
io:format("graph:    ~p~n", [reckon_db_graph_nif:implementation()]),
init:stop().
