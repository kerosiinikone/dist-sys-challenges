test-echo:
	cd echo && go build -o ../bin/echo .
	bash ./maelstrom/maelstrom test -w echo --bin ./bin/echo --node-count 1 --time-limit 10

test-unique-ids:
	cd unique_id && go build -o ../bin/unique_id .
	bash ./maelstrom/maelstrom test -w unique-ids --bin ./bin/unique_id --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

serve:
	bash ./maelstrom/maelstrom serve