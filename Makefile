test-echo:
	cd echo && go build -o ../bin/echo .
	git-bash ./maelstrom/maelstrom test -w echo --bin ./bin/echo --node-count 1 --time-limit 10

serve:
	git-bash ./maelstrom/maelstrom serve