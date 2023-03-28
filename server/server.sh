cd /raft-kv-store/server
# python3 -u -m server
python3 -u -m server > /raft-kv-store/server/raft/logcache/server.out &
tail -f /dev/null
