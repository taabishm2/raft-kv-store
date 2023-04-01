cd /raft-kv-store/
python3 -u -m server.main > /raft-kv-store/logs/logcache/server.out &
tail -f /dev/null
