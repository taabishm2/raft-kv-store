cd /raft-kv-store/
python3 -u -m server.main > /raft-kv-store/logs/logcache/server.out 2>&1 &
tail -f /dev/null
