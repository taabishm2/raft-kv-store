# build grpc files
python3 -m grpc_tools.protoc --proto_path=. ./kvstore.proto --python_out=. --grpc_python_out=.
python3 -m grpc_tools.protoc --proto_path=. ./raft.proto --python_out=./raft --grpc_python_out=./raft
