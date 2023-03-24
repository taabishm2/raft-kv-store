FROM ubuntu:22.04

RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip3 install grpcio grpcio-tools

# RUN python3 -m grpc_tools.protoc --proto_path=. ./kvstore.proto --python_out=. --grpc_python_out=.

COPY *.py /grpc_cache/

CMD ["python3", "-u", "/grpc_cache/server.py"]