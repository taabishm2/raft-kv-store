FROM ubuntu:22.04

RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip3 install grpcio grpcio-tools

COPY *.py /grpc_cache/

CMD ["python3", "-u", "/grpc_cache/server.py"]
