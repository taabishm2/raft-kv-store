FROM ubuntu:22.04

# RUN apt-get update
RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
    protobuf-compiler
RUN apt-get install -y python3 python3-pip
RUN pip3 install numpy
RUN pip3 install grpcio grpcio-tools
RUN pip3 install protobuf==3.20.*

COPY *.py /server/
COPY numstore.proto /server/
WORKDIR /server/

RUN protoc -I=. --python_out=. ./numstore.proto 
RUN python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. numstore.proto


CMD ["python3", "-u", "./server.py"]
# CMD ["python3", "-m", protoc -I. --python_out=./grpc_compiled --grpc_python_out=./grpc_compiled image_transform.proto
