pip3 install grpcio
pip3 install grpcio-tools

python3 -m grpc_tools.protoc --python_out=. --grpc_python_out=. -I. object_store.proto

