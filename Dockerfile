FROM ubuntu:14.04

MAINTAINER Marius Cobzarenco <marius@reinfer.io>

# Install clang
RUN apt-get update && \
    apt-get install -y build-essential make cmake clang git
ENV CC clang
ENV CXX clang++

RUN mkdir /src
RUN apt-get install -y protobuf-compiler libprotobuf-dev libprotoc-dev
RUN apt-get install -y libboost-dev libboost-program-options-dev libboost-system-dev
RUN cd /src && \
    git clone https://github.com/reinferio/riakpp.git
RUN cd /src/riakpp && \
    mkdir build && \
    cd build && \
    cmake -DBUILD_TESTS=on .. && \
    make

RUN /src/riakpp/build/test/unittests
