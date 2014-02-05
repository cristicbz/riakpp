FROM ubuntu:13.10

MAINTAINER Marius Cobzarenco <marius@reinfer.io>

RUN apt-get update
RUN apt-get install -y wget

# Install clang-3.5
RUN wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key|sudo apt-key add -
RUN echo "" >> /etc/apt/sources.list
RUN echo "deb http://llvm.org/apt/saucy/ llvm-toolchain-saucy main" >> /etc/apt/sources.list
RUN echo "deb-src http://llvm.org/apt/saucy/ llvm-toolchain-saucy main" >> /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y clang-3.5 git
ENV CC clang
ENV CXX clang++

RUN mkdir /src

RUN apt-get install -y libboost-program-options-dev
RUN cd /src &&  git clone https://github.com/reinferio/riakpp.git
RUN cd /src/riakpp && mkdir build && cd build && cmake .. && make -j4

RUN /src/riakpp/build/test/unittests
