FROM ubuntu:25.04 AS base

WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y \
  binutils \
  ca-certificates \
  curl \
  iproute2 \
  pkg-config \
  gnupg \
  software-properties-common \
  wget \
  git \
  python-is-python3 \
  cgroup-tools \
  libstdc++-15-dev \
  gcc-15 \
  g++-15

RUN apt update && apt install -y \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-json1.83.0 \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0

RUN apt update && apt install -y \
  libabsl-dev \
  libarchive-dev \
  libbenchmark-dev \
  libbrotli-dev \
  libcap-dev \
  libcgroup-dev \
  libcli11-dev \
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

FROM base AS builder

RUN apt update && apt install -y \
  cmake \
  clang \
  libssl-dev

RUN apt update && apt install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

ARG NPROC=4

RUN apt install -y \
            autoconf automake ca-certificates clang cmake cppcheck \
            curl gcc g++ git libncurses-dev \
            libtool make ninja-build \
            python3 python3-dev python3-numpy \
            python3-pip python3-pytest-xdist python3-setuptools \
            python3-typing-extensions zlib1g-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

# Install protobuf
ARG PROTOBUF_VERSION=21.12
RUN git clone -b v${PROTOBUF_VERSION} --recursive https://github.com/protocolbuffers/protobuf.git \
    && cd protobuf && ./autogen.sh \
    && ./configure --enable-static=no \
    && make -j${NPROC} install && ldconfig \
    && cd .. && rm -rf protobuf

# Clone and build llvm-project and run tests
ARG BUILD_SHARED_LIBS=OFF
RUN git clone -n https://github.com/llvm/llvm-project.git \
    && cd llvm-project \
    && git checkout 43d71baae36c8d8b5a9995aa35efebe09cc9c2d6 \
    && mkdir -p build && cd build \
# Build with clang since gcc on ppc64le doesn't support __builtin_thread_pointer
    && CC=clang CXX=clang++ \
       cmake -G Ninja ../llvm \
             -DLLVM_ENABLE_PROJECTS=mlir \
             -DLLVM_TARGETS_TO_BUILD="host" \
             -DCMAKE_BUILD_TYPE=Release \
             -DLLVM_ENABLE_ASSERTIONS=ON \
             -DLLVM_ENABLE_RTTI=ON \
             -DLLVM_ENABLE_LIBEDIT=OFF \
             -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS} \
    && cmake --build . --parallel ${NPROC} -- ${MAKEFLAGS} \
    && (cmake --build . --parallel ${NPROC} --target check-mlir || \
        [ "$(uname -m)" = "s390x" ]) \
    && rm -rf /tmp/* \
    && echo "llvm-project commit $(git rev-parse HEAD) successfully built"

RUN git clone --recursive https://github.com/onnx/onnx-mlir.git \
    && cd onnx-mlir/third_party/onnx \
    && CMAKE_ARGS="-DCMAKE_INSTALL_LIBDIR=lib -DONNX_USE_PROTOBUF_SHARED_LIBS=ON" python3 -m pip install --break-system-packages .

RUN  LLVM_PROJECT_ROOT=$(pwd)/llvm-project \
# Install llc, opt, and libraries
    && cp ${LLVM_PROJECT_ROOT}/build/bin/llc /usr/local/bin \
    && cp ${LLVM_PROJECT_ROOT}/build/bin/opt /usr/local/bin \
    && cp -a ${LLVM_PROJECT_ROOT}/build/lib/* /usr/local/lib \
# Build onnx-mlir, run tests, and install
    && cd onnx-mlir \
    && rm -rf build && mkdir -p build && cd build \
# NNPA acclerator is built on all archs to enable lit tests
# (dependent libzdnn is built on s390x only)
    && CC=clang CXX=clang++ \
       cmake -DMLIR_DIR=${LLVM_PROJECT_ROOT}/build/lib/cmake/mlir \
             -DCMAKE_BUILD_TYPE=Release \
             -DLLVM_ENABLE_ASSERTIONS=ON \
             -DCMAKE_INSTALL_MESSAGE=NEVER .. \
    && cmake --build . -j${NPROC}

RUN cp onnx-mlir/build/Release/bin/onnx-mlir /usr/local/bin && cp onnx-mlir/build/Release/lib/* /usr/local/lib/

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"
RUN rustup toolchain install 1.85.0-x86_64-unknown-linux-gnu

ENV TRIEDB_TARGET=triedb_driver

# Builder
COPY . .
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell -DQUILL_ACTIVE_LOG_LEVEL=QUILL_LOG_LEVEL_DEBUG" \
    CC=gcc-15 CXX=g++-15 cargo build --release --bin monad-rpc && \
    mv target/release/monad-rpc rpc && \
    cp `ls -Lt $(find target/release | grep -e "libquill.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libkeccak.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libsecp256k1.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libsilkpre.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libmonad_rpc.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so") | awk -F/ '!seen[$NF]++'` .

# Debug runner
FROM base AS runner-debug

RUN apt install -y clang

WORKDIR /usr/src/monad-bft
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

COPY --from=builder /usr/src/monad-bft/rpc /usr/local/bin/monad-rpc
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib
COPY --from=builder /usr/local/lib/libprotobuf.* /usr/local/lib
COPY --from=builder /usr/local/lib/libprotobuf.* /usr/local/lib

COPY --from=builder /usr/src/monad-bft/onnx-mlir/build/Release/bin/onnx-mlir /usr/local/bin 
COPY --from=builder /usr/src/monad-bft/onnx-mlir/build/Release/lib/* /usr/local/lib/

COPY --from=builder /usr/local/bin/opt /usr/local/bin 
COPY --from=builder /usr/local/bin/llc /usr/local/bin 

# Runner
FROM runner-debug AS runner

RUN strip \
    /usr/local/bin/monad-rpc \
    /usr/local/lib/*.so \
    /usr/local/lib/*.so.*
