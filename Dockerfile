# IMPORTANT: Changes in this file do not automatically affect the Docker image used by the CI server.
# You need to build and push it manually, see the wiki for details:
# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:22.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        clang-9 \
        clang-13 \
        clang-format-9 \
        clang-format-13 \
        clang-tidy-9 \
        clang-tidy-13 \
        cmake \
        curl \
        dos2unix \
        g++-9 \
        g++-11 \
        gcc-9 \
        gcc-11 \
        gcovr \
        git \
        graphviz \
        libboost-all-dev \
        libhwloc-dev \
        libncurses5-dev \
        libnuma-dev \
        libnuma1 \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        lld \
        lsb-release \
        man \
        parallel \
        postgresql-server-dev-all \
        python3 \
        python3-pip \
        software-properties-common \
        sudo \
        systemtap \
        systemtap-sdt-dev \
        valgrind \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer \
    && pip3 install scipy pandas matplotlib # preload large Python packages (installs numpy and others)

ENV OPOSSUM_HEADLESS_SETUP=true
