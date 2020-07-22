FROM ubuntu:20.04

# Assume defaults for all installs with noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# General upgrades and common build deps
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y \
    software-properties-common  \
    build-essential             \
    git                         \
    python3.8                   \
    python3.8-dev               \
    python3-pip

# Update python and pip callables
RUN                                             \
    ln -sf /usr/bin/python3.8 /usr/bin/python &&  \
    ln -sf /usr/bin/python3.8 /usr/bin/python3 && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Upgrade pip version
RUN pip install --upgrade pip

# Copy library
COPY . /actk/

# Install library
RUN pip install numpy==1.19.1 Cython==0.29.21
RUN pip install /actk/
