#!/bin/bash
set -e

# Get UID/GID from environment or default to 1000
HOST_UID=${HOST_UID:-1000}
HOST_GID=${HOST_GID:-1000}

# Create group if it doesn't exist
if ! getent group ${HOST_GID} >/dev/null; then
    groupadd -g ${HOST_GID} builder
fi

# Create user if it doesn't exist
if ! id -u ${HOST_UID} >/dev/null 2>&1; then
    useradd -u ${HOST_UID} -g ${HOST_GID} -m builder
fi

export CARGE_HOME=/home/builder/.cargo
export RUSTUP_HOME=/home/builder/.rustup

# Set ownership of the workdir
chown -R ${HOST_UID}:${HOST_GID} /app

# Switch to new user and execute the CMD
exec su builder -c "$@"
