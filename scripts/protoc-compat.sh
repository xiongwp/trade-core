#!/bin/sh

if [ "$1" = "--version" ]; then
    echo "libprotoc 3.21.12"
    exit 0
fi

unset PROTOC
exec protoc "$@"
