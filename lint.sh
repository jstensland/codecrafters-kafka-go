#! /bin/bash

# This script takes a file path as input, strips off the file it self to run the "golangci-lint run"
# on the entire directory
#
#
# For examples. if you run
#
# ./lint.sh my/file/path/file.go
#
# The script will run
#
# golangci-lint run my/file/path/...

# Check if a file path was provided
if [ -z "$1" ]; then
  echo "Usage: $0 <file_path>"
  exit 1
fi

# Get the directory path from the file path
DIR_PATH=$(dirname "$1")

# Run golangci-lint on the directory
echo "Running golangci-lint run ${DIR_PATH}"
golangci-lint run "${DIR_PATH}"
