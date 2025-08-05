#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Define image name and tag
IMAGE_NAME="xy-spark-rapids-dev"
IMAGE_TAG="1.0.0"

# Get the directory of the script to robustly find the Dockerfile
SCRIPT_DIR=$(dirname "$(realpath "$0")")
CONTEXT_DIR=$(dirname "$SCRIPT_DIR")

echo "Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "Dockerfile location: ${CONTEXT_DIR}/Dockerfile"

# Build the Docker image.
docker build \
    -t "${IMAGE_NAME}:${IMAGE_TAG}" \
    "${CONTEXT_DIR}"

echo "Build complete."
echo "Image available as: ${IMAGE_NAME}:${IMAGE_TAG}"