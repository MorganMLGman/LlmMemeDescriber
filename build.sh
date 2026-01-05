#!/usr/bin/env bash
set -euo pipefail
IMAGE_NAME=${1:-llm-meme-describer}
TAG=${2:-latest}
PLATFORMS=${3:-linux/amd64,linux/arm64}
CACHE_MODE=${4:-local} # local or registry
CACHE_REF=${5:-}       # when using registry cache provide <repo>/cache:tag
PUSH=${6:-false}

echo "Building multi-arch image ${IMAGE_NAME}:${TAG} for ${PLATFORMS}"

# Create builder if missing
if ! docker buildx ls | grep -q multiarch-builder; then
  docker buildx create --use --name multiarch-builder
else
  docker buildx use multiarch-builder
fi

# Ensure QEMU emulation
docker run --rm --privileged tonistiigi/binfmt --install all

# Enable BuildKit
export DOCKER_BUILDKIT=1

CACHE_OPTIONS=""
if [ "$CACHE_MODE" = "local" ]; then
  CACHE_DIR="$(pwd)/.buildx-cache"
  CACHE_OPTIONS="--cache-to=type=local,dest=$CACHE_DIR,mode=max --cache-from=type=local,src=$CACHE_DIR"
elif [ "$CACHE_MODE" = "registry" ] && [ -n "$CACHE_REF" ]; then
  CACHE_OPTIONS="--cache-to=type=registry,ref=$CACHE_REF,mode=max --cache-from=type=registry,ref=$CACHE_REF"
fi

if [ "$PUSH" = "true" ]; then
  docker buildx build --platform $PLATFORMS -t "$IMAGE_NAME:$TAG" $CACHE_OPTIONS --push .
else
  docker buildx build --platform $PLATFORMS -t "$IMAGE_NAME:$TAG" $CACHE_OPTIONS --load .
fi

echo "Done."
