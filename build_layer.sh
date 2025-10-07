#!/usr/bin/env bash
set -e
# builds into ./layer/python/lib/python3.11/site-packages
rm -rf layer/python
mkdir -p layer/python/lib/python3.11/site-packages

echo "Building Pillow into layer/python/ (docker required)..."

docker run --rm -v "$(pwd)/layer":/asset public.ecr.aws/sam/build-python3.11:latest \
  /bin/bash -lc "python3 -m pip install Pillow -t /asset/python/lib/python3.11/site-packages && chmod -R a+rx /asset"

echo "Zipping layer..."
cd layer
zip -r ../pillow-layer.zip python
cd ..
mv pillow-layer.zip layer/
echo "Layer created at layer/pillow-layer.zip"