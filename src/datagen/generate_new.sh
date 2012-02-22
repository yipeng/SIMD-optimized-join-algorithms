#!/bin/sh

echo "Generating build side..."
python genbuild_new.py > 016K_build.tbl
echo "Generating probe side..."
python genprobe_new.py > 256K_probe.tbl
