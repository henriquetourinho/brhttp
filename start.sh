#!/bin/bash
exec 2>&1
echo "Iniciando brhttp..."
strace -e trace=network ./brhttp-linux-amd64 --port=5571 --dir=www 2>&1 | head -100
