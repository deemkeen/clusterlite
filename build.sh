#!/bin/bash

CGO_ENABLED=1 GOOS=linux go build -a -ldflags='-w -s -extldflags "-static"' -o clusterlite
