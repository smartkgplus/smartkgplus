#!/bin/bash

pgrep java | while read -r pid ; do
    kill -9 $pid
done
