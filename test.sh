#!/usr/bin/env sh
touch test.sqlite
rm test.sqlite
cat test.ndjson | node import.js
node query.js foo taco