#!/bin/bash

MESSAGE="ping"

RESPONSE=$(echo "$MESSAGE" | docker run --rm -i \
  --network tp0_testing_net \
  alpine \
  sh -c "nc -w 3 server 12345")

if [ "$RESPONSE" = "$MESSAGE" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi
