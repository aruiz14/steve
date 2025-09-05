#!/bin/bash

set -euo pipefail

STEVE_SERVERS=$(kubectl get pods -l app=steve -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}' | awk 'NR > 1 {printf ","} {printf "http://%s:9080", $0} END {print ""}')
CONTEXT=$(kubectl config current-context)

kubectl exec -it k6-0 -- k6 run -e STEVE_SERVERS=${STEVE_SERVERS} -e KUBE_SERVERS=https://10.43.0.1 -e KUBECONFIG=/home/k6/k6.yaml -e CONTEXT=${CONTEXT} -e TOKEN=notneeded -e CONFIG_MAP_COUNT=500 -e VUS=300 -e CHANGE_API=kube -e WATCH_API=steve -e CHANGE_RATE=70 -e WATCH_DURATION=180 -e NAMESPACE=watch-tests ./k6/tests/steve_watch_benchmark.js
