#!/bin/bash

DARTBOARD=$(dirname $0)/../../../dartboard
kubectl --kubeconfig ${KUBECONFIG?:KUBECONFIG must be set} config view --raw > k6.yaml
# gsed -E 's;server: .*;server: https://10.43.0.1;' -i k6.yaml

cat > setup.yaml <<EOF
$(kubectl create secret generic k6-kubeconfig --from-file=k6.yaml --dry-run=client -o yaml)
---
$(kubectl create configmap k6-generic --from-file=${DARTBOARD}/k6/generic --dry-run=client -o yaml)
---
$(kubectl create configmap k6-lib --from-file=${DARTBOARD}/k6/lib --dry-run=client -o yaml)
---
$(kubectl create configmap k6-tests --from-file=${DARTBOARD}/k6/tests --dry-run=client -o yaml)
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: k6
spec:
  replicas: 1
  revisionHistoryLimit: 1
  selector:
    matchLabels:
      app: k6
  # strategy:
    # type: Recreate
  template:
    metadata:
      creationTimestamp: null
      labels:
        app: k6
    spec:
      containers:
      - name: k6
        image: grafana/k6
        command: ["/bin/sh", "-c", "exec sleep infinity"]
        env:
        #- name: HTTP_PROXY
        #  value: http://localhost:8888
        #- name: HTTPS_PROXY
        #  value: http://localhost:8888
        #- name: NO_PROXY
        #  value: jslib.k6.io
        volumeMounts:
        - name: k6-kubeconfig
          mountPath: /home/k6/
          readOnly: true
        - name: k6-generic-volume
          mountPath: /home/k6/k6/generic
        - name: k6-lib-volume
          mountPath: /home/k6/k6/lib
        - name: k6-tests-volume
          mountPath: /home/k6/k6/tests
      volumes:
      - name: k6-kubeconfig
        secret:
          secretName: k6-kubeconfig
      - name: k6-generic-volume
        configMap:
          name: k6-generic
      - name: k6-lib-volume
        configMap:
          name: k6-lib
      - name: k6-tests-volume
        configMap:
          name: k6-tests
EOF
kubectl apply -f setup.yaml
