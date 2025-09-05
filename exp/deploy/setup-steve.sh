#!/bin/bash

kubectl --kubeconfig ${KUBECONFIG?:KUBECONFIG must be set} config view --raw > rke2.yaml

kubectl create serviceaccount steve && kubectl create clusterrolebinding steve-clusteradmin --clusterrole=cluster-admin --serviceaccount default:steve || true
cat > steve-kubeconfig.yaml <<EOF
apiVersion: v1
clusters:
- cluster:
    insecure-skip-tls-verify: true
    server: http://localhost:8080
  name: upstream
contexts:
- context:
    cluster: upstream
    user: steve
  name: steve
current-context: steve
kind: Config
preferences: {}
users:
- name: steve
  user:
    token: $(kubectl create token steve | xargs echo -n)
EOF

cat > setup-steve.yaml <<EOF
$(kubectl create secret generic steve-kubeconfigs --from-file=steve-kubeconfig.yaml --from-file=rke2.yaml --dry-run=client -o yaml)
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: steve
spec:
  replicas: 1
  revisionHistoryLimit: 1
  selector:
    matchLabels:
      app: steve
  strategy:
    type: Recreate
  template:
    metadata:
      creationTimestamp: null
      labels:
        app: steve
    spec:
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 1
      containers:
      - name: steve
        image: aruiz14/experiments:steve-tracing
        args:
        - --https-listen-port=0
        - --sql-cache
        - --debug
        ports:
        - containerPort: 9080
          protocol: TCP
        env:
        - name: KUBECONFIG
          value: /home/steve/mnt/kubeconfigs/steve-kubeconfig.yaml
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: http://jaeger-collector.default.svc:4318
        volumeMounts:
        - name: kubeconfigs
          mountPath: /home/steve/mnt/kubeconfigs
          readOnly: true
      - name: limiter
        image: aruiz14/experiments:steve-tracing
        command: ["/usr/bin/proxylimiter"]
        env:
        - name: KUBECONFIG
          value: /home/steve/mnt/kubeconfigs/rke2.yaml
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: http://jaeger-collector.default.svc:4318
        args:
        - --no-limit
        - --kube-proxy
        volumeMounts:
        - name: kubeconfigs
          mountPath: /home/steve/mnt/kubeconfigs
          readOnly: true
      volumes:
      - name: kubeconfigs
        secret:
          secretName: steve-kubeconfigs
EOF
kubectl apply -f setup-steve.yaml
