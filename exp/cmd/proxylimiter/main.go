package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/rancher/wrangler/v3/pkg/data"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/tracing/tracing"

	"github.com/conduitio/bwlimit"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	kubeproxy "k8s.io/kubectl/pkg/proxy"
)

var (
	writeLimit = flag.Int("write-limit", int(300*bwlimit.KB), "write limit (B/s)")
	readLimit  = flag.Int("read-limit", int(300*bwlimit.KB), "read limit (B/s)")
	noLimit    = flag.Bool("no-limit", false, "do not limit")

	target    = flag.String("target", "", "target host:port")
	kubeProxy = flag.Bool("kube-proxy", false, "start kubernetes API proxy")
	verbose   = flag.Bool("verbose", false, "enable verbose logs")
)

func setupSignals() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		cancel()
	}()
	return ctx
}

func setupLogging() {
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	fs.Set("v", "9")
}

func main() {
	flag.Parse()
	if *verbose {
		setupLogging()
	}
	if err := run(); err != nil {
		log.Fatalf("%v\n", err)
	}
}

func run() error {
	if *target == "" && !*kubeProxy {
		return fmt.Errorf("all proxies are disabled")
	}

	ctx := setupSignals()
	if err := tracing.SetupTracing(ctx, "proxy"); err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return runForeground(ctx, ":6061", http.DefaultServeMux) })
	if *target != "" {
		log.Printf("Forwarding to %q at :8080\n", *target)
		eg.Go(func() error { return runForwarder(ctx, *target) })
	} else if *kubeProxy {
		log.Printf("Forwarding kubernetes API %q at :8080\n", *target)
		eg.Go(func() error { return runKubeProxy(ctx) })
	}

	return eg.Wait()
}

func runForwarder(ctx context.Context, targetHost string) error {
	u, err := url.Parse(targetHost)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = transport

	limitingProxy := httputil.NewSingleHostReverseProxy(u)
	if *noLimit {
		limitingProxy.Transport = transport
	} else {
		log.Printf("Limiting enabled, read=%dKB/s, write=%dKB/s\n", *readLimit/1000, *writeLimit/1000)
		limitingTransport := transport.Clone()
		limitingTransport.DialContext = bwlimit.NewDialer(&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}, bwlimit.Byte(*writeLimit), bwlimit.Byte(*readLimit)).DialContext
		limitingProxy.Transport = limitingTransport
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		r.Host = u.Host
		if !*noLimit && strings.HasSuffix(r.URL.Path, "/subscribe") {
			log.Println("Serving limited request to /subscribe")
			defer log.Println("Finished /subscribe limited request")
			limitingProxy.ServeHTTP(w, r)
			return
		}
		log.Printf("Serving request to %q with auth: %+v", r.URL.Path, r.Header)
		proxy.ServeHTTP(w, r)
		return
	})
	return runForeground(ctx, ":8080", mux)
}

func runKubeProxy(ctx context.Context) error {
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	if err != nil {
		return err
	}
	_ = restConfig
	proxy, err := kubeproxy.NewProxyHandler("/", &kubeproxy.FilterServer{
		AcceptPaths:   kubeproxy.MakeRegexpArrayOrDie(kubeproxy.DefaultPathAcceptRE),
		RejectPaths:   kubeproxy.MakeRegexpArrayOrDie(kubeproxy.DefaultPathRejectRE),
		AcceptHosts:   kubeproxy.MakeRegexpArrayOrDie(kubeproxy.DefaultHostAcceptRE),
		RejectMethods: kubeproxy.MakeRegexpArrayOrDie(kubeproxy.DefaultMethodRejectRE),
	}, restConfig, 0, false)
	if err != nil {
		return err
	}
	handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/default/configmaps" &&
			r.URL.Path != "/api/v1/namespaces/watch-tests/configmaps" &&
			r.URL.Path != "/api/v1/configmaps" {
			proxy.ServeHTTP(rw, r)
			return
		}
		if true || r.URL.Query().Get("watch") != "true" || !strings.Contains(r.Header.Get("Accept"), "application/json;as=Table") {
			proxy.ServeHTTP(rw, r)
			return
		}

		if _, ok := rw.(*responseWriterInterceptor); !ok {
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()

			rw = interceptWatch(ctx, rw)

			start := time.Now()
			log.Println("START Proxying request to", r.RequestURI, "from", r.RemoteAddr, "Accept = ", r.Header.Get("Accept"))
			defer log.Println("DONE  Proxying request to", r.RequestURI, "from", r.RemoteAddr, ", took", time.Since(start))
		}
		proxy.ServeHTTP(rw, r)
	})
	return runForeground(ctx, ":8080", handler)
}

type responseWriterInterceptor struct {
	http.ResponseWriter
	writeTo io.Writer
}

func (w *responseWriterInterceptor) Write(p []byte) (int, error) {
	return w.writeTo.Write(p)
}

type watchEvent struct {
	Type   watch.EventType            `json:"type"`
	Object *unstructured.Unstructured `json:"object"`

	span trace.Span
}

func spanForEvent(ctx context.Context, event watchEvent) trace.Span {
	if event.Type == "BOOKMARK" {
		_, span := noop.NewTracerProvider().Tracer("").Start(ctx, "bookmarkEvent")
		return span
	}
	obj := event.Object
	rv := obj.GetResourceVersion()
	rowToObject(obj)
	key := strings.TrimPrefix(obj.GetNamespace()+"/"+obj.GetName(), "/")

	ctx = tracing.RootContextForProxy(ctx, rv)
	_, span := otel.Tracer("").Start(ctx, "configmap-change",
		trace.WithAttributes(
			attribute.String("event.type", string(event.Type)),
			attribute.String("object.resourceVersion", rv),
			attribute.String("object.key", key),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	//if span.IsRecording() {
	//log.Println("Inside trace! TraceID =", span.SpanContext().TraceID().String(), "", ", SpanID = ", span.SpanContext().SpanID().String(), ", sampled =", span.SpanContext().IsSampled(), ", key = ", key, ", rv = ", rv)
	//}
	return span
}

func interceptWatch(ctx context.Context, rw http.ResponseWriter) http.ResponseWriter {
	const maxBuffer = 1000
	// Replace Writes to the ResponseWriter with a Pipe
	pr, pw := io.Pipe()
	interceptor := &responseWriterInterceptor{rw, pw}
	var out io.Writer = rw
	// Consume events with a JSON decoder, and puts them into a buffer
	// its get paused when the buffer is full, also pausing the writer
	events := make(chan watchEvent, maxBuffer)
	dec := json.NewDecoder(pr)
	go func() {
		<-ctx.Done()
		out = io.Discard
	}()
	go func() {
		// TODO: optimize this if needed? io.TeeReader(pr, &buf)
		defer pr.Close()
		defer close(events)
		for {
			select {
			case <-ctx.Done():
				log.Printf("!!!!!!!Context canceled! %+v\n", ctx.Err())
				return
			default:
			}
			var event watchEvent
			if err := dec.Decode(&event); err != nil {
				log.Printf("!!!!!!!Error decoding event to : %+v\n", err)
				return
			}
			event.span = spanForEvent(ctx, event)
			select {
			case events <- event:
			default:
				log.Println("!!!!!! Events channel is full!, waiting...")
				events <- event
				log.Println("!!!!!! ...... Done!")
			}
		}
	}()
	debugCtx, cancelDebug := context.WithCancel(ctx)
	go func() {
		defer cancelDebug()
		for event := range events {
			b, err := json.Marshal(event)
			if err != nil {
				log.Printf("!!!!!!!!!!! Error re-marshaling event, skipping...!: %+v\n", err)
				continue
			}
			if _, err := io.Copy(out, bytes.NewReader(b)); err != nil {
				log.Printf("!!!!!!!!!!! Error sending marshaled event!: %+v\n", err)
				return
			}
			event.span.End()
		}
	}()

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			fmt.Println("=========== Events channel length:", len(events), "/", cap(events))
			select {
			case <-t.C:
			case <-debugCtx.Done():
				return
			}
		}
	}()

	return interceptor
}

func runForeground(ctx context.Context, addr string, handler http.Handler) error {
	server := &http.Server{Addr: addr, Handler: handler}

	errCh := make(chan error)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return server.Shutdown(context.Background())
	}
}

func rowToObject(obj *unstructured.Unstructured) {
	if obj == nil {
		return
	}
	if obj.Object["kind"] != "Table" ||
		(obj.Object["apiVersion"] != "meta.k8s.io/v1" &&
			obj.Object["apiVersion"] != "meta.k8s.io/v1beta1") {
		return
	}

	items := tableToObjects(obj.Object)
	if len(items) == 1 {
		obj.Object = items[0].Object
	}
}

func tableToObjects(obj map[string]interface{}) []unstructured.Unstructured {
	var result []unstructured.Unstructured

	rows, _ := obj["rows"].([]interface{})
	for _, row := range rows {
		m, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		cells := m["cells"]
		object, ok := m["object"].(map[string]interface{})
		if !ok {
			continue
		}

		data.PutValue(object, cells, "metadata", "fields")
		result = append(result, unstructured.Unstructured{
			Object: object,
		})
	}

	return result
}
