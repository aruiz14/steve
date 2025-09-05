package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/wrangler/v3/pkg/data"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/tracing/tracing"
)

const (
	// defaultQPS and defaultBurst are used to configure the rest.Config for
	// factory created clients.
	defaultQPS   float32 = 10000
	defaultBurst int     = 100
)

type Factory struct {
	impersonate         bool
	tableClientCfg      *rest.Config
	tableWatchClientCfg *rest.Config
	clientCfg           *rest.Config
	watchClientCfg      *rest.Config
	metadata            metadata.Interface
	dynamic             dynamic.Interface
	Config              *rest.Config
}

type addQuery struct {
	values map[string]string
	next   http.RoundTripper
}

var _ utilnet.RoundTripperWrapper = (*addQuery)(nil)

func (a *addQuery) RoundTrip(req *http.Request) (*http.Response, error) {
	q := req.URL.Query()
	for k, v := range a.values {
		q.Set(k, v)
	}
	req.Header.Set("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io,application/json;as=Table;v=v1beta1;g=meta.k8s.io")
	req.URL.RawQuery = q.Encode()
	return a.next.RoundTrip(req)
}

func (a *addQuery) WrappedRoundTripper() http.RoundTripper {
	return a.next
}

type factoryOptions struct {
	qps   float32
	burst int
}

func defaultFactoryOptions() *factoryOptions {
	return &factoryOptions{
		qps:   defaultQPS,
		burst: defaultBurst,
	}
}

// WithQPSAndBurst configures the rest.Config used for creating the clients in
// the factory with the provided burst and qps configuration.
//
// See https://pkg.go.dev/k8s.io/client-go/rest#Config for more.
func WithQPSAndBurst(qps float32, burst int) FactoryOption {
	return func(opts *factoryOptions) {
		opts.qps = qps
		opts.burst = burst
	}
}

// FactoryOption is an option-func for configuring the newly created factory.
type FactoryOption func(*factoryOptions)

func NewFactory(cfg *rest.Config, impersonate bool, opts ...FactoryOption) (*Factory, error) {
	clientCfg := rest.CopyConfig(cfg)
	options := defaultFactoryOptions()
	for _, opt := range opts {
		opt(options)
	}

	clientCfg.QPS = options.qps
	clientCfg.Burst = options.burst

	watchClientCfg := rest.CopyConfig(clientCfg)
	watchClientCfg.Timeout = 30 * time.Minute

	setTable := func(rt http.RoundTripper) http.RoundTripper {
		return &addQuery{
			values: map[string]string{
				"includeObject": "Object",
			},
			next: rt,
		}
	}

	tableClientCfg := rest.CopyConfig(clientCfg)
	tableClientCfg.Wrap(setTable)
	tableClientCfg.AcceptContentTypes = "application/json;as=Table;v=v1;g=meta.k8s.io,application/json;as=Table;v=v1beta1;g=meta.k8s.io"
	tableWatchClientCfg := rest.CopyConfig(watchClientCfg)
	tableWatchClientCfg.Wrap(setTable)
	tableWatchClientCfg.AcceptContentTypes = "application/json;as=Table;v=v1;g=meta.k8s.io,application/json;as=Table;v=v1beta1;g=meta.k8s.io"

	md, err := metadata.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	d, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Factory{
		dynamic:             d,
		metadata:            md,
		impersonate:         impersonate,
		tableClientCfg:      tableClientCfg,
		tableWatchClientCfg: tableWatchClientCfg,
		clientCfg:           clientCfg,
		watchClientCfg:      watchClientCfg,
		Config:              watchClientCfg,
	}, nil
}

func (p *Factory) MetadataClient() metadata.Interface {
	return p.metadata
}

func (p *Factory) AdminDynamicClient() dynamic.Interface {
	return p.dynamic
}

func (p *Factory) IsImpersonating() bool {
	return p.impersonate
}

func (p *Factory) K8sInterface(ctx *types.APIRequest) (kubernetes.Interface, error) {
	cfg, err := setupConfig(ctx, p.clientCfg, p.impersonate)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(cfg)
}

func (p *Factory) AdminK8sInterface() (kubernetes.Interface, error) {
	return kubernetes.NewForConfig(p.clientCfg)
}

func (p *Factory) DynamicClient(ctx *types.APIRequest, warningHandler rest.WarningHandler) (dynamic.Interface, error) {
	return newDynamicClient(ctx, p.clientCfg, p.impersonate, warningHandler)
}

func (p *Factory) Client(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return newClient(ctx, p.clientCfg, s, namespace, p.impersonate, warningHandler)
}

func (p *Factory) AdminClient(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return newClient(ctx, p.clientCfg, s, namespace, false, warningHandler)
}

func (p *Factory) ClientForWatch(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return newClient(ctx, p.watchClientCfg, s, namespace, p.impersonate, warningHandler)
}

func (p *Factory) AdminClientForWatch(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return newClient(ctx, p.watchClientCfg, s, namespace, false, warningHandler)
}

func (p *Factory) TableClient(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	if attributes.Table(s) {
		return newClient(ctx, p.tableClientCfg, s, namespace, p.impersonate, warningHandler)
	}
	return p.Client(ctx, s, namespace, warningHandler)
}

func (p *Factory) TableAdminClient(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	if attributes.Table(s) {
		return newClient(ctx, p.tableClientCfg, s, namespace, false, warningHandler)
	}
	return p.AdminClient(ctx, s, namespace, warningHandler)
}

func (p *Factory) TableClientForWatch(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	if attributes.Table(s) {
		return newClient(ctx, p.tableWatchClientCfg, s, namespace, p.impersonate, warningHandler)
	}
	return p.ClientForWatch(ctx, s, namespace, warningHandler)
}

func (p *Factory) TableAdminClientForWatch(ctx *types.APIRequest, s *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	if attributes.Table(s) {
		return newClient(ctx, p.tableWatchClientCfg, s, namespace, false, warningHandler)
	}
	return p.AdminClientForWatch(ctx, s, namespace, warningHandler)
}

func setupConfig(ctx *types.APIRequest, cfg *rest.Config, impersonate bool) (*rest.Config, error) {
	if impersonate {
		user, ok := request.UserFrom(ctx.Context())
		if !ok {
			return nil, fmt.Errorf("user not found for impersonation")
		}
		cfg = rest.CopyConfig(cfg)
		cfg.Impersonate.UserName = user.GetName()
		cfg.Impersonate.Groups = user.GetGroups()
		cfg.Impersonate.Extra = user.GetExtra()
	}
	return cfg, nil
}

func newDynamicClient(ctx *types.APIRequest, cfg *rest.Config, impersonate bool, warningHandler rest.WarningHandler) (dynamic.Interface, error) {
	cfg, err := setupConfig(ctx, cfg, impersonate)
	cfg.WarningHandler = warningHandler
	if err != nil {
		return nil, err
	}
	if false {
		cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			if _, ok := rt.(*watchInterceptorRoundTripper); ok {
				return rt
			}
			return &watchInterceptorRoundTripper{next: rt}
		})
	}
	return dynamic.NewForConfig(cfg)
}

func newClient(ctx *types.APIRequest, cfg *rest.Config, s *types.APISchema, namespace string, impersonate bool, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	client, err := newDynamicClient(ctx, cfg, impersonate, warningHandler)
	if err != nil {
		return nil, err
	}

	gvr := attributes.GVR(s)
	return client.Resource(gvr).Namespace(namespace), nil
}

type watchInterceptorRoundTripper struct {
	next http.RoundTripper
}

func (w watchInterceptorRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path != "/api/v1/namespaces/default/configmaps" &&
		r.URL.Path != "/api/v1/namespaces/watch-tests/configmaps" &&
		r.URL.Path != "/api/v1/configmaps" {
		return w.next.RoundTrip(r)
	}
	if true || r.URL.Query().Get("watch") != "true" { //|| !strings.Contains(r.Header.Get("Accept"), "application/json;as=Table") {
		return w.next.RoundTrip(r)
	}

	logrus.Infoln("----INTERCEPTING WATCH REQUEST!! at rest.Client", r.URL)
	res, err := w.next.RoundTrip(r)
	if err != nil {
		return res, err
	}
	logrus.Infoln("----INTERCEPTED WATCH REQUEST!! at rest.Client", r.URL)
	res.Body = interceptBody(res.Body)
	return res, err
}

type bufferedBodyReader struct {
	sync.Mutex
	bytes.Buffer
	readCount   uint64
	writeCount  uint64
	remaining   <-chan position
	currentItem *position
	cancel      context.CancelFunc
}

type position struct {
	pos  uint64
	span trace.Span
}

func newBufferedBodyReader(posChan <-chan position) (context.Context, *bufferedBodyReader) {
	ctx, cancel := context.WithCancel(context.Background())
	return ctx, &bufferedBodyReader{
		remaining: posChan,
		cancel:    cancel,
	}
}

func (b *bufferedBodyReader) Read(p []byte) (n int, err error) {
	if b.currentItem == nil {
		v, ok := <-b.remaining
		if !ok {
			return 0, io.EOF
		}
		b.currentItem = &v
	}

	b.Lock()
	defer b.Unlock()
	n, err = b.Buffer.Read(p)
	b.readCount += uint64(n)
	if b.readCount >= b.currentItem.pos {
		b.currentItem.span.End()
		b.currentItem = nil
		b.Buffer.Truncate(b.Buffer.Len())
	}

	return
}

func (b *bufferedBodyReader) Write(p []byte) (n int, err error) {
	// Avoid locking to be done externally so writeCount can be immediately read afterwards
	n, err = b.Buffer.Write(p)
	b.writeCount += uint64(n)
	return
}

func (b *bufferedBodyReader) Close() error {
	log.Println("!!!!!!!BODY CLOSED!!")
	b.cancel()
	return nil
}

type watchEvent struct {
	Type   watch.EventType            `json:"type"`
	Object *unstructured.Unstructured `json:"object"`
}

func spanForEvent(event watchEvent) trace.Span {
	if event.Type == watch.Bookmark {
		_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "bookmarkEvent")
		return span
	}
	obj := event.Object
	rv := obj.GetResourceVersion()
	rowToObject(obj)
	key := strings.TrimPrefix(obj.GetNamespace()+"/"+obj.GetName(), "/")

	ctx := tracing.RootContextForWatcher(context.Background(), rv)
	_, span := otel.Tracer("").Start(ctx, "rest-client-watch-intercept",
		trace.WithAttributes(
			attribute.String("event.type", string(event.Type)),
			attribute.String("object.resourceVersion", rv),
			attribute.String("object.key", key),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	return span
}

func interceptBody(body io.ReadCloser) io.ReadCloser {
	if body == nil {
		return nil
	}

	const maxBuffer = 1000

	positions := make(chan position, maxBuffer)
	ctx, buffer := newBufferedBodyReader(positions)
	debugCtx, cancelDebug := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			fmt.Println("=========== Buffer channel length:", len(positions), "/", cap(positions))
			select {
			case <-t.C:
			case <-debugCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer cancelDebug()
		defer func() {
			defer body.Close()
			if _, err := io.Copy(io.Discard, body); err == io.EOF {
				return
			}
		}()
		defer close(positions)

		dec := json.NewDecoder(body)
		enc := json.NewEncoder(buffer)
		for {
			select {
			case <-ctx.Done():
				log.Printf("!!!!!!!Aborting decoder: %+v\n", ctx.Err())
				return
			default:
			}
			var event watchEvent
			if err := dec.Decode(&event); err != nil {
				if err != io.EOF {
					log.Printf("!!!!!!!Error decoding event: %+v\n", err)
				}
				return
			}
			pos := position{
				span: spanForEvent(event),
			}
			buffer.Lock()
			if err := func() error {
				defer buffer.Unlock()
				if err := enc.Encode(event); err != nil {
					return err
				}
				pos.pos = buffer.writeCount
				return nil
			}(); err != nil {
				log.Printf("!!!!!!!Error re-encoding event: %+v\n", err)
				return
			}
			select {
			case positions <- pos:
			default:
				log.Println("!!!!!! Positions channel is full!, waiting...")
				positions <- pos
				log.Println("!!!!!! ...... Done!")
			}
		}
	}()
	return buffer
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
