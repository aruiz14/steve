package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
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

//func writeTo(dst[], s string) {
//	hex.Encode()
//
//}

func setupTracing(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	//exp, err := stdouttrace.New()
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}

	// Ensure default SDK resources and the required service name are set.
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithIDGenerator(&idGenerator{}),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)

	return tp, nil
}

func traceIdFromResourceVersion(i int) trace.TraceID {
	var tid trace.TraceID
	hash := md5.Sum([]byte(fmt.Sprintf("%d", i)))
	copy(tid[:], hash[:])
	return tid
}

func spanIdFromString(s string) trace.SpanID {
	var sid trace.SpanID
	if padding := cap(sid) - len(s); padding > 0 {
		s = strings.Repeat("0", padding) + s
	} else {
		s = s[-padding:]
	}
	copy(sid[:], s)
	return sid
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("%v\n", err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(setupSignals())
	defer cancel()

	tpProducer, err := setupTracing(ctx, "experiment-producer")
	if err != nil {
		return err
	}
	tpConsumer, err := setupTracing(ctx, "experiment-consumer")
	if err != nil {
		return err
	}
	scc := trace.SpanContextConfig{
		TraceID:    trace.TraceID{},
		SpanID:     trace.SpanID{},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}
	trace.NewSpanContext(scc)

	//var tc propagation.TraceContext
	ready := make(chan struct{})
	if err := func() error {
		eg, ctx := errgroup.WithContext(ctx)
		for _, rv := range []string{"123", "456", "789"} {
			var tid trace.TraceID
			if n, err := strconv.Atoi(rv); err == nil {
				tid = traceIdFromResourceVersion(n)
			}
			for _, n := range []string{"a", "b", "c"} {
				log.Println("Creating traces for HOSTNAME=", "foo-"+n)
				//traceId := rv
				//sampleBit := 0
				//if true {
				//	sampleBit = 1
				//}
				//ctx := tc.Extract(ctx, propagation.MapCarrier{
				//	"traceparent": fmt.Sprintf("00-%032s-%016x-%02d", traceId, 1, sampleBit),
				//})
				//cctx := tc.Extract(ctx, propagation.MapCarrier{
				//	"traceparent": fmt.Sprintf("00-%032s-%016x-%02d", traceId, n, sampleBit),
				//})
				eg.Go(func() error {
					<-ready
					func() {
						//time.Sleep(1 * time.Second)
						//os.Setenv("HOSTNAME", "foo-"+n)
						//defer os.Unsetenv("HOSTNAME")

						//ctx := tracing.ToRemoteContext(ctx, rv)
						func() {
							tracer := tpProducer.Tracer("", trace.WithInstrumentationAttributes(attribute.String("replica", n), attribute.String("resourceVersion", rv)))
							ctx := trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
								TraceID:    tid,
								SpanID:     spanIdFromString("kubeapi"),
								TraceFlags: trace.FlagsSampled,
								Remote:     true,
							}))
							//ctx := tc.Extract(ctx, propagation.MapCarrier{
							//	"traceparent": fmt.Sprintf("00-%032s-%016x-%02d", traceId, 1, 1),
							//})
							ctx = WithCustomNextSpanID(ctx, spanIdFromString(n))
							_, rootSpan := tracer.Start(ctx, "top-level-producer", trace.WithSpanKind(trace.SpanKindProducer))
							rootSpan.AddEvent("Event sent!")
							defer rootSpan.End()
							log.Printf("Event produced! TraceID=%q, SpanID=%q", rootSpan.SpanContext().TraceID(), rootSpan.SpanContext().SpanID())
						}()
						func() {
							tracer := tpConsumer.Tracer("", trace.WithInstrumentationAttributes(attribute.String("replica", n), attribute.String("resourceVersion", rv)))
							ctx := trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
								TraceID:    tid,
								SpanID:     spanIdFromString(n),
								TraceFlags: trace.FlagsSampled,
								Remote:     true,
							}))
							ctx, rootSpan := tracer.Start(ctx, "top-level-consumer", trace.WithSpanKind(trace.SpanKindConsumer))
							rootSpan.AddEvent("Event received!")
							defer rootSpan.End()
							log.Printf("Event consumed! TraceID=%q, SpanID=%q", rootSpan.SpanContext().TraceID(), rootSpan.SpanContext().SpanID())
							_, span := tpConsumer.Tracer("").Start(ctx, "level1",
								trace.WithAttributes(attribute.String("replica", n)),
								trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("resourceVersion", rv)))
							time.Sleep(100 * time.Millisecond)
							rootSpan.AddEvent("Event processed!")
							defer span.End()
						}()
					}()
					return nil
				})
			}
		}
		close(ready)
		return eg.Wait()
	}(); err != nil {
		return fmt.Errorf("error during tracing: %w", err)
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return tpProducer.ForceFlush(ctx)
	})
	eg.Go(func() error {
		return tpConsumer.ForceFlush(ctx)
	})
	return eg.Wait()
}
