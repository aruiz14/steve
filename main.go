package main

import (
	"flag"
	"net/http"
	"os"
	"runtime"

	_ "net/http/pprof"

	"github.com/rancher/dynamiclistener/server"
	"github.com/rancher/steve/pkg/debug"
	stevecli "github.com/rancher/steve/pkg/server/cli"
	"github.com/rancher/steve/pkg/version"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/client-go/util/tracing/tracing"
	"k8s.io/klog/v2"
)

var (
	config      stevecli.Config
	debugconfig debug.Config
)

func init() {
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(1)
}

func setupKlog() {
	fs := flag.NewFlagSet("klog", flag.PanicOnError)
	klog.InitFlags(fs)
	if err := fs.Parse([]string{"-v=2"}); err != nil {
		logrus.Fatal(err)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "steve"
	app.Version = version.FriendlyVersion()
	app.Usage = ""
	app.Flags = append(
		stevecli.Flags(&config),
		debug.Flags(&debugconfig)...)
	app.Action = run

	go http.ListenAndServe(":6060", nil)

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func run(_ *cli.Context) error {
	ctx := signals.SetupSignalContext()
	debugconfig.MustSetupDebug()
	attrs := []attribute.KeyValue{
		attribute.Bool("sql.cache", debugconfig.SQLCache),
	}
	if err := tracing.SetupTracing(ctx, "steve", attrs...); err != nil {
		return err
	}
	s, err := config.ToServer(ctx, debugconfig.SQLCache)
	if err != nil {
		return err
	}
	return s.ListenAndServe(ctx, config.HTTPSListenPort, config.HTTPListenPort, &server.ListenOpts{DisplayServerLogs: true})
}
