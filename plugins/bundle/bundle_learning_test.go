package bundle

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	bundle "github.com/open-policy-agent/opa/bundle"
	"github.com/open-policy-agent/opa/config"
	"github.com/open-policy-agent/opa/download"
	"github.com/open-policy-agent/opa/internal/file/archive"
	"github.com/open-policy-agent/opa/logging"
	"github.com/open-policy-agent/opa/metrics"
	"github.com/open-policy-agent/opa/plugins"
	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/storage/disk"
	"github.com/open-policy-agent/opa/util/test"
)

func Test0(t *testing.T) {
	test.WithTempFS(nil, func(dir string) {
		writePprof(0)
		var metrics metrics.Metrics = metrics.New()
		var bufArchive *bytes.Buffer = generateFixture0(180_000)
		writeFixture(bufArchive)
		// var bufArchive *bytes.Buffer = loadFixture0()
		loader := bundle.NewTarballLoaderWithBaseURL(bufArchive, "")
		bundleName := "bundle0"
		bundleReader := bundle.NewCustomReader(loader).
			WithMetrics(metrics).
			// WithBundleVerificationConfig(&bvc). ??? important ???
			WithLazyLoadingMode(true).
			WithBundleName(bundleName)
		bundle, err := bundleReader.Read()
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		ctx := context.Background()
		store, err := disk.New(ctx, logging.NewNoOpLogger(), nil, disk.Options{Dir: dir, Partitions: []storage.Path{
			storage.MustParsePath("/resources/fr/foo/authorization"),
		}})
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		serviceName := "test-svc"
		manager, err := plugins.New(nil, serviceName, store)
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		var plugin *Plugin = New(&Config{}, manager)
		plugin.status[bundleName] = &Status{Name: bundleName, Metrics: metrics}
		plugin.downloaders[bundleName] = download.New(download.Config{}, plugin.manager.Client(""), bundleName)
		plugin.oneShot(ctx, bundleName, download.Update{Bundle: &bundle, Metrics: metrics, Size: snapshotBundleSize})
		writePprof(1)
		runtime.GC()
		writePprof(2)
	})
}

func Test1(t *testing.T) {
	test.WithTempFS(nil, func(dir string) {
		writePprof(0)
		// var bufArchive *bytes.Buffer = generateFixture0(160_000)
		// writeFixture(bufArchive)
		var bufArchive *bytes.Buffer = loadFixture0()
		// var bufArchive *bytes.Buffer = loadFixture1()
		tsURLBase := "/opa-test/"
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(bufArchive.Bytes())
		}))
		defer ts.Close()
		bundleName := "bundle0"
		ctx := context.Background()
		store, err := disk.New(ctx, logging.NewNoOpLogger(), nil, disk.Options{Dir: dir, Partitions: []storage.Path{
			storage.MustParsePath("/resources/fr/foo/authorization"),
		}})
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		serviceName := "test-svc"
		manager, err := plugins.New(nil, serviceName, store)
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		done := make(chan interface{})
		manager.RegisterPluginStatusListener(t.Name(), func(status map[string]*plugins.Status) {
			var s plugins.Status = *status["bundle"]
			if s.State == "OK" {
				done <- "a"
			}
		})
		if err := manager.Reconfigure(&config.Config{
			Services: []byte(fmt.Sprintf("{\"%s\":{ \"url\": \"%s\"}}", serviceName, ts.URL+tsURLBase))}); err != nil {
			t.Fatalf("Error configuring plugin manager: %s", err)
		}
		var plugin *Plugin = createPlugin(serviceName, bundleName, manager)
		if err := plugin.Start(ctx); err != nil {
			t.Fatal("unexpected error:", err)
		}
		<-done
		plugin.Stop(ctx)
		writePprof(1)
		runtime.GC()
		writePprof(2)
	})
}

func createPlugin(serviceName string, bundleName string, manager *plugins.Manager) *Plugin {
	downloadTrigger := plugins.TriggerPeriodic
	MinDelaySeconds := int64(1)
	MaxDelaySeconds := int64(2)
	LongPollingTimeoutSeconds := int64(1)
	baseConf := download.Config{Trigger: &downloadTrigger, Polling: download.PollingConfig{
		MinDelaySeconds:           &MinDelaySeconds,
		MaxDelaySeconds:           &MaxDelaySeconds,
		LongPollingTimeoutSeconds: &LongPollingTimeoutSeconds,
	}}
	var plugin *Plugin = New(&Config{}, manager)
	plugin.status[bundleName] = &Status{Name: bundleName}
	callback := func(ctx context.Context, u download.Update) {
		plugin.oneShot(ctx, bundleName, u)
	}
	plugin.downloaders[bundleName] = download.New(baseConf, plugin.manager.Client(serviceName), bundleName).WithCallback(callback).WithLazyLoadingMode(true)
	return plugin
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func generateFixture0(n int) *bytes.Buffer {
	files := make([][2]string, n)
	for i := 0; i < n; i++ {
		var fileName = fmt.Sprintf(`/resources/fr/foo/authorization/%d%s/data.json`, i, randomString(4))
		var content = `{
  "` + randomString(36) + `" : {
    "grp": ["` + randomString(36) + `", "` + randomString(36) + `"],
    "rol": ["` + randomString(36) + `","` + randomString(36) + `"]
  },
  "` + randomString(36) + `" : {
    "grp": ["` + randomString(36) + `", "` + randomString(36) + `"],
     "rol": ["` + randomString(36) + `","` + randomString(36) + `"]
  }
}`
		files[i] = [2]string{fileName, content}
	}
	var buf *bytes.Buffer = archive.MustWriteTarGz(files)
	return buf
}

func writeFixture(buf *bytes.Buffer) {
	err := os.WriteFile("/tmp/OPA_LEARNING/fixture0.tar.gz", buf.Bytes(), 0600)
	if err != nil {
		os.Exit(1)
	}
}

func loadFixture0() *bytes.Buffer {
	f, err := os.Open("/tmp/OPA_LEARNING/fixture0.tar.gz")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()
	var buf bytes.Buffer
	buf.ReadFrom(f)
	return &buf
}

func writePprof(i int) {
	fAlloc, err := os.Create(fmt.Sprint("/tmp/OPA_LEARNING/alloc", i))
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer fAlloc.Close()
	if err := pprof.Lookup("allocs").WriteTo(fAlloc, 0); err != nil {
		log.Fatal("could not write alloc profile: ", err)
	}

	fHeap, err := os.Create(fmt.Sprint("/tmp/OPA_LEARNING/heap", i))
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer fHeap.Close()
	if err := pprof.Lookup("heap").WriteTo(fHeap, 0); err != nil {
		log.Fatal("could not write heap profile: ", err)
	}

}
