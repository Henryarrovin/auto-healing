package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Henryarrovin/auto-healing/ai"
	"github.com/Henryarrovin/auto-healing/healer"
	"github.com/Henryarrovin/auto-healing/mail"
	"github.com/Henryarrovin/auto-healing/watcher"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("[healer] starting auto-healing service")

	cfg, err := buildKubeConfig()
	if err != nil {
		log.Fatalf("kubeconfig: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("k8s client: %v", err)
	}

	namespace := envOr("NAMESPACE", "auth")

	diagnoser := ai.NewDiagnoser()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := diagnoser.Ping(pingCtx); err != nil {
		log.Printf("[healer] ollama ping warning: %v", err)
	}
	cancel()

	mailCfg := mail.Config{
		Host:     mustEnv("SMTP_HOST"),
		Port:     envOr("SMTP_PORT", "587"),
		Username: envOr("SMTP_USERNAME", ""),
		Password: envOr("SMTP_PASSWORD", ""),
		From:     mustEnv("SMTP_FROM"),
		To:       mustEnv("ALERT_EMAIL_TO"),
	}
	notifier := mail.NewNotifier(mailCfg)

	h := healer.New(client, namespace, diagnoser, notifier)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Pod watcher — CrashLoopBackOff, OOMKilled, DB errors in logs
	podWatcher := watcher.NewPodWatcher(client, namespace, h)
	go podWatcher.Run(ctx)

	// Deployment watcher — failed / stalled rollouts
	deployWatcher := watcher.NewDeploymentWatcher(client, namespace, h)
	go deployWatcher.Run(ctx)

	// Endpoint watcher — service with 0 ready pods
	epWatcher := watcher.NewEndpointWatcher(client, namespace, h)
	go epWatcher.Run(ctx)

	// Kafka consumer lag poller (every 30 s)
	lagWatcher := watcher.NewLagWatcher(mustEnv("KAFKA_BROKERS"), h)
	go lagWatcher.Run(ctx, 30*time.Second)

	log.Printf("[healer] watching namespace=%s | ollama=%s model=%s",
		namespace,
		envOr("OLLAMA_URL", "http://ollama-service.auth.svc.cluster.local:11434"),
		envOr("OLLAMA_MODEL", "qwen2.5:1.5b"),
	)
	<-ctx.Done()
	log.Println("[healer] shutting down")
}

func buildKubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	kubeconfig := envOr("KUBECONFIG", os.ExpandEnv("$HOME/.kube/config"))
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
