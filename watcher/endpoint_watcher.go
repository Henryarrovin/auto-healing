package watcher

import (
	"context"
	"log"
	"time"

	"github.com/Henryarrovin/auto-healing/healer"
	"github.com/Henryarrovin/auto-healing/types"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type EndpointWatcher struct {
	client    kubernetes.Interface
	namespace string
	healer    *healer.Healer
}

func NewEndpointWatcher(client kubernetes.Interface, namespace string, h *healer.Healer) *EndpointWatcher {
	return &EndpointWatcher{client: client, namespace: namespace, healer: h}
}

func (w *EndpointWatcher) Run(ctx context.Context) {
	for {
		if err := w.watch(ctx); err != nil {
			log.Printf("[endpoint-watcher] watch error: %v — retrying in 5s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (w *EndpointWatcher) watch(ctx context.Context) error {
	watcher, err := w.client.CoreV1().Endpoints(w.namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-watcher.ResultChan():
			if !ok {
				return nil
			}
			if ev.Type != watch.Modified && ev.Type != watch.Added {
				continue
			}
			ep, ok := ev.Object.(*corev1.Endpoints)
			if !ok {
				continue
			}
			if ep.Namespace == "kube-system" || ep.Name == "kubernetes" {
				continue
			}
			w.inspect(ctx, ep)
		}
	}
}

func (w *EndpointWatcher) inspect(ctx context.Context, ep *corev1.Endpoints) {
	readyCount := 0
	for _, sub := range ep.Subsets {
		readyCount += len(sub.Addresses)
	}
	if readyCount == 0 {
		w.healer.Heal(ctx, types.Event{
			Kind:      "Endpoint",
			Name:      ep.Name,
			Namespace: ep.Namespace,
			Reason:    "service has 0 ready endpoints",
			Raw:       "service: " + ep.Name + "\nready_addresses: 0",
		})
	}
}
