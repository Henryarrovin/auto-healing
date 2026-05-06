package watcher

import (
	"context"
	"log"
	"time"

	"github.com/Henryarrovin/auto-healing/healer"
	"github.com/Henryarrovin/auto-healing/types"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type DeploymentWatcher struct {
	client    kubernetes.Interface
	namespace string
	healer    *healer.Healer
}

func NewDeploymentWatcher(client kubernetes.Interface, namespace string, h *healer.Healer) *DeploymentWatcher {
	return &DeploymentWatcher{client: client, namespace: namespace, healer: h}
}

func (w *DeploymentWatcher) Run(ctx context.Context) {
	for {
		if err := w.watch(ctx); err != nil {
			log.Printf("[deploy-watcher] watch error: %v — retrying in 5s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (w *DeploymentWatcher) watch(ctx context.Context) error {
	watcher, err := w.client.AppsV1().Deployments(w.namespace).Watch(ctx, metav1.ListOptions{})
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
			if ev.Type != watch.Modified {
				continue
			}
			deploy, ok := ev.Object.(*appsv1.Deployment)
			if !ok {
				continue
			}
			w.inspect(ctx, deploy)
		}
	}
}

func (w *DeploymentWatcher) inspect(ctx context.Context, deploy *appsv1.Deployment) {
	for _, cond := range deploy.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing &&
			cond.Status == "False" &&
			cond.Reason == "ProgressDeadlineExceeded" {
			w.healer.Heal(ctx, types.Event{
				Kind:      "Deployment",
				Name:      deploy.Name,
				Namespace: deploy.Namespace,
				Reason:    "rollout deadline exceeded",
				Raw:       deploySummary(deploy),
			})
			return
		}
		if cond.Type == appsv1.DeploymentAvailable &&
			cond.Status == "False" &&
			time.Since(cond.LastTransitionTime.Time) > 2*time.Minute {
			w.healer.Heal(ctx, types.Event{
				Kind:      "Deployment",
				Name:      deploy.Name,
				Namespace: deploy.Namespace,
				Reason:    "deployment unavailable for > 2 minutes",
				Raw:       deploySummary(deploy),
			})
			return
		}
	}

	if deploy.Spec.Replicas != nil &&
		*deploy.Spec.Replicas > 0 &&
		deploy.Status.ReadyReplicas == 0 &&
		deploy.Status.ObservedGeneration > 1 {
		w.healer.Heal(ctx, types.Event{
			Kind:      "Deployment",
			Name:      deploy.Name,
			Namespace: deploy.Namespace,
			Reason:    "zero ready replicas after initial rollout",
			Raw:       deploySummary(deploy),
		})
	}
}

func deploySummary(d *appsv1.Deployment) string {
	desired := int32(0)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return "deployment: " + d.Name +
		"\n  desired=" + i32s(desired) +
		" ready=" + i32s(d.Status.ReadyReplicas) +
		" available=" + i32s(d.Status.AvailableReplicas) +
		" updated=" + i32s(d.Status.UpdatedReplicas)
}

func i32s(n int32) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 4)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
