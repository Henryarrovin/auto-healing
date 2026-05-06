package healer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Henryarrovin/auto-healing/ai"
	"github.com/Henryarrovin/auto-healing/mail"
	"github.com/Henryarrovin/auto-healing/types"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Healer struct {
	client    kubernetes.Interface
	namespace string
	ai        *ai.Diagnoser
	mail      *mail.Notifier
	cooldowns map[string]time.Time
}

func New(client kubernetes.Interface, namespace string, diagnoser *ai.Diagnoser, notifier *mail.Notifier) *Healer {
	return &Healer{
		client:    client,
		namespace: namespace,
		ai:        diagnoser,
		mail:      notifier,
		cooldowns: make(map[string]time.Time),
	}
}

func (h *Healer) Heal(ctx context.Context, ev types.Event) {
	key := fmt.Sprintf("%s/%s", ev.Kind, ev.Name)

	if last, ok := h.cooldowns[key]; ok && time.Since(last) < 2*time.Minute {
		log.Printf("[healer] cooldown active for %s, skipping", key)
		return
	}
	h.cooldowns[key] = time.Now()

	log.Printf("[healer] healing %s %s — reason: %s", ev.Kind, ev.Name, ev.Reason)

	diagCh := make(chan string, 1)
	go func() {
		diagnosis, err := h.ai.Diagnose(ctx, ev)
		if err != nil {
			diagCh <- fmt.Sprintf("AI diagnosis unavailable: %v", err)
			return
		}
		diagCh <- diagnosis
	}()

	var actionTaken string
	var actionErr error

	switch ev.Kind {
	case "Pod":
		actionTaken, actionErr = h.healPod(ctx, ev)
	case "Deployment":
		actionTaken, actionErr = h.healDeployment(ctx, ev)
	case "Endpoint":
		actionTaken, actionErr = h.healEndpoint(ctx, ev)
	case "KafkaLag":
		actionTaken, actionErr = h.healKafkaLag(ctx, ev)
	default:
		actionTaken = "no action defined for kind " + ev.Kind
	}

	diagnosis := <-diagCh

	status := "✅ healed"
	if actionErr != nil {
		status = fmt.Sprintf("⚠️ action failed: %v", actionErr)
		log.Printf("[healer] action error for %s: %v", key, actionErr)
	}

	h.mail.Send(mail.Message{
		Resource:   fmt.Sprintf("%s/%s", ev.Kind, ev.Name),
		Reason:     ev.Reason,
		Action:     actionTaken,
		Status:     status,
		Diagnosis:  diagnosis,
		Namespace:  ev.Namespace,
		OccurredAt: time.Now().Format(time.RFC3339),
	})
}

func (h *Healer) healPod(ctx context.Context, ev types.Event) (string, error) {
	err := h.client.CoreV1().Pods(ev.Namespace).Delete(ctx, ev.Name, metav1.DeleteOptions{})
	if err != nil {
		return "", fmt.Errorf("delete pod: %w", err)
	}
	log.Printf("[healer] deleted pod %s/%s", ev.Namespace, ev.Name)
	return fmt.Sprintf("deleted pod %s (controller will reschedule)", ev.Name), nil
}

func (h *Healer) healDeployment(ctx context.Context, ev types.Event) (string, error) {
	deploy, err := h.client.AppsV1().Deployments(ev.Namespace).Get(ctx, ev.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get deployment: %w", err)
	}

	rsList, err := h.client.AppsV1().ReplicaSets(ev.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list replicasets: %w", err)
	}

	if prev := findPreviousRS(deploy, rsList.Items); prev != nil {
		deploy.Spec.Template = prev.Spec.Template
		_, err = h.client.AppsV1().Deployments(ev.Namespace).Update(ctx, deploy, metav1.UpdateOptions{})
		if err != nil {
			return "", fmt.Errorf("rollback update: %w", err)
		}
		log.Printf("[healer] rolled back deployment %s/%s to RS %s", ev.Namespace, ev.Name, prev.Name)
		return fmt.Sprintf("rolled back %s to previous ReplicaSet %s", ev.Name, prev.Name), nil
	}

	return h.restartDeployment(ctx, ev.Namespace, deploy)
}

func (h *Healer) restartDeployment(ctx context.Context, ns string, deploy *appsv1.Deployment) (string, error) {
	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = make(map[string]string)
	}
	deploy.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
	_, err := h.client.AppsV1().Deployments(ns).Update(ctx, deploy, metav1.UpdateOptions{})
	if err != nil {
		return "", fmt.Errorf("restart patch: %w", err)
	}
	return fmt.Sprintf("triggered rolling restart of %s", deploy.Name), nil
}

func findPreviousRS(deploy *appsv1.Deployment, rsList []appsv1.ReplicaSet) *appsv1.ReplicaSet {
	var best *appsv1.ReplicaSet
	for i := range rsList {
		rs := &rsList[i]
		owned := false
		for _, ref := range rs.OwnerReferences {
			if ref.UID == deploy.UID {
				owned = true
				break
			}
		}
		if !owned {
			continue
		}
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 {
			if best == nil || rs.CreationTimestamp.After(best.CreationTimestamp.Time) {
				best = rs
			}
		}
	}
	return best
}

func (h *Healer) healEndpoint(ctx context.Context, ev types.Event) (string, error) {
	svc, err := h.client.CoreV1().Services(ev.Namespace).Get(ctx, ev.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get service: %w", err)
	}

	deploys, err := h.client.AppsV1().Deployments(ev.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list deployments: %w", err)
	}

	for i := range deploys.Items {
		d := &deploys.Items[i]
		if selectorMatches(svc.Spec.Selector, d.Spec.Template.Labels) {
			return h.restartDeployment(ctx, ev.Namespace, d)
		}
	}
	return fmt.Sprintf("service %s has no endpoints — no matching deployment found", ev.Name), nil
}

func selectorMatches(selector, labels map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func (h *Healer) healKafkaLag(ctx context.Context, ev types.Event) (string, error) {
	deploy, err := h.client.AppsV1().Deployments(ev.Namespace).Get(ctx, ev.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get consumer deployment: %w", err)
	}

	current := *deploy.Spec.Replicas
	desired := current + 1
	if desired > 5 {
		return fmt.Sprintf("%s already at max replicas (5), not scaling further", ev.Name), nil
	}

	deploy.Spec.Replicas = &desired
	_, err = h.client.AppsV1().Deployments(ev.Namespace).Update(ctx, deploy, metav1.UpdateOptions{})
	if err != nil {
		return "", fmt.Errorf("scale deployment: %w", err)
	}
	log.Printf("[healer] scaled %s/%s: %d → %d", ev.Namespace, ev.Name, current, desired)
	return fmt.Sprintf("scaled %s from %d → %d replicas to reduce lag", ev.Name, current, desired), nil
}
