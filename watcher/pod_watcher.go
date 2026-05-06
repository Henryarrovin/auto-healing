package watcher

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"
	"time"

	"github.com/Henryarrovin/auto-healing/healer"
	"github.com/Henryarrovin/auto-healing/types"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

var dbErrorPatterns = []string{
	"connection refused",
	"dial tcp",
	"no such host",
	"too many connections",
	"pgconn: connect",
	"pq: ",
	"connection pool exhausted",
	"FATAL: password authentication failed",
}

type PodWatcher struct {
	client    kubernetes.Interface
	namespace string
	healer    *healer.Healer
}

func NewPodWatcher(client kubernetes.Interface, namespace string, h *healer.Healer) *PodWatcher {
	return &PodWatcher{client: client, namespace: namespace, healer: h}
}

func (w *PodWatcher) Run(ctx context.Context) {
	for {
		if err := w.watch(ctx); err != nil {
			log.Printf("[pod-watcher] watch error: %v — retrying in 5s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (w *PodWatcher) watch(ctx context.Context) error {
	watcher, err := w.client.CoreV1().Pods(w.namespace).Watch(ctx, metav1.ListOptions{})
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
			pod, ok := ev.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			w.inspect(ctx, pod)
		}
	}
}

func (w *PodWatcher) inspect(ctx context.Context, pod *corev1.Pod) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			w.healer.Heal(ctx, types.Event{
				Kind:      "Pod",
				Name:      pod.Name,
				Namespace: pod.Namespace,
				Reason:    "CrashLoopBackOff on container " + cs.Name,
				Raw:       podSummary(pod),
			})
			return
		}
		if cs.LastTerminationState.Terminated != nil &&
			cs.LastTerminationState.Terminated.Reason == "OOMKilled" {
			w.healer.Heal(ctx, types.Event{
				Kind:      "Pod",
				Name:      pod.Name,
				Namespace: pod.Namespace,
				Reason:    "OOMKilled on container " + cs.Name,
				Raw:       podSummary(pod),
			})
			return
		}
	}

	if pod.Status.Phase == corev1.PodRunning {
		w.scanLogs(ctx, pod)
	}
}

func (w *PodWatcher) scanLogs(ctx context.Context, pod *corev1.Pod) {
	tailLines := int64(50)
	stream, err := w.client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		TailLines: &tailLines,
	}).Stream(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	buf, _ := io.ReadAll(stream)
	logs := string(buf)
	reason := detectDBError(logs)
	if reason == "" {
		return
	}

	w.healer.Heal(ctx, types.Event{
		Kind:      "Pod",
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Reason:    "DB connection failure: " + reason,
		Raw:       "recent logs:\n" + lastNLines(logs, 20),
	})
}

func detectDBError(logs string) string {
	lower := strings.ToLower(logs)
	for _, pat := range dbErrorPatterns {
		if strings.Contains(lower, strings.ToLower(pat)) {
			return pat
		}
	}
	return ""
}

func podSummary(pod *corev1.Pod) string {
	var sb strings.Builder
	sb.WriteString("pod: " + pod.Name + "\nphase: " + string(pod.Status.Phase) + "\n")
	for _, cs := range pod.Status.ContainerStatuses {
		sb.WriteString("container: " + cs.Name + "\n")
		if cs.State.Waiting != nil {
			sb.WriteString("  waiting: " + cs.State.Waiting.Reason + "\n")
		}
		if cs.LastTerminationState.Terminated != nil {
			sb.WriteString("  last exit: " + cs.LastTerminationState.Terminated.Reason + "\n")
		}
	}
	return sb.String()
}

func lastNLines(s string, n int) string {
	scanner := bufio.NewScanner(strings.NewReader(s))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}
