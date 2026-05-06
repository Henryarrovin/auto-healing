package watcher

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Henryarrovin/auto-healing/healer"
	"github.com/Henryarrovin/auto-healing/types"
	"github.com/IBM/sarama"
)

const lagThreshold = 500

// maps Kafka consumer group names → Kubernetes Deployment names.
var groupDeploymentMap = map[string]string{
	"payment-log-consumer": "payment-gateway",
	"auth-log-consumer":    "auth-service",
}

type LagWatcher struct {
	brokers []string
	healer  *healer.Healer
}

func NewLagWatcher(brokerList string, h *healer.Healer) *LagWatcher {
	return &LagWatcher{
		brokers: strings.Split(brokerList, ","),
		healer:  h,
	}
}

func (w *LagWatcher) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.checkLag(ctx)
		}
	}
}

func (w *LagWatcher) checkLag(ctx context.Context) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(w.brokers, cfg)
	if err != nil {
		log.Printf("[kafka-lag] cannot connect to brokers %v: %v", w.brokers, err)
		return
	}
	defer admin.Close()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Printf("[kafka-lag] list groups: %v", err)
		return
	}

	client, err := sarama.NewClient(w.brokers, cfg)
	if err != nil {
		log.Printf("[kafka-lag] sarama client: %v", err)
		return
	}
	defer client.Close()

	for group := range groups {
		deployName, known := groupDeploymentMap[group]
		if !known {
			continue
		}

		lag, err := consumerGroupLag(admin, client, group)
		if err != nil {
			log.Printf("[kafka-lag] lag for group %s: %v", group, err)
			continue
		}

		log.Printf("[kafka-lag] group=%s lag=%d", group, lag)

		if lag > lagThreshold {
			w.healer.Heal(ctx, types.Event{
				Kind:      "KafkaLag",
				Name:      deployName,
				Namespace: "auth",
				Reason:    fmt.Sprintf("consumer group %q lag=%d > threshold=%d", group, lag, lagThreshold),
				Raw:       fmt.Sprintf("group: %s\nlag: %d\nbrokers: %s", group, lag, strings.Join(w.brokers, ",")),
			})
		}
	}
}

func consumerGroupLag(admin sarama.ClusterAdmin, client sarama.Client, group string) (int64, error) {
	offsets, err := admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return 0, err
	}

	var totalLag int64
	for topic, partitions := range offsets.Blocks {
		for partition, block := range partitions {
			newest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				continue
			}
			committed := block.Offset
			if committed < 0 {
				committed = 0
			}
			if lag := newest - committed; lag > 0 {
				totalLag += lag
			}
		}
	}
	return totalLag, nil
}
