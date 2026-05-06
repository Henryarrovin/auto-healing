package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Henryarrovin/auto-healing/types"
)

const defaultOllamaURL = "http://ollama-service.auth.svc.cluster.local:11434"

type Diagnoser struct {
	baseURL string
	model   string
	client  *http.Client
}

func NewDiagnoser() *Diagnoser {
	base := os.Getenv("OLLAMA_URL")
	if base == "" {
		base = defaultOllamaURL
	}
	model := os.Getenv("OLLAMA_MODEL")
	if model == "" {
		model = "qwen2.5:1.5b"
	}
	return &Diagnoser{
		baseURL: base,
		model:   model,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

type generateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type generateResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
	Error    string `json:"error,omitempty"`
}

// sends the healing event to Ollama and returns a concise diagnosis.
func (d *Diagnoser) Diagnose(ctx context.Context, ev types.Event) (string, error) {
	reqBody, _ := json.Marshal(generateRequest{
		Model:  d.model,
		Prompt: buildPrompt(ev),
		Stream: false,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		d.baseURL+"/api/generate", bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama unreachable (%s): %w", d.baseURL, err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama %d: %s", resp.StatusCode, string(raw))
	}

	var result generateResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if result.Error != "" {
		return "", fmt.Errorf("ollama error: %s", result.Error)
	}

	log.Printf("[ollama] diagnosis for %s/%s (%d chars)", ev.Kind, ev.Name, len(result.Response))
	return result.Response, nil
}

// checks Ollama is reachable and warns if the model isn't pulled yet.
func (d *Diagnoser) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL+"/api/tags", nil)
	if err != nil {
		return err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("ollama not reachable at %s: %w", d.baseURL, err)
	}
	defer resp.Body.Close()

	var result struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse tags: %w", err)
	}
	for _, m := range result.Models {
		if m.Name == d.model || m.Name == d.model+":latest" {
			log.Printf("[ollama] model %q ready at %s", d.model, d.baseURL)
			return nil
		}
	}
	log.Printf("[ollama] WARNING: model %q not found — ensure the init container pulled it", d.model)
	return nil
}

func buildPrompt(ev types.Event) string {
	return fmt.Sprintf(`You are an expert SRE specialising in Kubernetes, Go microservices, PostgreSQL, and Kafka.
A production auto-healer has just fired. Give a concise diagnosis (3-5 sentences) then list 2-3 concrete follow-up steps a human engineer should take if the automatic fix does not resolve the issue. Be specific to the stack described.

Event kind:     %s
Resource name:  %s
Namespace:      %s
Trigger reason: %s

Cluster context / raw signal:
%s

Stack: Go gRPC services, PostgreSQL (payment_db + s3demo), Kafka, Kubernetes (minikube), Wire DI.
Reply in plain text only — no markdown headers, no bullet symbols, just numbered steps.`,
		ev.Kind, ev.Name, ev.Namespace, ev.Reason, ev.Raw)
}
