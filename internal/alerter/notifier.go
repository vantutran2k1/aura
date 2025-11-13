package alerter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
)

type Notifier interface {
	Notify(ctx context.Context, rule Rule, message string) error
}

type WebhookNotifier struct {
	client *http.Client
}

func NewWebhookNotifier() *WebhookNotifier {
	return &WebhookNotifier{
		client: &http.Client{},
	}
}

func (n *WebhookNotifier) Notify(ctx context.Context, rule Rule, message string) error {
	payload := fmt.Sprintf(`{"rule_name": "%s", "message": "%s"}`, rule.Name, message)

	req, err := http.NewRequestWithContext(ctx, "POST", rule.TargetURL, bytes.NewBufferString(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook failed with status code %d", resp.StatusCode)
	}

	return nil
}
