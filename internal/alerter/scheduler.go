package alerter

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/vantutran2k1/aura/gen/go/aura/v1"
)

type Scheduler struct {
	store       *RuleStore
	notifier    Notifier
	queryClient pb.StorageServiceClient
	ticker      *time.Ticker
}

func NewScheduler(store *RuleStore, notifier Notifier, qc pb.StorageServiceClient, interval time.Duration) *Scheduler {
	return &Scheduler{
		store:       store,
		notifier:    notifier,
		queryClient: qc,
		ticker:      time.NewTicker(interval),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("scheduler stopping")
			s.ticker.Stop()
			return nil
		case <-s.ticker.C:
			if err := s.checkRules(ctx); err != nil {
				log.Printf("error checking rules: %v", err)
			}
		}
	}
}

func (s *Scheduler) checkRules(ctx context.Context) error {
	rules, err := s.store.GetRulesToFire(ctx)
	if err != nil {
		return fmt.Errorf("failed to get rules: %w", err)
	}

	for _, rule := range rules {
		log.Printf("checking rule: %s", rule.Name)

		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		req := &pb.QueryLogsRequest{
			Query:             rule.Query,
			Limit:             100, // TODO: no hardcoded value here
			StartTimeUnixNano: time.Now().Add(-time.Duration(rule.IntervalSeconds) * time.Second).UnixNano(),
			EndTimeUnixNano:   time.Now().UnixNano(),
		}

		resp, err := s.queryClient.QueryLogs(queryCtx, req)
		cancel()

		if err != nil {
			log.Printf("error querying storage for rule %d: %v", rule.ID, err)
			continue
		}

		logCount := len(resp.Logs)
		isFiring := logCount > rule.Threshold

		if isFiring && rule.CurrentState == "OK" {
			log.Printf("alert firing: %s", rule.Name)
			msg := fmt.Sprintf("alert %s is firing: found %d logs (threshold > %d)", rule.Name, logCount, rule.Threshold)
			if err := s.notifier.Notify(ctx, rule, msg); err != nil {
				log.Printf("notify error: %v", err)
			}
			s.store.UpdateRuleState(ctx, rule.ID, "FIRING", true)
		} else if !isFiring && rule.CurrentState == "FIRING" {
			log.Printf("alert resolved: %s", rule.Name)
			msg := fmt.Sprintf("alert %s is resolved: found %d logs (threshold <= %d)", rule.Name, logCount, rule.Threshold)
			s.notifier.Notify(ctx, rule, msg)
			s.store.UpdateRuleState(ctx, rule.ID, "OK", false)
		} else {
			s.store.UpdateRuleState(ctx, rule.ID, rule.CurrentState, false)
		}
	}

	return nil
}
