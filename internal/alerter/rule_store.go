package alerter

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Rule struct {
	ID              int
	Name            string
	Query           string
	Threshold       int
	IntervalSeconds int
	CurrentState    string
	TargetType      string
	TargetURL       string
}

type RuleStore struct {
	db *pgxpool.Pool
}

func NewRuleStore(db *pgxpool.Pool) *RuleStore {
	return &RuleStore{db: db}
}

func (s *RuleStore) GetRulesToFire(ctx context.Context) ([]Rule, error) {
	query := `
	SELECT id, name, query, threshold, interval_seconds, current_state, notification_target_type, notification_target_url 
	FROM alert_rules
	WHERE last_checked_at IS NULL OR last_checked_at <= NOW() - (interval_seconds * '1 second'::interval)
	`
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []Rule
	for rows.Next() {
		var r Rule
		if err := rows.Scan(&r.ID, &r.Name, &r.Query, &r.Threshold, &r.IntervalSeconds, &r.CurrentState, &r.TargetType, &r.TargetURL); err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func (s *RuleStore) UpdateRuleState(ctx context.Context, ruleID int, newState string, isFiring bool) error {
	var query string
	if isFiring {
		query = "UPDATE alert_rules SET current_state = $1, last_checked_at = NOW(), last_fired_at = NOW() WHERE id = $2"
	} else {
		query = "UPDATE alert_rules SET current_state = $1, last_checked_at = NOW() WHERE id = $2"
	}

	_, err := s.db.Exec(ctx, query, newState, ruleID)
	return err
}
