package auth

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/spf13/viper"
)

type Permission string

const (
	PermissionRead  Permission = "read"
	PermissionWrite Permission = "write"
)

type KeyConfig struct {
	Name        string   `mapstructure:"name"`
	Key         string   `mapstructure:"key"`
	Permissions []string `mapstructure:"permissions"`
}

type Authenticator struct {
	keyMap map[string]map[Permission]bool
}

func NewAuthenticator(v *viper.Viper) (*Authenticator, error) {
	var keys []KeyConfig
	if err := v.UnmarshalKey("auth.keys", &keys); err != nil {
		return nil, err
	}

	keyMap := make(map[string]map[Permission]bool)
	for _, keyCfg := range keys {
		if keyCfg.Key == "" {
			continue
		}
		perms := make(map[Permission]bool)
		for _, pStr := range keyCfg.Permissions {
			perms[Permission(pStr)] = true
		}
		keyMap[keyCfg.Key] = perms
		log.Printf("loaded auth key: %s (Permissions: %v)", keyCfg.Name, keyCfg.Permissions)
	}

	return &Authenticator{keyMap: keyMap}, nil
}

func (a *Authenticator) RequireAuth(required Permission) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, err := extractKey(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			permissions, ok := a.keyMap[key]
			if !ok {
				http.Error(w, "invalid api key", http.StatusUnauthorized)
				return
			}

			if !permissions[required] {
				http.Error(w, "forbidden: key does not have required '"+string(required)+"' permission", http.StatusForbidden)
				return
			}

			ctx := context.WithValue(r.Context(), "api_key", key)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractKey(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
			return parts[1], nil
		}
		return "", fmt.Errorf("invalid Authorization header format")
	}

	apiKeyHeader := r.Header.Get("X-API-Key")
	if apiKeyHeader != "" {
		return apiKeyHeader, nil
	}

	return "", fmt.Errorf("api key not found")
}
