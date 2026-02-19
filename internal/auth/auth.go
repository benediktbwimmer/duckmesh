package auth

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type Identity struct {
	TenantID string
	Roles    []string
}

func (i Identity) HasRole(role string) bool {
	for _, candidate := range i.Roles {
		if candidate == role {
			return true
		}
	}
	return false
}

type APIKeyValidator interface {
	Validate(ctx context.Context, apiKey string) (Identity, bool)
}

type StaticAPIKeyValidator struct {
	keys map[string]Identity
}

func NewStaticAPIKeyValidator(spec string) (*StaticAPIKeyValidator, error) {
	validator := &StaticAPIKeyValidator{keys: map[string]Identity{}}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return validator, nil
	}

	entries := strings.Split(spec, ",")
	for _, entry := range entries {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid static key entry %q: expected key:tenant:role|role", entry)
		}
		key := strings.TrimSpace(parts[0])
		tenant := strings.TrimSpace(parts[1])
		if key == "" || tenant == "" {
			return nil, fmt.Errorf("invalid static key entry %q: empty key/tenant", entry)
		}
		roleParts := strings.Split(strings.TrimSpace(parts[2]), "|")
		roles := make([]string, 0, len(roleParts))
		for _, role := range roleParts {
			role = strings.TrimSpace(role)
			if role == "" {
				continue
			}
			roles = append(roles, role)
		}
		if len(roles) == 0 {
			return nil, fmt.Errorf("invalid static key entry %q: at least one role is required", entry)
		}
		sort.Strings(roles)
		validator.keys[key] = Identity{TenantID: tenant, Roles: roles}
	}

	return validator, nil
}

func (v *StaticAPIKeyValidator) Validate(_ context.Context, apiKey string) (Identity, bool) {
	identity, ok := v.keys[apiKey]
	return identity, ok
}
