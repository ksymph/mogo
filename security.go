package main

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type TokenBucket struct {
	tokens float64
	last   time.Time
}

func (tb *TokenBucket) Allow(rps float64, burst int) bool {
	now := time.Now()
	elapsed := now.Sub(tb.last).Seconds()
	tb.tokens += elapsed * rps
	if tb.tokens > float64(burst) {
		tb.tokens = float64(burst)
	}
	tb.last = now
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

var (
	generalLimiters     = make(map[string]*TokenBucket)
	adminFailedAttempts = make(map[string]int)
	limiterMu           sync.Mutex
)

func allowRateLimit(ip string) bool {
	limiterMu.Lock()
	defer limiterMu.Unlock()

	rps := appSettings.RateLimitRPS
	burst := appSettings.RateLimitBurst

	if rps <= 0 {
		return true
	}

	lim, exists := generalLimiters[ip]
	if !exists {
		lim = &TokenBucket{tokens: float64(burst), last: time.Now()}
		generalLimiters[ip] = lim
	}
	return lim.Allow(rps, burst)
}

func isIpLockedOut(ip string) bool {
	if appSettings.AdminMaxRetries <= 0 {
		return false
	}
	limiterMu.Lock()
	defer limiterMu.Unlock()
	return adminFailedAttempts[ip] >= appSettings.AdminMaxRetries
}

func recordAdminAttempt(ip string, success bool) {
	if appSettings.AdminMaxRetries <= 0 {
		return
	}
	limiterMu.Lock()
	defer limiterMu.Unlock()
	if success {
		delete(adminFailedAttempts, ip)
	} else {
		adminFailedAttempts[ip]++
	}
}

func setAuthCookie(w http.ResponseWriter, key string) {
	http.SetCookie(w, &http.Cookie{Name: "mogo_token", Value: key, Path: "/", HttpOnly: true, MaxAge: 86400 * 7})
}

func getIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		ip = strings.Split(fwd, ",")[0]
	}
	return strings.TrimSpace(ip)
}

func adminLockoutMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if isIpLockedOut(getIP(r)) {
			http.Error(w, "Too Many Failed Attempts", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

func (env *Environment) corsAndRateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origins := appSettings.AllowedOrigins
		if origins == "" {
			origins = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origins)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if !allowRateLimit(getIP(r)) {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

func (env *Environment) adminMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return adminLockoutMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var key string
		if cookie, err := r.Cookie("mogo_token"); err == nil {
			key = cookie.Value
		} else if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			key = strings.TrimPrefix(auth, "Bearer ")
		}

		if key == "" {
			http.Error(w, "Unauthorized", 401)
			return
		}

		ip := getIP(r)
		var foundKey *ApiKey
		for _, ak := range appSettings.ApiKeys {
			if ak.Key == key {
				foundKey = &ak
				break
			}
		}

		if foundKey != nil {
			p := foundKey.Permissions
			if env.IsStaging {
				if val, ok := p["staging"].(bool); !ok || !val {
					http.Error(w, "Forbidden: Staging access denied", 403)
					return
				}
			} else {
				if val, ok := p["production"].(bool); !ok || !val {
					http.Error(w, "Forbidden: Production access denied", 403)
					return
				}
			}

			recordAdminAttempt(ip, true)
			permsBytes, _ := json.Marshal(p)
			r.Header.Set("X-Admin-Perms", string(permsBytes))
			next(w, r)
			return
		}

		recordAdminAttempt(ip, false)
		http.Error(w, "Invalid API Key", 401)
	})
}

func getPermissions(r *http.Request) map[string]any {
	var p map[string]any
	perms := r.Header.Get("X-Admin-Perms")
	if perms != "" {
		json.Unmarshal([]byte(perms), &p)
	}
	return p
}

func hasPermission(r *http.Request, section string) bool {
	p := getPermissions(r)
	if p == nil {
		return false
	}
	val, ok := p[section].(bool)
	return ok && val
}

func hasCollectionPermission(r *http.Request, collection string, action string) bool {
	p := getPermissions(r)
	if p == nil {
		return false
	}
	colls, ok := p["collections"].(map[string]any)
	if !ok {
		return false
	}

	check := func(cName string) bool {
		actionsRaw, ok := colls[cName]
		if !ok {
			return false
		}
		actions, ok := actionsRaw.([]any)
		if !ok {
			return false
		}
		for _, a := range actions {
			if str, isStr := a.(string); isStr && str == action {
				return true
			}
		}
		return false
	}

	return check("*") || check(collection)
}
