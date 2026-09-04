package auth

import (
"context"
"errors"
"net/http"
"strings"
"time"

"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const (
UserContextKey contextKey = "user_claims"
APIKeyHeader   string     = "X-API-Key"
)

type Claims struct {
Subject string `json:"sub"`
Role    string `json:"role"`
jwt.RegisteredClaims
}

type AuthGuard struct {
jwtSecret []byte
validKeys map[string]string // apiKey -> clientName
}

func NewAuthGuard(jwtSecret string, apiKeys map[string]string) *AuthGuard {
return &AuthGuard{
jwtSecret: []byte(jwtSecret),
validKeys: apiKeys,
}
}

// GenerateJWT creates a new token valid for the given duration
func (a *AuthGuard) GenerateJWT(subject, role string, ttl time.Duration) (string, error) {
claims := Claims{
Subject: subject,
Role:    role,
RegisteredClaims: jwt.RegisteredClaims{
ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)),
IssuedAt:  jwt.NewNumericDate(time.Now()),
},
}
token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
return token.SignedString(a.jwtSecret)
}

// Middleware protects endpoints requiring API Key or JWT authentication
func (a *AuthGuard) Middleware(next http.Handler) http.Handler {
return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 1. Check API Key
if apiKey := r.Header.Get(APIKeyHeader); apiKey != "" {
if clientName, exists := a.validKeys[apiKey]; exists {
ctx := context.WithValue(r.Context(), UserContextKey, &Claims{Subject: clientName, Role: "service"})
next.ServeHTTP(w, r.WithContext(ctx))
return
}
http.Error(w, `{"error":"Invalid API Key"}`, http.StatusUnauthorized)
return
}

// 2. Check JWT Bearer Token
authHeader := r.Header.Get("Authorization")
if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
http.Error(w, `{"error":"Missing or malformed Authorization header"}`, http.StatusUnauthorized)
return
}

tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
claims := &Claims{}

token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
return nil, errors.New("unexpected signing method")
}
return a.jwtSecret, nil
})

if err != nil || !token.Valid {
http.Error(w, `{"error":"Invalid or expired token"}`, http.StatusUnauthorized)
return
}

ctx := context.WithValue(r.Context(), UserContextKey, claims)
next.ServeHTTP(w, r.WithContext(ctx))
})
}

