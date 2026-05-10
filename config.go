package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	lua "github.com/yuin/gopher-lua"
)

//go:embed admin.html style.css prism-code.js
var embedFS embed.FS

var (
	rootDir        string
	appSettings    Settings
	ProdEnv        *Environment
	StagingEnv     *Environment
	restoreMu      sync.Mutex
	stagingMu      sync.Mutex
	stagingStarted bool
)

type middlewareCacheEntry struct {
	Proto   *lua.FunctionProto
	ModTime time.Time
	Err     error
}

type Environment struct {
	IsStaging      bool
	BaseDir        string
	DataPath       string
	PublicPath     string
	RoutesPath     string
	ScriptsPath    string
	UploadsPath    string
	MiddlewarePath string

	ConfigDBConn    *sql.DB
	DataDBConn      *sql.DB
	LogDBConn       *sql.DB
	SchemaCache     sync.Map
	Scheduler       gocron.Scheduler
	LuaPool         *sync.Pool
	Routes          []Route
	RoutesMu        sync.RWMutex
	middlewareCache sync.Map
}

func NewEnvironment(baseDir string, isStaging bool) *Environment {
	env := &Environment{
		IsStaging:      isStaging,
		BaseDir:        baseDir,
		DataPath:       filepath.Join(baseDir, "data"),
		PublicPath:     filepath.Join(baseDir, "public"),
		RoutesPath:     filepath.Join(baseDir, "routes"),
		ScriptsPath:    filepath.Join(baseDir, "scripts"),
		UploadsPath:    filepath.Join(baseDir, "uploads"),
		MiddlewarePath: filepath.Join(baseDir, "middleware"),
	}
	os.MkdirAll(env.DataPath, os.ModePerm)
	os.MkdirAll(env.ScriptsPath, os.ModePerm)
	os.MkdirAll(env.RoutesPath, os.ModePerm)
	os.MkdirAll(env.PublicPath, os.ModePerm)
	os.MkdirAll(env.UploadsPath, os.ModePerm)
	os.MkdirAll(env.MiddlewarePath, os.ModePerm)
	return env
}

type ApiKey struct {
	ID          int            `json:"id"`
	Key         string         `json:"key"`
	Name        string         `json:"name"`
	Permissions map[string]any `json:"permissions"`
	Created     int64          `json:"created"`
}

type Settings struct {
	ApiKeys         []ApiKey `json:"api_keys"`
	BackupDestDir   string   `json:"backup_dest_dir"`
	BackupType      string   `json:"backup_type"`
	BackupRetention int      `json:"backup_retention"`
	BackupSched     string   `json:"backup_sched"`
	BackupSchedMeta string   `json:"backup_sched_meta"`

	AllowedOrigins  string  `json:"allowed_origins"`
	RateLimitRPS    float64 `json:"rate_limit_rps"`
	RateLimitBurst  int     `json:"rate_limit_burst"`
	AdminMaxRetries int     `json:"admin_max_retries"`

	LogRetentionDays  int  `json:"log_retention_days"`
	MaxLogCountPerReq int  `json:"max_log_count_per_req"`
	UnsafeLua         bool `json:"unsafe_lua"`
	Port              int  `json:"port"`

	StagingEnabled bool `json:"staging_enabled"`
	StagingPort    int  `json:"staging_port"`

	RoutesPath     string   `json:"routes_path"`
	MiddlewarePath string   `json:"middleware_path"`
	PublicPath     string   `json:"public_path"`
	ScriptsPath    string   `json:"scripts_path"`
	UploadsPath    string   `json:"uploads_path"`
	CustomDirs     []string `json:"custom_dirs"`
}

func resolvePath(baseDir, customPath, defaultName string) string {
	if customPath == "" {
		return filepath.Join(baseDir, defaultName)
	}
	if filepath.IsAbs(customPath) {
		return customPath
	}
	return filepath.Join(baseDir, customPath)
}

func applyPathsToEnv(env *Environment, s Settings) {
	env.RoutesPath = resolvePath(env.BaseDir, s.RoutesPath, "routes")
	env.MiddlewarePath = resolvePath(env.BaseDir, s.MiddlewarePath, "middleware")
	env.PublicPath = resolvePath(env.BaseDir, s.PublicPath, "public")
	env.ScriptsPath = resolvePath(env.BaseDir, s.ScriptsPath, "scripts")
	env.UploadsPath = resolvePath(env.BaseDir, s.UploadsPath, "uploads")

	os.MkdirAll(env.RoutesPath, os.ModePerm)
	os.MkdirAll(env.MiddlewarePath, os.ModePerm)
	os.MkdirAll(env.PublicPath, os.ModePerm)
	os.MkdirAll(env.ScriptsPath, os.ModePerm)
	os.MkdirAll(env.UploadsPath, os.ModePerm)
}

func loadSettings() {
	appSettings.RateLimitRPS = 100
	appSettings.RateLimitBurst = 200
	appSettings.AdminMaxRetries = 5
	appSettings.LogRetentionDays = 30
	appSettings.MaxLogCountPerReq = 100
	appSettings.Port = 8080
	appSettings.StagingPort = 8090
	appSettings.StagingEnabled = false
	appSettings.BackupType = "complete"
	appSettings.BackupRetention = 10

	b, err := os.ReadFile(filepath.Join(ProdEnv.DataPath, "settings.json"))
	if err == nil {
		json.Unmarshal(b, &appSettings)
	}

	applyPathsToEnv(ProdEnv, appSettings)
	for _, dir := range appSettings.CustomDirs {
		os.MkdirAll(resolvePath(rootDir, dir, dir), os.ModePerm)
	}

	ProdEnv.initLuaPool()
}

func saveSettings(s Settings) {
	b, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(filepath.Join(ProdEnv.DataPath, "settings.json"), b, 0644)
	appSettings = s

	applyPathsToEnv(ProdEnv, appSettings)
	for _, dir := range appSettings.CustomDirs {
		os.MkdirAll(resolvePath(rootDir, dir, dir), os.ModePerm)
	}

	ProdEnv.initLuaPool()
	if StagingEnv != nil {
		StagingEnv.initLuaPool()
	}
}

// Request Logger mapping for DB and Lua logs
type ctxKeyLogger struct{}

type RequestLogger struct {
	Count int
	Limit int
	IP    string
	Env   *Environment
	Mu    sync.Mutex
}

func (rl *RequestLogger) Log(level, scriptPath, contextStr, message string) {
	if rl == nil {
		return
	}
	rl.Mu.Lock()
	defer rl.Mu.Unlock()

	if rl.Limit > 0 && rl.Count > rl.Limit {
		return
	}
	if rl.Limit > 0 && rl.Count == rl.Limit {
		rl.Env.appLog("warn", scriptPath, rl.IP, "[system] Log limit reached for this request")
		rl.Count++
		return
	}
	rl.Count++
	// Fallback to Env formatting
	rl.Env.appLog(level, scriptPath, rl.IP, "["+contextStr+"] "+message)
}
