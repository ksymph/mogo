package main

import (
	"archive/zip"
	"context"
	crand "crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-co-op/gocron/v2"
	lua "github.com/yuin/gopher-lua"
	_ "modernc.org/sqlite"
)

//go:embed admin.html style.css prism-code.js
var embedFS embed.FS

var (
	rootDir      string
	appSettings  Settings
	ProdEnv      *Environment
	StagingEnv   *Environment
	restoreMu    sync.Mutex
	stagingMu    sync.Mutex
	stagingStarted bool
)

type Environment struct {
	IsStaging   bool
	BaseDir     string
	DataPath    string
	PublicPath  string
	RoutesPath  string
	ScriptsPath string
	UploadsPath string

	ConfigDBConn *sql.DB
	DataDBConn   *sql.DB
	LogDBConn    *sql.DB
	SchemaCache  sync.Map
	Scheduler    gocron.Scheduler
	LuaPool      *sync.Pool
	Routes       []Route
	RoutesMu     sync.RWMutex
}

func NewEnvironment(baseDir string, isStaging bool) *Environment {
	env := &Environment{
		IsStaging:   isStaging,
		BaseDir:     baseDir,
		DataPath:    filepath.Join(baseDir, "data"),
		PublicPath:  filepath.Join(baseDir, "public"),
		RoutesPath:  filepath.Join(baseDir, "routes"),
		ScriptsPath: filepath.Join(baseDir, "scripts"),
		UploadsPath: filepath.Join(baseDir, "uploads"),
	}
	os.MkdirAll(env.DataPath, os.ModePerm)
	os.MkdirAll(env.ScriptsPath, os.ModePerm)
	os.MkdirAll(env.RoutesPath, os.ModePerm)
	os.MkdirAll(env.PublicPath, os.ModePerm)
	os.MkdirAll(env.UploadsPath, os.ModePerm)
	return env
}

// -----------------------------------------------------------------------------
// 0. SETTINGS & CONFIG
// -----------------------------------------------------------------------------
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

	AllowedOrigins string  `json:"allowed_origins"`
	RateLimitRPS   float64 `json:"rate_limit_rps"`
	RateLimitBurst int     `json:"rate_limit_burst"`
	AdminMaxRetries int     `json:"admin_max_retries"`

	LogRetentionDays int  `json:"log_retention_days"`
	UnsafeLua        bool `json:"unsafe_lua"`
	Port             int  `json:"port"`

	StagingEnabled bool `json:"staging_enabled"`
	StagingPort    int  `json:"staging_port"`
}

func loadSettings() {
	appSettings.RateLimitRPS = 100
	appSettings.RateLimitBurst = 200
	appSettings.AdminMaxRetries = 5
	appSettings.LogRetentionDays = 30
	appSettings.Port = 8080
	appSettings.StagingPort = 8090
	appSettings.StagingEnabled = false
	appSettings.BackupType = "complete"
	appSettings.BackupRetention = 10

	b, err := os.ReadFile(filepath.Join(ProdEnv.DataPath, "settings.json"))
	if err == nil {
		json.Unmarshal(b, &appSettings)
	}

	ProdEnv.initLuaPool()
}

func saveSettings(s Settings) {
	b, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(filepath.Join(ProdEnv.DataPath, "settings.json"), b, 0644)
	appSettings = s
	ProdEnv.initLuaPool()
	if StagingEnv != nil {
		StagingEnv.initLuaPool()
	}
}

// -----------------------------------------------------------------------------
// 1. DATABASE, CACHE & LIMITERS
// -----------------------------------------------------------------------------
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

func (env *Environment) initDB() {
	var err error
	
	configPath := filepath.Join(env.DataPath, "config.sqlite")
	env.ConfigDBConn, err = sql.Open("sqlite", configPath)
	if err != nil {
		log.Fatalf("Failed to open Config database: %v", err)
	}
	env.initSystemTables()
	if !env.IsStaging {
		env.initMasterKey()
	}

	dataPath := filepath.Join(env.DataPath, "data.sqlite")
	env.DataDBConn, err = sql.Open("sqlite", dataPath)
	if err != nil {
		log.Fatalf("Failed to open Data database: %v", err)
	}
	env.bootstrapDataDB()

	logDBPath := filepath.Join(env.DataPath, "log.sqlite")
	env.LogDBConn, err = sql.Open("sqlite", logDBPath+"?_journal_mode=WAL")
	if err != nil {
		log.Fatalf("Failed to open Log database: %v", err)
	}
	_, err = env.LogDBConn.Exec(`CREATE TABLE IF NOT EXISTS _logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		level TEXT,
		script_path TEXT,
		origin TEXT,
		message TEXT
	)`)
	if err != nil {
		log.Printf("Error creating logs table: %v", err)
	}
}

func (env *Environment) bootstrapDataDB() {
	rows, err := env.ConfigDBConn.Query("SELECT name FROM _collections ORDER BY id ASC")
	if err != nil {
		return
	}
	defer rows.Close()

	var collections []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		collections = append(collections, name)
	}

	for _, cName := range collections {
		schema := env.getCollectionSchema(cName)

		sqlFields := []string{"id INTEGER PRIMARY KEY AUTOINCREMENT", "created INTEGER", "updated INTEGER"}
		for _, field := range schema {
			sqlType := "TEXT"
			switch field.Type {
			case "number":
				sqlType = "REAL"
			case "bool":
				sqlType = "BOOLEAN"
			}
			sqlFields = append(sqlFields, fmt.Sprintf("%s %s", field.Name, sqlType))
		}
		createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", cName, strings.Join(sqlFields, ", "))
		_, err := env.DataDBConn.Exec(createSQL)
		if err != nil {
			log.Printf("Failed to bootstrap table %s: %v", cName, err)
		}
	}
}

func (env *Environment) appLog(level, scriptPath, origin, message string) {
	if env.LogDBConn == nil {
		return
	}
	_, err := env.LogDBConn.Exec(`INSERT INTO _logs (level, script_path, origin, message) VALUES (?, ?, ?, ?)`, level, scriptPath, origin, message)
	if err != nil {
		log.Printf("Failed to write to app log: %v", err)
	}
}

func (env *Environment) initSystemTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS _collections (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			type TEXT DEFAULT 'base',
			created INTEGER,
			updated INTEGER
		);`,
		`CREATE TABLE IF NOT EXISTS _schema (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			collection_id INTEGER,
			field TEXT NOT NULL,
			type TEXT NOT NULL,
			required BOOLEAN DEFAULT 0,
			position INTEGER DEFAULT 0,
			FOREIGN KEY(collection_id) REFERENCES _collections(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS _cron_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			schedule TEXT NOT NULL,
			schedule_meta TEXT,
			script_path TEXT NOT NULL,
			active BOOLEAN DEFAULT 1,
			prevent_overlap BOOLEAN DEFAULT 0,
			created INTEGER
		);`,
	}
	for _, q := range queries {
		if _, err := env.ConfigDBConn.Exec(q); err != nil {
			log.Fatalf("System Table Init Error: %v", err)
		}
	}

	env.ConfigDBConn.Exec("ALTER TABLE _cron_jobs ADD COLUMN schedule_meta TEXT")
	env.ConfigDBConn.Exec("ALTER TABLE _schema ADD COLUMN position INTEGER DEFAULT 0")
	env.ConfigDBConn.Exec("ALTER TABLE _cron_jobs ADD COLUMN prevent_overlap BOOLEAN DEFAULT 0")
}

func (env *Environment) initMasterKey() {
	if len(appSettings.ApiKeys) == 0 {
		b := make([]byte, 16)
		crand.Read(b)
		newKey := "sk_" + hex.EncodeToString(b)

		var allPerms map[string]any
		json.Unmarshal([]byte(`{"production":true,"staging":true,"collections":{"*":["item","schema"]},"routes":true,"schedules":true,"public":true,"settings":true,"keys":true,"logs":true}`), &allPerms)

		appSettings.ApiKeys = append(appSettings.ApiKeys, ApiKey{
			ID:          1,
			Key:         newKey,
			Name:        "admin",
			Permissions: allPerms,
			Created:     time.Now().Unix(),
		})
		saveSettings(appSettings)
		
		// Generate a default .gitignore on fresh install
		gitIgnorePath := filepath.Join(rootDir, ".gitignore")
		if _, err := os.Stat(gitIgnorePath); os.IsNotExist(err) {
			ignoreContent := "data/data.sqlite\ndata/log.sqlite\ndata/settings.json\ndata/backups/\nstaging/\nuploads/\n"
			os.WriteFile(gitIgnorePath, []byte(ignoreContent), 0644)
		}

		fmt.Println("\n=================================================================")
		fmt.Printf(" INITIAL API KEY: %s\n", newKey)
		fmt.Printf(" Auto-login link: http://localhost:%d/admin/?key=%s\n", appSettings.Port, newKey)
		fmt.Println("=================================================================")
	}
}

func generateNewMasterKey() {
	b := make([]byte, 16)
	crand.Read(b)
	newKey := "sk_" + hex.EncodeToString(b)

	var allPerms map[string]any
	json.Unmarshal([]byte(`{"production":true,"staging":true,"collections":{"*":["item","schema"]},"routes":true,"schedules":true,"public":true,"settings":true,"keys":true,"logs":true}`), &allPerms)

	maxID := 0
	for _, ak := range appSettings.ApiKeys {
		if ak.ID > maxID {
			maxID = ak.ID
		}
	}

	appSettings.ApiKeys = append(appSettings.ApiKeys, ApiKey{
		ID:          maxID + 1,
		Key:         newKey,
		Name:        "cli-generated",
		Permissions: allPerms,
		Created:     time.Now().Unix(),
	})
	saveSettings(appSettings)

	fmt.Println("\n=================================================================")
	fmt.Printf(" NEW API KEY GENERATED: %s\n", newKey)
	fmt.Println("=================================================================")
}

// -----------------------------------------------------------------------------
// 1.5. CRON JOBS & BACKUPS
// -----------------------------------------------------------------------------
func (env *Environment) initCron() {
	if env.Scheduler != nil {
		env.Scheduler.Shutdown()
	}

	s, err := gocron.NewScheduler()
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}
	env.Scheduler = s

	s.NewJob(
		gocron.DailyJob(1, gocron.NewAtTimes(gocron.NewAtTime(0, 0, 0))),
		gocron.NewTask(func() {
			retention := appSettings.LogRetentionDays
			if retention <= 0 {
				retention = 30
			}
			if env.LogDBConn != nil {
				_, err := env.LogDBConn.Exec(fmt.Sprintf("DELETE FROM _logs WHERE timestamp < datetime('now', '-%d days')", retention))
				if err != nil {
					env.appLog("error", "system", "cron", "Failed to prune logs: "+err.Error())
				}
			}
		}),
	)

	rows, err := env.ConfigDBConn.Query("SELECT id, schedule, schedule_meta, script_path, prevent_overlap FROM _cron_jobs WHERE active = 1")
	if err == nil {
		defer rows.Close()
		parseIntField := func(v any) int {
			switch val := v.(type) {
			case string:
				i, _ := strconv.Atoi(val)
				return i
			case float64:
				return int(val)
			default:
				return 0
			}
		}

		for rows.Next() {
			var id, preventOverlap int
			var schedule, scriptPath string
			var scheduleMeta sql.NullString
			rows.Scan(&id, &schedule, &scheduleMeta, &scriptPath, &preventOverlap)

			fullPath := filepath.Join(env.ScriptsPath, scriptPath)

			mode := "raw"
			var meta map[string]any
			if scheduleMeta.Valid && scheduleMeta.String != "" {
				if err := json.Unmarshal([]byte(scheduleMeta.String), &meta); err == nil {
					if m, ok := meta["mode"].(string); ok {
						mode = m
					}
				}
			}

			var jobDef gocron.JobDefinition
			if mode == "interval" {
				dur := time.Duration(parseIntField(meta["d"]))*24*time.Hour +
					time.Duration(parseIntField(meta["h"]))*time.Hour +
					time.Duration(parseIntField(meta["m"]))*time.Minute +
					time.Duration(parseIntField(meta["s"]))*time.Second
				if dur <= 0 {
					dur = time.Minute
				}
				jobDef = gocron.DurationJob(dur)
			} else {
				jobDef = gocron.CronJob(schedule, true)
			}

			opts := []gocron.JobOption{
				gocron.WithTags(strconv.Itoa(id)),
			}
			if preventOverlap == 1 {
				opts = append(opts, gocron.WithSingletonMode(gocron.LimitModeReschedule))
			}

			_, err := env.Scheduler.NewJob(
				jobDef,
				gocron.NewTask(func() { env.runCronScript(fullPath) }),
				opts...,
			)
			if err != nil {
				log.Printf("Failed to load cron job %d: %v", id, err)
			}
		}
	}

	if !env.IsStaging && appSettings.BackupSched != "" && appSettings.BackupSched != "Disabled" {
		mode := "raw"
		var meta map[string]any
		if appSettings.BackupSchedMeta != "" {
			if err := json.Unmarshal([]byte(appSettings.BackupSchedMeta), &meta); err == nil {
				if m, ok := meta["mode"].(string); ok {
					mode = m
				}
			}
		}

		var jobDef gocron.JobDefinition
		if mode == "interval" {
			parseIntField := func(v any) int {
				switch val := v.(type) {
				case string:
					i, _ := strconv.Atoi(val)
					return i
				case float64:
					return int(val)
				default:
					return 0
				}
			}
			dur := time.Duration(parseIntField(meta["d"]))*24*time.Hour +
				time.Duration(parseIntField(meta["h"]))*time.Hour +
				time.Duration(parseIntField(meta["m"]))*time.Minute +
				time.Duration(parseIntField(meta["s"]))*time.Second
			if dur <= 0 {
				dur = time.Minute
			}
			jobDef = gocron.DurationJob(dur)
		} else {
			jobDef = gocron.CronJob(appSettings.BackupSched, true)
		}

		_, err := env.Scheduler.NewJob(
			jobDef,
			gocron.NewTask(func() {
				createBackup(appSettings.BackupDestDir, appSettings.BackupType)
			}),
		)
		if err != nil {
			log.Printf("Failed to load backup cron job: %v", err)
		}
	}

	env.Scheduler.Start()
}

func (env *Environment) runCronScript(scriptPath string) error {
	L := env.LuaPool.Get().(*lua.LState)
	defer env.LuaPool.Put(L)
	top := L.GetTop()
	defer L.SetTop(top)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	L.SetContext(ctx)
	defer L.RemoveContext()

	fn, err := L.LoadFile(scriptPath)
	if err != nil {
		log.Printf("Cron error loading %s: %v", scriptPath, err)
		return err
	}

	luaEnv := L.NewTable()
	mt := L.NewTable()
	L.SetField(mt, "__index", L.Get(lua.GlobalsIndex))
	L.SetMetatable(luaEnv, mt)
	fn.Env = luaEnv

	L.Push(fn)
	if err := L.PCall(0, 0, nil); err != nil {
		log.Printf("Cron error running %s: %v", scriptPath, err)
		return err
	}
	
	return nil
}

func zipSource(w *zip.Writer, source, prefix string) error {
	info, err := os.Stat(source)
	if err != nil {
		return nil // Ignore non-existent sources
	}

	if info.IsDir() {
		return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			header, err := zip.FileInfoHeader(info)
			if err != nil {
				return err
			}
			header.Name = filepath.ToSlash(filepath.Join(prefix, relPath))
			header.Method = zip.Deflate

			writer, err := w.CreateHeader(header)
			if err != nil {
				return err
			}
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(writer, file)
			return err
		})
	}

	// It's a single file
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	header.Name = filepath.ToSlash(prefix)
	header.Method = zip.Deflate
	writer, err := w.CreateHeader(header)
	if err != nil {
		return err
	}
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(writer, file)
	return err
}

func createBackup(destDir string, backupType string) error {
	if destDir == "" {
		destDir = filepath.Join(ProdEnv.DataPath, "backups")
	}
	if backupType == "" {
		backupType = "complete" // Default to complete backup
	}
	os.MkdirAll(destDir, os.ModePerm)

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("%s_%s.zip", timestamp, backupType)
	dstPath := filepath.Join(destDir, filename)

	outFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer outFile.Close()

	w := zip.NewWriter(outFile)
	defer w.Close()

	pathsToBackup := make(map[string]string)
	switch backupType {
	case "content":
		pathsToBackup[filepath.Join(rootDir, "data", "data.sqlite")] = "data/data.sqlite"
		pathsToBackup[filepath.Join(rootDir, "uploads")] = "uploads"
	case "template":
		pathsToBackup[filepath.Join(rootDir, "data", "config.sqlite")] = "data/config.sqlite"
		pathsToBackup[filepath.Join(rootDir, "routes")] = "routes"
		pathsToBackup[filepath.Join(rootDir, "scripts")] = "scripts"
		pathsToBackup[filepath.Join(rootDir, "public")] = "public"
		pathsToBackup[filepath.Join(rootDir, ".gitignore")] = ".gitignore" // Added
	case "complete":
		fallthrough
	default:
		pathsToBackup[filepath.Join(rootDir, "data", "config.sqlite")] = "data/config.sqlite"
		pathsToBackup[filepath.Join(rootDir, "data", "data.sqlite")] = "data/data.sqlite"
		pathsToBackup[filepath.Join(rootDir, "data", "settings.json")] = "data/settings.json"
		pathsToBackup[filepath.Join(rootDir, "routes")] = "routes"
		pathsToBackup[filepath.Join(rootDir, "scripts")] = "scripts"
		pathsToBackup[filepath.Join(rootDir, "public")] = "public"
		pathsToBackup[filepath.Join(rootDir, "uploads")] = "uploads"
		pathsToBackup[filepath.Join(rootDir, ".gitignore")] = ".gitignore" // Added
	}

	for source, prefix := range pathsToBackup {
		if err := zipSource(w, source, prefix); err != nil {
			log.Printf("Error adding %s to backup: %v", source, err)
		}
	}

	w.Close()
	outFile.Close()

	// Apply retention policy
	if appSettings.BackupRetention > 0 {
		files, err := os.ReadDir(destDir)
		if err != nil {
			return nil // Don't fail the whole backup for retention error
		}

		type backupFile struct {
			Path string
			Time time.Time
		}
		var backups []backupFile

		re := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_.*\.zip$`)
		for _, file := range files {
			if !file.IsDir() {
				matches := re.FindStringSubmatch(file.Name())
				if len(matches) > 1 {
					t, err := time.Parse("2006-01-02_15-04-05", matches[1])
					if err == nil {
						backups = append(backups, backupFile{Path: filepath.Join(destDir, file.Name()), Time: t})
					}
				}
			}
		}

		if len(backups) > appSettings.BackupRetention {
			sort.Slice(backups, func(i, j int) bool {
				return backups[i].Time.Before(backups[j].Time)
			})

			toDelete := len(backups) - appSettings.BackupRetention
			for i := 0; i < toDelete; i++ {
				os.Remove(backups[i].Path)
			}
		}
	}

	return nil
}

func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	os.MkdirAll(dest, 0755)

	for _, f := range r.File {
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip vulnerability
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)

		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}
	return nil
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}

		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()

		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()

		_, err = io.Copy(out, in)
		return err
	})
}

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

func (env *Environment) getCollectionSet() map[string]bool {
	colSet := make(map[string]bool)
	rows, err := env.ConfigDBConn.Query("SELECT name FROM _collections")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var n string
			rows.Scan(&n)
			colSet[n] = true
		}
	}
	return colSet
}

func (env *Environment) expandRecord(colName string, row map[string]any, cache map[string]map[string]any, activePath map[string]bool, colSet map[string]bool, forLua bool) map[string]any {
	idStr := fmt.Sprintf("%v", row["id"])
	cacheKey := colName + ":" + idStr

	if existing, ok := cache[cacheKey]; ok {
		if activePath[cacheKey] && !forLua {
			// JSON cycle break: return ID to prevent json.Marshal from panicking
			return map[string]any{"id": row["id"]}
		}
		return existing
	}

	out := make(map[string]any)
	for k, v := range row {
		out[k] = v
	}
	cache[cacheKey] = out
	activePath[cacheKey] = true

	schema := env.getCollectionSchema(colName)

	for _, sf := range schema {
		if colSet[sf.Type] {
			val := row[sf.Name]
			if val == nil || val == "" {
				continue
			}
			vStr := strings.TrimSpace(fmt.Sprintf("%v", val))
			if vStr == "" || vStr == "nil" {
				continue
			}

			var idList []any
			isArray := strings.HasPrefix(vStr, "[")
			if isArray {
				if err := json.Unmarshal([]byte(vStr), &idList); err != nil {
					idList = []any{vStr}
					isArray = false
				}
			} else {
				idList = []any{vStr}
			}

			var expanded []any
			for _, relId := range idList {
				relIdStr := fmt.Sprintf("%v", relId)
				relCacheKey := sf.Type + ":" + relIdStr

				if existing, ok := cache[relCacheKey]; ok {
					if activePath[relCacheKey] && !forLua {
						expanded = append(expanded, map[string]any{"id": relId})
					} else {
						expanded = append(expanded, existing)
					}
					continue
				}

				relRows, err := queryDB(env.DataDBConn, fmt.Sprintf("SELECT * FROM %s WHERE id = ?", sf.Type), relIdStr)
				if err == nil && len(relRows) > 0 {
					relExpanded := env.expandRecord(sf.Type, relRows[0], cache, activePath, colSet, forLua)
					expanded = append(expanded, relExpanded)
				} else {
					expanded = append(expanded, relId)
				}
			}

			if isArray {
				out[sf.Name] = expanded
			} else if len(expanded) > 0 {
				out[sf.Name] = expanded[0]
			}
		}
	}

	delete(activePath, cacheKey)
	return out
}

func toLValueCyclic(L *lua.LState, val interface{}, visited map[string]lua.LValue) lua.LValue {
	if val == nil {
		return lua.LNil
	}
	switch v := val.(type) {
	case map[string]interface{}:
		ptrStr := fmt.Sprintf("%p", v)
		if tbl, ok := visited[ptrStr]; ok {
			return tbl
		}
		tbl := L.NewTable()
		visited[ptrStr] = tbl
		for k, mapVal := range v {
			tbl.RawSetString(k, toLValueCyclic(L, mapVal, visited))
		}
		return tbl
	case []interface{}:
		ptrStr := fmt.Sprintf("%p", v)
		if tbl, ok := visited[ptrStr]; ok {
			return tbl
		}
		tbl := L.NewTable()
		visited[ptrStr] = tbl
		for i, sliceVal := range v {
			tbl.RawSetInt(i+1, toLValueCyclic(L, sliceVal, visited))
		}
		return tbl
	default:
		return toLValue(L, val)
	}
}

func (env *Environment) getCollectionSchema(collection string) []SchemaField {
	if cached, ok := env.SchemaCache.Load(collection); ok {
		return cached.([]SchemaField)
	}
	var schema []SchemaField = []SchemaField{}
	q := "SELECT s.field, s.type, s.required FROM _schema s JOIN _collections c ON s.collection_id = c.id WHERE c.name = ? ORDER BY s.position ASC, s.id ASC"
	rows, err := env.ConfigDBConn.Query(q, collection)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var field, typ string
			var required bool
			rows.Scan(&field, &typ, &required)
			schema = append(schema, SchemaField{Name: field, Type: typ, Required: required})
		}
	}
	env.SchemaCache.Store(collection, schema)
	return schema
}

func (env *Environment) createCollectionInDB(name string, schema []SchemaField) error {
	var exists int
	env.ConfigDBConn.QueryRow("SELECT COUNT(*) FROM _collections WHERE name = ?", name).Scan(&exists)
	if exists > 0 {
		return fmt.Errorf("collection already exists")
	}

	res, err := env.ConfigDBConn.Exec("INSERT INTO _collections (name, created, updated) VALUES (?, ?, ?)",
		name, time.Now().Unix(), time.Now().Unix())
	if err != nil {
		return err
	}
	collectionID, _ := res.LastInsertId()

	sqlFields := []string{"id INTEGER PRIMARY KEY AUTOINCREMENT", "created INTEGER", "updated INTEGER"}
	for i, field := range schema {
		sqlType := "TEXT"
		switch field.Type {
		case "number":
			sqlType = "REAL"
		case "bool":
			sqlType = "BOOLEAN"
		}
		env.ConfigDBConn.Exec("INSERT INTO _schema (collection_id, field, type, required, position) VALUES (?, ?, ?, ?, ?)",
			collectionID, field.Name, field.Type, field.Required, i)
		sqlFields = append(sqlFields, fmt.Sprintf("%s %s", field.Name, sqlType))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", name, strings.Join(sqlFields, ", "))
	_, err = env.DataDBConn.Exec(createSQL)
	env.SchemaCache.Delete(name)
	return err
}

func (env *Environment) updateCollectionInDB(name string, schema []SchemaField) error {
	var collectionID int
	err := env.ConfigDBConn.QueryRow("SELECT id FROM _collections WHERE name = ?", name).Scan(&collectionID)
	if err != nil {
		return fmt.Errorf("collection not found")
	}

	existingSchema := make(map[string]string)
	rows, err := env.ConfigDBConn.Query("SELECT field, type FROM _schema WHERE collection_id = ?", collectionID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var f, t string
			rows.Scan(&f, &t)
			existingSchema[f] = t
		}
	}

	newFields := make(map[string]bool)
	for i, field := range schema {
		newFields[field.Name] = true
		if _, exists := existingSchema[field.Name]; !exists {
			sqlType := "TEXT"
			switch field.Type {
			case "number":
				sqlType = "REAL"
			case "bool":
				sqlType = "BOOLEAN"
			}
			env.DataDBConn.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", name, field.Name, sqlType))
			env.ConfigDBConn.Exec("INSERT INTO _schema (collection_id, field, type, required, position) VALUES (?, ?, ?, ?, ?)",
				collectionID, field.Name, field.Type, field.Required, i)
		} else {
			env.ConfigDBConn.Exec("UPDATE _schema SET type = ?, required = ?, position = ? WHERE collection_id = ? AND field = ?", field.Type, field.Required, i, collectionID, field.Name)
		}
	}

	for existingField := range existingSchema {
		if !newFields[existingField] {
			env.DataDBConn.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", name, existingField))
			env.ConfigDBConn.Exec("DELETE FROM _schema WHERE collection_id = ? AND field = ?", collectionID, existingField)
		}
	}

	env.ConfigDBConn.Exec("UPDATE _collections SET updated = ? WHERE id = ?", time.Now().Unix(), collectionID)
	env.SchemaCache.Delete(name)
	return nil
}

func queryDB(db *sql.DB, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range cols {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		row := make(map[string]interface{})
		for i, col := range cols {
			if b, ok := vals[i].([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = vals[i]
			}
		}
		results = append(results, row)
	}
	return results, nil
}

// -----------------------------------------------------------------------------
// 2. SECURITY & AUTH & ROUTING
// -----------------------------------------------------------------------------
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

// -----------------------------------------------------------------------------
// 3. ADMIN API HANDLERS
// -----------------------------------------------------------------------------

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

func (env *Environment) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", 405)
		return
	}
	var creds struct {
		APIKey string `json:"api_key"`
	}
	json.NewDecoder(r.Body).Decode(&creds)

	ip := getIP(r)

	var foundKey *ApiKey
	for _, ak := range appSettings.ApiKeys {
		if ak.Key == creds.APIKey {
			foundKey = &ak
			break
		}
	}

	if foundKey != nil {
		p := foundKey.Permissions

		if env.IsStaging {
			if val, ok := p["staging"].(bool); !ok || !val {
				recordAdminAttempt(ip, false)
				http.Error(w, "Forbidden: Staging access denied", 403)
				return
			}
		} else {
			if val, ok := p["production"].(bool); !ok || !val {
				recordAdminAttempt(ip, false)
				http.Error(w, "Forbidden: Production access denied", 403)
				return
			}
		}

		recordAdminAttempt(ip, true)
		setAuthCookie(w, creds.APIKey)
		w.Write([]byte(`{"success":true}`))
		return
	}

	recordAdminAttempt(ip, false)
	http.Error(w, "Invalid API Key", 401)
}

func (env *Environment) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	envStr := "production"
	if env.IsStaging {
		envStr = "staging"
	}
	json.NewEncoder(w).Encode(map[string]string{
		"permissions": r.Header.Get("X-Admin-Perms"),
		"environment": envStr,
	})
}

func (env *Environment) handleAdminFilesRename(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", 405)
		return
	}

	var req struct {
		Base    string `json:"base"`
		OldPath string `json:"old_path"`
		NewPath string `json:"new_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	baseDir := env.PublicPath
	permKey := "public"

	if req.Base == "routes" {
		baseDir = env.RoutesPath
		permKey = "routes"
	} else if req.Base == "schedules" {
		baseDir = env.ScriptsPath
		permKey = "schedules"
	} else if req.Base == "uploads" {
		baseDir = env.UploadsPath
		permKey = "uploads"
	}

	if permKey == "uploads" {
		p := getPermissions(r)
		// Require at least some collection permissions to manipulate uploads
		if p == nil || p["collections"] == nil {
			http.Error(w, "Forbidden", 403)
			return
		}
	} else if !hasPermission(r, permKey) {
		http.Error(w, "Forbidden", 403)
		return
	}

	// Security: Prevent directory traversal
	if strings.Contains(req.OldPath, "..") || strings.Contains(req.NewPath, "..") {
		http.Error(w, "Invalid path provided", 400)
		return
	}

	oldFullPath := filepath.Join(baseDir, req.OldPath)
	newFullPath := filepath.Join(baseDir, req.NewPath)

	if _, err := os.Stat(oldFullPath); os.IsNotExist(err) {
		http.Error(w, "Source file or directory not found", 404)
		return
	}

	// Ensure the parent directory of the new destination path exists before moving
	if err := os.MkdirAll(filepath.Dir(newFullPath), os.ModePerm); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create destination directories: %v", err), 500)
		return
	}

	if err := os.Rename(oldFullPath, newFullPath); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Re-init routes if modifying routing system directories
	if req.Base == "routes" {
		env.initRoutes()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"success":true}`))
}

func (env *Environment) handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		json.NewEncoder(w).Encode(appSettings)
	} else if r.Method == "POST" {
		s := appSettings
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil && err != io.EOF {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		wasStagingEnabled := appSettings.StagingEnabled
		saveSettings(s)

		ProdEnv.initCron()
		if StagingEnv != nil {
			StagingEnv.initCron()
		}

		if !wasStagingEnabled && s.StagingEnabled {
			startStagingServer()
		}

		w.Write([]byte(`{"success":true}`))
	}
}

func (env *Environment) handleAdminBackupREST(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	destDir := appSettings.BackupDestDir
	if destDir == "" {
		destDir = filepath.Join(ProdEnv.DataPath, "backups")
	}

	if r.Method == "GET" {
		files, err := os.ReadDir(destDir)
		if err != nil {
			if os.IsNotExist(err) {
				json.NewEncoder(w).Encode([]string{})
				return
			}
			http.Error(w, err.Error(), 500)
			return
		}
		type backupInfo struct {
			Filename string `json:"filename"`
			Date     string `json:"date"`
			Type     string `json:"type"`
			Size     int64  `json:"size"`
		}
		var backups []backupInfo
		re := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_(\w+)\.zip$`)
		for _, file := range files {
			if !file.IsDir() {
				matches := re.FindStringSubmatch(file.Name())
				if len(matches) == 3 {
					info, err := file.Info()
					if err == nil {
						backups = append(backups, backupInfo{
							Filename: file.Name(),
							Date:     matches[1],
							Type:     matches[2],
							Size:     info.Size(),
						})
					}
				}
			}
		}
		sort.Slice(backups, func(i, j int) bool {
			return backups[i].Date > backups[j].Date
		})
		json.NewEncoder(w).Encode(backups)
	} else if r.Method == "POST" {
		var req struct {
			Type string `json:"type"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		backupType := req.Type
		if backupType == "" {
			backupType = appSettings.BackupType
		}
		err := createBackup(destDir, backupType)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"success":true}`))
	} else if r.Method == "DELETE" {
		filename := r.URL.Query().Get("file")
		if filename == "" || strings.Contains(filename, "..") {
			http.Error(w, "Invalid filename", 400)
			return
		}
		err := os.Remove(filepath.Join(destDir, filename))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"success":true}`))
	}
}

func (env *Environment) handleAdminBackupDownload(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	destDir := appSettings.BackupDestDir
	if destDir == "" {
		destDir = filepath.Join(ProdEnv.DataPath, "backups")
	}

	filename := r.URL.Query().Get("file")
	if filename == "" || strings.Contains(filename, "..") {
		http.Error(w, "Invalid filename", 400)
		return
	}

	filePath := filepath.Join(destDir, filename)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		http.Error(w, "File not found", 404)
		return
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filename))
	w.Header().Set("Content-Type", "application/zip")
	http.ServeFile(w, r, filePath)
}

func (env *Environment) handleAdminBackupRestore(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	restoreMu.Lock()
	defer restoreMu.Unlock()

	var req struct {
		File string `json:"file"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	destDir := appSettings.BackupDestDir
	if destDir == "" {
		destDir = filepath.Join(ProdEnv.DataPath, "backups")
	}
	zipPath := filepath.Join(destDir, req.File)
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		http.Error(w, "Backup file not found", 404)
		return
	}
	
	re := regexp.MustCompile(`_(\w+)\.zip$`)
	matches := re.FindStringSubmatch(req.File)
	if len(matches) < 2 {
		http.Error(w, "Could not determine backup type from filename", 400)
		return
	}
	backupType := matches[1]

	// 1. Close all DB connections
	ProdEnv.ConfigDBConn.Close()
	ProdEnv.DataDBConn.Close()
	ProdEnv.LogDBConn.Close()
	if StagingEnv != nil {
		StagingEnv.ConfigDBConn.Close()
		StagingEnv.DataDBConn.Close()
		StagingEnv.LogDBConn.Close()
	}

	// 2. Delete existing files based on backup type
	pathsToClear := map[string][]string{
		"content":  {"data/data.sqlite", "uploads"},
		"template": {"data/config.sqlite", "data/settings.json", "routes", "scripts", "public"},
		"complete": {"data/config.sqlite", "data/data.sqlite", "data/settings.json", "routes", "scripts", "public", "uploads"},
	}
	if paths, ok := pathsToClear[backupType]; ok {
		for _, p := range paths {
			fullPath := filepath.Join(rootDir, p)
			if info, err := os.Stat(fullPath); err == nil {
				if info.IsDir() {
					os.RemoveAll(fullPath)
				} else {
					os.Remove(fullPath)
				}
			}
		}
	} else {
		http.Error(w, "Unknown backup type for restore", 400)
		return
	}

	// 3. Unzip the backup
	if err := unzip(zipPath, rootDir); err != nil {
		log.Printf("CRITICAL: Restore failed during unzip: %v. The application may be in an inconsistent state. Please restart.", err)
		http.Error(w, "Failed to unzip backup: "+err.Error(), 500)
		return
	}

	// 4. Re-initialize the application state
	loadSettings() // Load new settings from restored file
	ProdEnv.initDB()
	ProdEnv.initRoutes()
	ProdEnv.initCron()
	
	stagingMu.Lock()
	stagingStarted = false // Allow staging to be restarted if needed
	stagingMu.Unlock()
	if appSettings.StagingEnabled {
		startStagingServer()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"success":true}`))
}

func (env *Environment) handleStagingSync(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	if StagingEnv == nil {
		http.Error(w, "Staging environment is not enabled", 400)
		return
	}
	var req struct {
		Direction   string `json:"direction"`
		Collections bool   `json:"collections"`
		Routes      bool   `json:"routes"`
		Public      bool   `json:"public"`
		Schedules   bool   `json:"schedules"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	var sourceEnv, targetEnv *Environment
	if req.Direction == "prod_to_staging" {
		sourceEnv = ProdEnv
		targetEnv = StagingEnv
	} else if req.Direction == "staging_to_prod" {
		sourceEnv = StagingEnv
		targetEnv = ProdEnv
	} else {
		http.Error(w, "Invalid direction", 400)
		return
	}

	if req.Public {
		os.RemoveAll(targetEnv.PublicPath)
		copyDir(sourceEnv.PublicPath, targetEnv.PublicPath)
	}
	if req.Routes {
		os.RemoveAll(targetEnv.RoutesPath)
		copyDir(sourceEnv.RoutesPath, targetEnv.RoutesPath)
		targetEnv.initRoutes()
	}
	if req.Schedules {
		os.RemoveAll(targetEnv.ScriptsPath)
		copyDir(sourceEnv.ScriptsPath, targetEnv.ScriptsPath)

		targetEnv.ConfigDBConn.Exec("DELETE FROM _cron_jobs")
		rows, err := sourceEnv.ConfigDBConn.Query("SELECT name, schedule, schedule_meta, script_path, active, prevent_overlap, created FROM _cron_jobs")
		if err == nil {
			for rows.Next() {
				var name, schedule, scriptPath string
				var active, preventOverlap int
				var scheduleMeta sql.NullString
				var createdRaw any
				rows.Scan(&name, &schedule, &scheduleMeta, &scriptPath, &active, &preventOverlap, &createdRaw)
				if req.Direction == "prod_to_staging" {
					active = 0
				}
				var created any = createdRaw
				if b, ok := createdRaw.([]byte); ok {
					created = string(b)
				}
				targetEnv.ConfigDBConn.Exec("INSERT INTO _cron_jobs (name, schedule, schedule_meta, script_path, active, prevent_overlap, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
					name, schedule, scheduleMeta, scriptPath, active, preventOverlap, created)
			}
			rows.Close()
		}
		targetEnv.initCron()
	}
	if req.Collections {
		var targetCollections []string
		tRows, _ := targetEnv.ConfigDBConn.Query("SELECT name FROM _collections")
		if tRows != nil {
			for tRows.Next() {
				var name string
				tRows.Scan(&name)
				targetCollections = append(targetCollections, name)
			}
			tRows.Close()
		}

		for _, tc := range targetCollections {
			targetEnv.DataDBConn.Exec("DROP TABLE IF EXISTS " + tc)
		}
		targetEnv.ConfigDBConn.Exec("DELETE FROM _collections")
		targetEnv.ConfigDBConn.Exec("DELETE FROM _schema")

		var collections []string
		rows, _ := sourceEnv.ConfigDBConn.Query("SELECT name FROM _collections ORDER BY id ASC")
		if rows != nil {
			for rows.Next() {
				var name string
				rows.Scan(&name)
				collections = append(collections, name)
			}
			rows.Close()
		}

		for _, cName := range collections {
			schema := sourceEnv.getCollectionSchema(cName)
			targetEnv.createCollectionInDB(cName, schema)
		}

		targetDBPath := filepath.Join(targetEnv.DataPath, "data.sqlite")
		conn, err := sourceEnv.DataDBConn.Conn(r.Context())
		if err == nil {
			defer conn.Close()
			_, err = conn.ExecContext(r.Context(), fmt.Sprintf("ATTACH DATABASE '%s' AS target_db", targetDBPath))
			if err == nil {
				for _, cName := range collections {
					conn.ExecContext(r.Context(), fmt.Sprintf("INSERT INTO target_db.%s SELECT * FROM main.%s", cName, cName))
				}
				conn.ExecContext(r.Context(), "DETACH DATABASE target_db")
			}
		}

		targetEnv.SchemaCache.Range(func(key, value interface{}) bool {
			targetEnv.SchemaCache.Delete(key)
			return true
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"success":true}`))
}

func (env *Environment) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "logs") {
		http.Error(w, "Forbidden", 403)
		return
	}
	if r.Method == "GET" {
		level := r.URL.Query().Get("level")
		whereClause := "1=1"
		args := []interface{}{}
		if level != "" {
			whereClause += " AND level = ?"
			args = append(args, level)
		}
		query := "SELECT * FROM _logs WHERE " + whereClause + " ORDER BY id DESC"
		logs, err := queryDB(env.LogDBConn, query, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var total int
		countQuery := "SELECT COUNT(*) FROM _logs WHERE " + whereClause
		env.LogDBConn.QueryRow(countQuery, args...).Scan(&total)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"logs":  logs,
			"total": total,
		})
	} else if r.Method == "DELETE" {
		_, err := env.LogDBConn.Exec("DELETE FROM _logs")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}
}

func (env *Environment) handleAdminKeys(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "keys") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	
	if r.Method == "GET" {
		if appSettings.ApiKeys == nil {
			json.NewEncoder(w).Encode([]ApiKey{})
			return
		}
		// Sort keys so newest appear first (matching old database ORDER BY id DESC)
		sortedKeys := make([]ApiKey, len(appSettings.ApiKeys))
		copy(sortedKeys, appSettings.ApiKeys)
		sort.Slice(sortedKeys, func(i, j int) bool {
			return sortedKeys[i].ID > sortedKeys[j].ID
		})
		json.NewEncoder(w).Encode(sortedKeys)
		return
	}
	if r.Method == "POST" {
		var req struct {
			ID          int            `json:"id"`
			Name        string         `json:"name"`
			Permissions map[string]any `json:"permissions"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if req.Name == "" {
			req.Name = "Unnamed Key"
		}

		if req.ID > 0 {
			for i, ak := range appSettings.ApiKeys {
				if ak.ID == req.ID {
					appSettings.ApiKeys[i].Name = req.Name
					appSettings.ApiKeys[i].Permissions = req.Permissions
					saveSettings(appSettings)
					w.Write([]byte(`{"success":true}`))
					return
				}
			}
			http.Error(w, "Key not found", 404)
			return
		}

		b := make([]byte, 16)
		crand.Read(b)
		newKey := "sk_" + hex.EncodeToString(b)
		
		maxID := 0
		for _, ak := range appSettings.ApiKeys {
			if ak.ID > maxID {
				maxID = ak.ID
			}
		}

		appSettings.ApiKeys = append(appSettings.ApiKeys, ApiKey{
			ID:          maxID + 1,
			Key:         newKey,
			Name:        req.Name,
			Permissions: req.Permissions,
			Created:     time.Now().Unix(),
		})
		saveSettings(appSettings)

		w.Write([]byte(`{"success":true, "key":"` + newKey + `"}`))
		return
	}
	if r.Method == "DELETE" {
		idStr := r.URL.Query().Get("id")
		id, _ := strconv.Atoi(idStr)
		
		var newKeys []ApiKey
		for _, ak := range appSettings.ApiKeys {
			if ak.ID != id {
				newKeys = append(newKeys, ak)
			}
		}
		appSettings.ApiKeys = newKeys
		saveSettings(appSettings)
		w.Write([]byte(`{"success":true}`))
		return
	}
}

func (env *Environment) handleAdminCrons(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "schedules") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		rows, _ := env.ConfigDBConn.Query("SELECT id, name, schedule, schedule_meta, script_path, active, created FROM _cron_jobs ORDER BY id DESC")
		defer rows.Close()
		nextRuns := make(map[int]string)
		if env.Scheduler != nil {
			 for _, job := range env.Scheduler.Jobs() {
				  tags := job.Tags()
				  if len(tags) > 0 {
				      if jID, err := strconv.Atoi(tags[0]); err == nil {
				          if nr, err := job.NextRun(); err == nil {
				              nextRuns[jID] = nr.Format(time.RFC3339)
				          }
				      }
				  }
			 }
		}
		results := []map[string]any{}
		for rows.Next() {
			var id, active, preventOverlap int
			var scheduleMeta sql.NullString
			var name, schedule, scriptPath string
			var createdRaw any
			rows.Scan(&id, &name, &schedule, &scheduleMeta, &scriptPath, &active, &preventOverlap, &createdRaw)

			var created any = createdRaw
			if b, ok := createdRaw.([]byte); ok {
				created = string(b)
			}

			results = append(results, map[string]any{
				"id":            id,
				"name":          name,
				"schedule":      schedule,
				"schedule_meta": scheduleMeta.String,
				"script_path":   scriptPath,
				"active":        active == 1,
				"created":       created,
				"next_run":      nextRuns[id],
			})
		}
		json.NewEncoder(w).Encode(results)
		return
	}
	if r.Method == "POST" {
		var req struct {
			ID             int    `json:"id"`
			Name           string `json:"name"`
			Schedule       string `json:"schedule"`
			ScheduleMeta   string `json:"schedule_meta"`
			ScriptPath     string `json:"script_path"`
			Active         bool   `json:"active"`
			PreventOverlap bool   `json:"prevent_overlap"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		
		activeInt := 0
		if req.Active {
			activeInt = 1
		}
		preventOverlapInt := 0
		if req.PreventOverlap {
			preventOverlapInt = 1
		}

		if req.ID > 0 {
			env.ConfigDBConn.Exec("UPDATE _cron_jobs SET name=?, schedule=?, schedule_meta=?, script_path=?, active=?, prevent_overlap=? WHERE id=?",
				req.Name, req.Schedule, req.ScheduleMeta, req.ScriptPath, activeInt, preventOverlapInt, req.ID)
		} else {
			env.ConfigDBConn.Exec("INSERT INTO _cron_jobs (name, schedule, schedule_meta, script_path, active, prevent_overlap, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
				req.Name, req.Schedule, req.ScheduleMeta, req.ScriptPath, activeInt, preventOverlapInt, time.Now().Unix())
		}
		env.initCron()
		w.Write([]byte(`{"success":true}`))
		return
	}
	if r.Method == "DELETE" {
		id := r.URL.Query().Get("id")
		env.ConfigDBConn.Exec("DELETE FROM _cron_jobs WHERE id = ?", id)
		env.initCron()
		w.Write([]byte(`{"success":true}`))
		return
	}
}

func (env *Environment) handleAdminSchedulesRun(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "schedules") {
		http.Error(w, "Forbidden", 403)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", 405)
		return
	}

	var req struct {
		ScriptPath string `json:"script_path"`
		Repeat     int    `json:"repeat"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// Traversal protection
	if strings.Contains(req.ScriptPath, "..") {
		http.Error(w, "Invalid script path", 400)
		return
	}

	if req.Repeat <= 0 {
		req.Repeat = 1
	}

	fullPath := filepath.Join(env.ScriptsPath, req.ScriptPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		http.Error(w, "Script not found", 404)
		return
	}

	// Execute sequence and fail gracefully on Lua errors
	for i := 0; i < req.Repeat; i++ {
		if err := env.runCronScript(fullPath); err != nil {
			http.Error(w, fmt.Sprintf("Execution error on iteration %d: %v", i+1, err), 500)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"success":true}`))
}

func (env *Environment) handleAdminData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api/collections")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if path == "" || path == "/" {
		if r.Method == "GET" {
			rows, _ := env.ConfigDBConn.Query("SELECT * FROM _collections")
			cols, _ := rows.Columns()
			results := []map[string]any{}
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				rows.Scan(ptrs...)
				row := make(map[string]any)
				for i, col := range cols {
					row[col] = vals[i]
				}
				
				cName := row["name"].(string)
				if hasCollectionPermission(r, cName, "item") || hasCollectionPermission(r, cName, "schema") {
					row["schema"] = env.getCollectionSchema(cName)
					results = append(results, row)
				}
			}
			json.NewEncoder(w).Encode(results)
			return
		}
		if r.Method == "POST" {
			var req struct {
				Name   string        `json:"name"`
				Schema []SchemaField `json:"schema"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if !hasCollectionPermission(r, req.Name, "schema") {
				http.Error(w, "Forbidden", 403)
				return
			}
			if err := env.createCollectionInDB(req.Name, req.Schema); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.Write([]byte(`{"success":true}`))
			return
		}
	}

	collection := parts[0]
	if collection == "_api_keys" {
		http.Error(w, "Forbidden", 403)
		return
	}

	if len(parts) == 1 {
		if r.Method == "GET" {
			if !hasCollectionPermission(r, collection, "item") {
				http.Error(w, "Forbidden", 403)
				return
			}
			search := strings.TrimSpace(r.URL.Query().Get("search"))
			whereClause := "1=1"
			var args []interface{}

			if search != "" {
				if strings.Contains(search, ":") {
					searchParts := strings.SplitN(search, ":", 2)
					field, val := searchParts[0], searchParts[1]
					whereClause += fmt.Sprintf(" AND %s LIKE ?", field)

					schema := env.getCollectionSchema(collection)
					var fieldType string
					for _, sf := range schema {
						if sf.Name == field {
							fieldType = sf.Type
							break
						}
					}
					if fieldType == "bool" {
						valLower := strings.ToLower(val)
						if valLower == "true" {
							args = append(args, "true")
						} else if valLower == "false" {
							args = append(args, "false")
						} else {
							args = append(args, "%"+val+"%")
						}
					} else {
						args = append(args, "%"+val+"%")
					}
				} else {
					schema := env.getCollectionSchema(collection)
					if len(schema) > 0 {
						var orClauses []string
						for _, sf := range schema {
							orClauses = append(orClauses, fmt.Sprintf("%s LIKE ?", sf.Name))
							args = append(args, "%"+search+"%")
						}
						whereClause += " AND (" + strings.Join(orClauses, " OR ") + ")"
					}
				}
			}

			var total int
			env.DataDBConn.QueryRow("SELECT COUNT(*) FROM "+collection+" WHERE "+whereClause, args...).Scan(&total)

			query := "SELECT * FROM " + collection + " WHERE " + whereClause + " ORDER BY id DESC"
			items, err := queryDB(env.DataDBConn, query, args...)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			
			colSet := env.getCollectionSet()
			cache := make(map[string]map[string]any)
			var expandedItems []map[string]any
			
			for _, item := range items {
				activePath := make(map[string]bool)
				expanded := env.expandRecord(collection, item, cache, activePath, colSet, false)
				expandedItems = append(expandedItems, expanded)
			}
			
			if expandedItems == nil {
				expandedItems = []map[string]any{} // ensure empty array instead of null
			}
			
			json.NewEncoder(w).Encode(map[string]interface{}{"items": expandedItems, "total": total})
			return
		}
		if r.Method == "POST" {
			if !hasCollectionPermission(r, collection, "item") {
				http.Error(w, "Forbidden", 403)
				return
			}
			var data map[string]any
			json.NewDecoder(r.Body).Decode(&data)
			cols, placeholders, args := []string{}, []string{}, []any{}
			if _, ok := data["created"]; !ok {
				data["created"] = time.Now().Unix()
			}
			if _, ok := data["updated"]; !ok {
				data["updated"] = time.Now().Unix()
			}
			for k, v := range data {
				cols = append(cols, k)
				placeholders = append(placeholders, "?")
				switch val := v.(type) {
				case map[string]any, []any:
					args = append(args, formatLuaTable(val))
				default:
					args = append(args, v)
				}
			}
			q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", collection, strings.Join(cols, ","), strings.Join(placeholders, ","))
			if _, err := env.DataDBConn.Exec(q, args...); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.Write([]byte(`{"success":true}`))
			return
		}
		if r.Method == "PUT" {
			if !hasCollectionPermission(r, collection, "schema") {
				http.Error(w, "Forbidden", 403)
				return
			}
			var req struct {
				Schema []SchemaField `json:"schema"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if err := env.updateCollectionInDB(collection, req.Schema); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.Write([]byte(`{"success":true}`))
			return
		}
		if r.Method == "DELETE" {
			if strings.HasPrefix(collection, "_") {
				http.Error(w, "Forbidden", 403)
				return
			}
			ids := r.URL.Query().Get("ids")
			if ids != "" {
				if !hasCollectionPermission(r, collection, "item") {
					http.Error(w, "Forbidden", 403)
					return
				}
				idList := strings.Split(ids, ",")
				for _, idStr := range idList {
					env.DataDBConn.Exec("DELETE FROM "+collection+" WHERE id = ?", idStr)
				}
				w.Write([]byte(`{"success":true}`))
				return
			}
			if !hasCollectionPermission(r, collection, "schema") {
				http.Error(w, "Forbidden", 403)
				return
			}
			env.DataDBConn.Exec("DROP TABLE " + collection)
			env.ConfigDBConn.Exec("DELETE FROM _schema WHERE collection_id = (SELECT id FROM _collections WHERE name = ?)", collection)
			env.ConfigDBConn.Exec("DELETE FROM _collections WHERE name = ?", collection)
			env.SchemaCache.Delete(collection)
			w.Write([]byte(`{"success":true}`))
			return
		}
	}

	if len(parts) == 2 {
		id := parts[1]
		if r.Method == "PUT" {
			if !hasCollectionPermission(r, collection, "item") {
				http.Error(w, "Forbidden", 403)
				return
			}
			var data map[string]any
			json.NewDecoder(r.Body).Decode(&data)
			data["updated"] = time.Now().Unix()

			cols, args := []string{}, []any{}
			for k, v := range data {
				if k == "id" || k == "created" {
					continue
				}
				cols = append(cols, k+" = ?")
				switch val := v.(type) {
				case map[string]any, []any:
					args = append(args, formatLuaTable(val))
				default:
					args = append(args, v)
				}
			}
			args = append(args, id)
			q := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", collection, strings.Join(cols, ","))
			if _, err := env.DataDBConn.Exec(q, args...); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.Write([]byte(`{"success":true}`))
			return
		}
		if r.Method == "DELETE" {
			if !hasCollectionPermission(r, collection, "item") {
				http.Error(w, "Forbidden", 403)
				return
			}
			env.DataDBConn.Exec("DELETE FROM "+collection+" WHERE id = ?", id)
			w.Write([]byte(`{"success":true}`))
			return
		}
	}
}

func (env *Environment) handleAdminFiles(w http.ResponseWriter, r *http.Request) {
	base := r.URL.Query().Get("base")
	baseDir := env.PublicPath
	permKey := "public"

	if base == "routes" {
		baseDir = env.RoutesPath
		permKey = "routes"
	} else if base == "schedules" {
		baseDir = env.ScriptsPath
		permKey = "schedules"
	} else if base == "uploads" {
		baseDir = env.UploadsPath
		permKey = "uploads"
	}

	if permKey == "uploads" {
		p := getPermissions(r)
		// Require at least some collection permissions to upload
		if p == nil || p["collections"] == nil {
			http.Error(w, "Forbidden", 403)
			return
		}
	} else if !hasPermission(r, permKey) {
		http.Error(w, "Forbidden", 403)
		return
	}

	if r.Method == "GET" {
		target := r.URL.Query().Get("path")
		raw := r.URL.Query().Get("raw") == "true"

		if target != "" {
			if strings.Contains(target, "..") {
				http.Error(w, "Invalid path", 403)
				return
			}
			content, err := os.ReadFile(filepath.Join(baseDir, target))
			if err != nil {
				http.Error(w, "Not found", 404)
				return
			}
			if raw {
				w.Write(content)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"content": string(content)})
			return
		}

		type FileInfo struct {
			Path  string `json:"path"`
			IsDir bool   `json:"is_dir"`
		}
		var files []FileInfo = []FileInfo{}
		filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				rel, _ := filepath.Rel(baseDir, path)
				if rel != "." {
					files = append(files, FileInfo{Path: filepath.ToSlash(rel), IsDir: info.IsDir()})
				}
			}
			return nil
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(files)

	} else if r.Method == "POST" {
		contentType := r.Header.Get("Content-Type")

		// Handle Form / Drag & Drop Uploads
		if strings.HasPrefix(contentType, "multipart/form-data") {
			if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB Memory bounds
				http.Error(w, err.Error(), 400)
				return
			}

			path := r.FormValue("path")
			if strings.Contains(path, "..") {
				http.Error(w, "Invalid path", 403)
				return
			}
			targetDir := filepath.Join(baseDir, path)
			os.MkdirAll(targetDir, os.ModePerm)

			for _, fheaders := range r.MultipartForm.File {
				for _, hdr := range fheaders {
					safeFileName := filepath.Base(hdr.Filename)
					if safeFileName == "." || safeFileName == "/" || safeFileName == "\\" {
						continue
					}

					srcFile, err := hdr.Open()
					if err != nil {
						continue
					}

					dstPath := filepath.Join(targetDir, safeFileName)
					dstFile, err := os.Create(dstPath)
					if err == nil {
						io.Copy(dstFile, srcFile)
						dstFile.Close()
					}
					srcFile.Close()
				}
			}

			if base == "routes" {
				env.initRoutes()
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"success":true}`))
			return
		}

		// Handle standard JSON File/Folder Creation
		var req struct {
			Path    string `json:"path"`
			Content string `json:"content"`
			IsDir   bool   `json:"is_dir"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if strings.Contains(req.Path, "..") {
			http.Error(w, "Invalid", 403)
			return
		}
		fullPath := filepath.Join(baseDir, req.Path)

		if req.IsDir {
			os.MkdirAll(fullPath, os.ModePerm)
		} else {
			os.MkdirAll(filepath.Dir(fullPath), os.ModePerm)
			os.WriteFile(fullPath, []byte(req.Content), 0644)
		}
		if base == "routes" {
			env.initRoutes()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	} else if r.Method == "DELETE" {
		path := r.URL.Query().Get("path")
		if strings.Contains(path, "..") {
			http.Error(w, "Invalid", 403)
			return
		}
		os.RemoveAll(filepath.Join(baseDir, path))
		if base == "routes" {
			env.initRoutes()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}
}

// -----------------------------------------------------------------------------
// 4. LUA EXTENSIONS & ENGINE
// -----------------------------------------------------------------------------
func (env *Environment) injectDB(L *lua.LState) {
	dbTbl := L.NewTable()
	dbMeta := L.NewTable()
	L.SetField(dbMeta, "__index", L.NewFunction(func(L *lua.LState) int {
		colName := L.CheckString(2)
		colTbl := L.NewTable()
		colTbl.RawSetString("_name", lua.LString(colName))

		// db.<collection>:get(query, limit, sortBy)
		colTbl.RawSetString("get", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()

			whereVals := []any{}
			whereCols := []string{"1=1"}

			if L.GetTop() >= 2 && L.Get(2).Type() == lua.LTTable {
				queryTbl := L.CheckTable(2)
				if queryMap, ok := luaValueToInterface(queryTbl).(map[string]any); ok {
					for k, v := range queryMap {
						whereCols = append(whereCols, fmt.Sprintf("%s = ?", k))
						whereVals = append(whereVals, v)
					}
				}
			}

			limit := 0
			reverseSort := false
			if L.GetTop() >= 3 && L.Get(3).Type() == lua.LTNumber {
				limitFloat := float64(L.CheckNumber(3))
				reverseSort = math.Signbit(limitFloat)
				limit = int(math.Abs(limitFloat))
			}

			sortBy := ""
			if L.GetTop() >= 4 && L.Get(4).Type() == lua.LTString {
				sortBy = L.CheckString(4)
			}

			orderClause := "ORDER BY id ASC"
			if sortBy != "" {
				sortCol := sortBy
				if !strings.Contains(strings.ToUpper(sortCol), " ASC") && !strings.Contains(strings.ToUpper(sortCol), " DESC") {
					sortCol += " ASC"
				}
				orderClause = "ORDER BY " + sortCol
			}

			if reverseSort {
				parts := strings.Split(orderClause[9:], ",") // remove "ORDER BY "
				for i, part := range parts {
					part = strings.TrimSpace(part)
					upperPart := strings.ToUpper(part)
					if strings.HasSuffix(upperPart, " ASC") {
						parts[i] = part[:len(part)-4] + " DESC"
					} else if strings.HasSuffix(upperPart, " DESC") {
						parts[i] = part[:len(part)-5] + " ASC"
					}
				}
				orderClause = "ORDER BY " + strings.Join(parts, ", ")
			}

			limitClause := ""
			if limit > 0 {
				limitClause = fmt.Sprintf("LIMIT %d", limit)
			}

			colSet := env.getCollectionSet()
			results, err := queryDB(env.DataDBConn, q, whereVals...)
			if err != nil {
				L.Push(L.NewTable())
				return 1
			}

			cache := make(map[string]map[string]any)
			visitedLua := make(map[string]lua.LValue)
			arr := L.NewTable()

			for i, r := range results {
				activePath := make(map[string]bool)
				expandedMap := env.expandRecord(cName, r, cache, activePath, colSet, true)
				arr.RawSetInt(i+1, toLValueCyclic(L, expandedMap, visitedLua))
			}

			L.Push(arr)
			return 1
		}))

		// db.<collection>:insert(data)
		colTbl.RawSetString("insert", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			data, ok := luaValueToInterface(L.CheckTable(2)).(map[string]any)
			if !ok {
				L.Push(lua.LNil)
				L.Push(lua.LString("invalid data format"))
				return 2
			}

			now := time.Now().Unix()
			if _, exists := data["created"]; !exists {
				data["created"] = now
			}
			if _, exists := data["updated"]; !exists {
				data["updated"] = now
			}

			cols, placeholders, args := []string{}, []string{}, []any{}
			for k, v := range data {
				cols = append(cols, k)
				placeholders = append(placeholders, "?")
				switch val := v.(type) {
				case map[string]any, []any:
					args = append(args, formatLuaTable(val))
				default:
					args = append(args, v)
				}
			}
			q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", cName, strings.Join(cols, ","), strings.Join(placeholders, ","))
			res, err := env.DataDBConn.Exec(q, args...)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			id, _ := res.LastInsertId()
			L.Push(lua.LNumber(id))
			return 1
		}))

		// db.<collection>:update(query, updateMap)
		colTbl.RawSetString("update", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			queryMap, _ := luaValueToInterface(L.OptTable(2, L.NewTable())).(map[string]any)
			updateMap, _ := luaValueToInterface(L.OptTable(3, L.NewTable())).(map[string]any)

			updateMap["updated"] = time.Now().Unix()

			whereVals, whereCols := []any{}, []string{"1=1"}
			for k, v := range queryMap {
				whereCols = append(whereCols, fmt.Sprintf("%s = ?", k))
				whereVals = append(whereVals, v)
			}

			setCols, setVals := []string{}, []any{}
			for k, v := range updateMap {
				if k == "id" || k == "created" {
					continue
				}
				setCols = append(setCols, fmt.Sprintf("%s = ?", k))
				switch val := v.(type) {
				case map[string]any, []any:
					setVals = append(setVals, formatLuaTable(val))
				default:
					setVals = append(setVals, v)
				}
			}

			q := fmt.Sprintf("UPDATE %s SET %s WHERE %s", cName, strings.Join(setCols, ", "), strings.Join(whereCols, " AND "))
			args := append(setVals, whereVals...)
			_, err := env.DataDBConn.Exec(q, args...)
			if err != nil {
				L.Push(lua.LBool(false))
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(lua.LBool(true))
			return 1
		}))

		// db.<collection>:delete(query)
		colTbl.RawSetString("delete", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			queryMap, _ := luaValueToInterface(L.OptTable(2, L.NewTable())).(map[string]any)

			whereVals, whereCols := []any{}, []string{"1=1"}
			for k, v := range queryMap {
				whereCols = append(whereCols, fmt.Sprintf("%s = ?", k))
				whereVals = append(whereVals, v)
			}

			q := fmt.Sprintf("DELETE FROM %s WHERE %s", cName, strings.Join(whereCols, " AND "))
			_, err := env.DataDBConn.Exec(q, whereVals...)
			if err != nil {
				L.Push(lua.LBool(false))
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(lua.LBool(true))
			return 1
		}))

		colMeta := L.NewTable()
		L.SetField(colMeta, "__call", L.NewFunction(func(L *lua.LState) int {
			arg1 := L.Get(2)
			var query string
			var startArg int

			if arg1.Type() == lua.LTTable {
				query = L.CheckString(3)
				startArg = 4
			} else if arg1.Type() == lua.LTString {
				query = arg1.String()
				startArg = 3
			} else {
				L.Push(lua.LNil)
				L.Push(lua.LString("invalid arguments to exec"))
				return 2
			}

			var args []interface{}
			for i := startArg; i <= L.GetTop(); i++ {
				args = append(args, luaValueToInterface(L.Get(i)))
			}

			queryUpper := strings.ToUpper(strings.TrimSpace(query))
			if strings.HasPrefix(queryUpper, "SELECT") || strings.HasPrefix(queryUpper, "PRAGMA") {
				results, err := queryDB(env.DataDBConn, query, args...)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				arr := L.NewTable()
				for i, r := range results {
					arr.RawSetInt(i+1, mapToLTable(L, r))
				}
				L.Push(arr)
				return 1
			} else {
				res, err := env.DataDBConn.Exec(query, args...)
				if err != nil {
					L.Push(lua.LBool(false))
					L.Push(lua.LString(err.Error()))
					return 2
				}
				affected, _ := res.RowsAffected()
				L.Push(lua.LBool(true))
				L.Push(lua.LNumber(affected))
				return 2
			}
		}))
		L.SetMetatable(colTbl, colMeta)

		L.Push(colTbl)
		return 1
	}))
	L.SetMetatable(dbTbl, dbMeta)
	L.SetGlobal("db", dbTbl)
}

func getScript(L *lua.LState) string {
	if dbg, ok := L.GetStack(1); ok {
		L.GetInfo("S", dbg, lua.LNil)
		return filepath.Base(dbg.Source)
	}
	return "unknown"
}

func formatLuaTable(val any) string {
	b, err := json.Marshal(val)
	if err == nil {
		return string(b)
	}
	return fmt.Sprintf("%v", val)
}

func luaValueToInterface(v lua.LValue) any {
	switch v.Type() {
	case lua.LTNumber:
		return float64(v.(lua.LNumber))
	case lua.LTString:
		return string(v.(lua.LString))
	case lua.LTBool:
		return bool(v.(lua.LBool))
	case lua.LTTable:
		tbl := v.(*lua.LTable)
		maxn := tbl.MaxN()
		if maxn > 0 {
			arr := make([]any, maxn)
			for i := 1; i <= maxn; i++ {
				arr[i-1] = luaValueToInterface(tbl.RawGetInt(i))
			}
			return arr
		}
		m := make(map[string]any)
		tbl.ForEach(func(k, v lua.LValue) {
			m[k.String()] = luaValueToInterface(v)
		})
		return m
	default:
		return nil
	}
}

// -----------------------------------------------------------------------------
// 6. ROUTING & HANDLER
// -----------------------------------------------------------------------------
type Route struct {
	Pattern  *regexp.Regexp
	Params   []string
	FilePath string
}

func (env *Environment) initRoutes() {
	paramRegex := regexp.MustCompile(`\[([^/]+)\]`)
	var newRoutes []Route
	filepath.Walk(env.RoutesPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".lua" {
			return nil
		}
		relPath, _ := filepath.Rel(env.RoutesPath, path)
		urlPath := "/" + strings.TrimSuffix(filepath.ToSlash(relPath), ".lua")
		if strings.HasSuffix(urlPath, "/index") {
			urlPath = strings.TrimSuffix(urlPath, "index")
		}
		if urlPath != "/" && strings.HasSuffix(urlPath, "/") {
			urlPath = strings.TrimSuffix(urlPath, "/")
		}
		var params []string
		pattern := paramRegex.ReplaceAllStringFunc(urlPath, func(m string) string {
			p := m[1 : len(m)-1]
			params = append(params, p)
			return fmt.Sprintf(`(?P<%s>[^/]+)`, p)
		})
		newRoutes = append(newRoutes, Route{Pattern: regexp.MustCompile("^" + pattern + "$"), Params: params, FilePath: path})
		return nil
	})
	sort.Slice(newRoutes, func(i, j int) bool {
		if len(newRoutes[i].Params) != len(newRoutes[j].Params) {
			return len(newRoutes[i].Params) < len(newRoutes[j].Params)
		}
		return len(newRoutes[i].FilePath) > len(newRoutes[j].FilePath)
	})

	env.RoutesMu.Lock()
	env.Routes = newRoutes
	env.RoutesMu.Unlock()
}

func (env *Environment) watchRoutes() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("Failed to create fsnotify watcher: %v", err)
		return
	}
	defer watcher.Close()

	// Walk through the routes directory and add all subdirectories to the watcher
	err = filepath.Walk(env.RoutesPath, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.IsDir() {
			watcher.Add(path)
		}
		return nil
	})
	if err != nil {
		log.Printf("Failed to walk routes directory for watcher: %v", err)
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			
			// If a new directory is created, add it to the watcher dynamically
			if event.Op&fsnotify.Create == fsnotify.Create {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					watcher.Add(event.Name)
				}
			}

			// Reload routes on any relevant modification
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				env.initRoutes()
			}
			
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("fsnotify watcher error: %v", err)
		}
	}
}

func (env *Environment) matchRoute(urlPath string) (*Route, map[string]string) {
	env.RoutesMu.RLock()
	defer env.RoutesMu.RUnlock()

	for _, r := range env.Routes {
		if m := r.Pattern.FindStringSubmatch(urlPath); m != nil {
			params := make(map[string]string)
			for i, name := range r.Pattern.SubexpNames() {
				if i != 0 && name != "" {
					params[name] = m[i]
				}
			}
			res := r
			return &res, params
		}
	}
	return nil, nil
}

func (env *Environment) createLuaState() *lua.LState {
	L := lua.NewState(lua.Options{
		SkipOpenLibs: true,
	})

	safeLibs := map[string]lua.LGFunction{
		lua.LoadLibName:      lua.OpenPackage,
		lua.BaseLibName:      lua.OpenBase,
		lua.TabLibName:       lua.OpenTable,
		lua.StringLibName:    lua.OpenString,
		lua.MathLibName:      lua.OpenMath,
		lua.CoroutineLibName: lua.OpenCoroutine,
		lua.OsLibName:        lua.OpenOs,
	}
	for name, lib := range safeLibs {
		L.Push(L.NewFunction(lib))
		L.Push(lua.LString(name))
		L.Call(1, 0)
	}

	pkg := L.GetGlobal("package")
	if pkgTbl, ok := pkg.(*lua.LTable); ok {
		pathStr := pkgTbl.RawGetString("path").String()
		paths := strings.Split(pathStr, ";")
		var newPaths []string

		rootSlash := strings.TrimSuffix(filepath.ToSlash(env.BaseDir), "/")
		if rootSlash == "" {
			newPaths = append(newPaths, "/?.lua", "/?/init.lua")
		} else {
			newPaths = append(newPaths, rootSlash+"/?.lua", rootSlash+"/?/init.lua")
		}

		for _, p := range paths {
			if strings.HasPrefix(p, "./") || strings.HasPrefix(p, ".\\") {
				continue
			}
			newPaths = append(newPaths, p)
		}
		pkgTbl.RawSetString("path", lua.LString(strings.Join(newPaths, ";")))
	}

	if !appSettings.UnsafeLua {
		osTbl := L.GetGlobal("os").(*lua.LTable)
		dangerous := []string{"execute", "exit", "remove", "rename", "setenv", "getenv", "tmpname"}
		for _, op := range dangerous {
			L.SetField(osTbl, op, lua.LNil)
		}
	} else {
		L.Push(L.NewFunction(lua.OpenIo))
		L.Push(lua.LString(lua.IoLibName))
		L.Call(1, 0)

		L.Push(L.NewFunction(lua.OpenDebug))
		L.Push(lua.LString(lua.DebugLibName))
		L.Call(1, 0)
	}

	env.injectDB(L)
	env.injectMgoAPI(L)

	return L
}

func (env *Environment) initLuaPool() {
	env.LuaPool = &sync.Pool{
		New: func() any {
			return env.createLuaState()
		},
	}
}

func (env *Environment) injectMgoAPI(L *lua.LState) {
	logMod := L.NewTable()
	L.SetField(logMod, "info", L.NewFunction(func(L *lua.LState) int {
		env.appLog("info", getScript(L), "lua", L.CheckString(1))
		return 0
	}))
	L.SetField(logMod, "warn", L.NewFunction(func(L *lua.LState) int {
		env.appLog("warn", getScript(L), "lua", L.CheckString(1))
		return 0
	}))
	L.SetField(logMod, "error", L.NewFunction(func(L *lua.LState) int {
		env.appLog("error", getScript(L), "lua", L.CheckString(1))
		return 0
	}))
	L.SetField(logMod, "get", L.NewFunction(func(L *lua.LState) int {
		limit := 1
		argIdx := 1
		if L.GetTop() >= 1 && L.Get(1).Type() == lua.LTTable {
			argIdx = 2
		}
		if L.GetTop() >= argIdx {
			limit = L.CheckInt(argIdx)
		}
		if limit <= 0 {
			limit = 1
		}

		arr := L.NewTable()
		if env.LogDBConn != nil {
			rows, err := env.LogDBConn.Query("SELECT id, timestamp, level, origin, script_path, message FROM _logs ORDER BY id DESC LIMIT ?", limit)
			if err == nil {
				defer rows.Close()
				i := 1
				for rows.Next() {
					var id int
					var timestamp, level, origin, script, message string
					if err := rows.Scan(&id, &timestamp, &level, &origin, &script, &message); err == nil {
						row := L.NewTable()
						row.RawSetString("id", lua.LNumber(id))
						row.RawSetString("timestamp", lua.LString(timestamp))
						row.RawSetString("level", lua.LString(level))
						row.RawSetString("origin", lua.LString(origin))
						row.RawSetString("script", lua.LString(script))
						row.RawSetString("message", lua.LString(message))
						arr.RawSetInt(i, row)
						i++
					}
				}
			}
		}
		L.Push(arr)
		return 1
	}))
	L.SetGlobal("log", logMod)

	L.SetGlobal("http", L.NewFunction(luaHttpRequest))
}

func luaHttpRequestReal(L *lua.LState, method string) int {
	url := L.CheckString(1)
	opts := L.OptTable(2, L.NewTable())

	var reqBody io.Reader
	if bodyVal := opts.RawGetString("body"); bodyVal.Type() != lua.LTNil {
		reqBody = strings.NewReader(bodyVal.String())
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	if headersVal := opts.RawGetString("headers"); headersVal.Type() == lua.LTTable {
		headersVal.(*lua.LTable).ForEach(func(k, v lua.LValue) { req.Header[k.String()] = []string{v.String()} })
	}
	if reqBody != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	resTable := L.NewTable()
	resTable.RawSetString("status", lua.LNumber(resp.StatusCode))
	resTable.RawSetString("body", lua.LString(string(bodyBytes)))

	headersTable := L.NewTable()
	for k, v := range resp.Header {
		if len(v) > 0 {
			headersTable.RawSetString(k, lua.LString(v[0]))
		}
	}
	resTable.RawSetString("headers", headersTable)

	L.Push(resTable)
	return 1
}

func luaHttpRequest(L *lua.LState) int {
	method := L.CheckString(1)
	L.Remove(1)
	return luaHttpRequestReal(L, strings.ToUpper(method))
}

func mapToLTable(L *lua.LState, m map[string]interface{}) *lua.LTable {
	tbl := L.NewTable()
	for k, v := range m {
		tbl.RawSetString(k, toLValue(L, v))
	}
	return tbl
}

func sliceToLTable(L *lua.LState, s []interface{}) *lua.LTable {
	tbl := L.NewTable()
	for i, v := range s {
		tbl.RawSetInt(i+1, toLValue(L, v))
	}
	return tbl
}

func toLValue(L *lua.LState, val interface{}) lua.LValue {
	switch v := val.(type) {
	case string:
		return lua.LString(v)
	case float64:
		return lua.LNumber(v)
	case int:
		return lua.LNumber(v)
	case bool:
		return lua.LBool(v)
	case nil:
		return lua.LNil
	case map[string]interface{}:
		return mapToLTable(L, v)
	case []interface{}:
		return sliceToLTable(L, v)
	default:
		return lua.LString(fmt.Sprintf("%v", val))
	}
}

func (env *Environment) mogoHandler(w http.ResponseWriter, r *http.Request) {
	route, params := env.matchRoute(r.URL.Path)
	if route == nil {
		http.Error(w, "404 Not Found", 404)
		return
	}

	L := env.LuaPool.Get().(*lua.LState)
	top := L.GetTop()

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	L.SetContext(ctx)

	handoff := false
	defer func() {
		cancel()
		if !handoff {
			L.RemoveContext()
			L.SetTop(top)
			env.LuaPool.Put(L)
		}
	}()

	fn, err := L.LoadFile(route.FilePath)
	if err != nil {
		env.appLog("error", route.FilePath, "router", err.Error())
		http.Error(w, "500 Internal Server Error", 500)
		return
	}

	luaEnv := L.NewTable()
	mt := L.NewTable()
	L.SetField(mt, "__index", L.Get(lua.GlobalsIndex))
	L.SetMetatable(luaEnv, mt)
	fn.Env = luaEnv

	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		env.appLog("error", route.FilePath, "execution", err.Error())
		http.Error(w, "500 Internal Server Error: execution failed", 500)
		return
	}

	ret := L.Get(-1)
	if ret.Type() != lua.LTTable {
		http.Error(w, "500 Internal Server Error: script must return a table", 500)
		return
	}
	routeTable := ret.(*lua.LTable)

	methodFunc := routeTable.RawGetString(strings.ToUpper(r.Method))
	if methodFunc.Type() == lua.LTNil {
		methodFunc = routeTable.RawGetString("ANY")
	}

	if methodFunc.Type() != lua.LTFunction {
		http.Error(w, "405 Method Not Allowed", 405)
		return
	}

	reqTable := L.NewTable()
	reqTable.RawSetString("method", lua.LString(r.Method))
	reqTable.RawSetString("path", lua.LString(r.URL.Path))
	reqTable.RawSetString("ip", lua.LString(getIP(r)))

	queryTable := L.NewTable()
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			queryTable.RawSetString(k, lua.LString(v[0]))
		}
	}
	reqTable.RawSetString("query", queryTable)

	paramTable := L.NewTable()
	for k, v := range params {
		paramTable.RawSetString(k, lua.LString(v))
	}
	reqTable.RawSetString("params", paramTable)

	headerTable := L.NewTable()
	for k, v := range r.Header {
		if len(v) > 0 {
			headerTable.RawSetString(k, lua.LString(v[0]))
		}
	}
	reqTable.RawSetString("headers", headerTable)

	cookiesTable := L.NewTable()
	for _, c := range r.Cookies() {
		cookiesTable.RawSetString(c.Name, lua.LString(c.Value))
	}
	reqTable.RawSetString("cookies", cookiesTable)

	if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		var body interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			reqTable.RawSetString("body", toLValue(L, body))
		}
	} else if strings.Contains(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") || strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
		if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
			r.ParseMultipartForm(32 << 20)
		} else {
			r.ParseForm()
		}
		bodyTable := L.NewTable()
		for k, v := range r.Form {
			if len(v) > 0 {
				bodyTable.RawSetString(k, lua.LString(v[0]))
			}
		}
		reqTable.RawSetString("body", bodyTable)
	}

	filesTable := L.NewTable()
	if r.MultipartForm != nil {
		for key, headers := range r.MultipartForm.File {
			if len(headers) > 0 {
				fileHeader := headers[0]
				fileTable := L.NewTable()
				fileTable.RawSetString("filename", lua.LString(fileHeader.Filename))
				fileTable.RawSetString("size", lua.LNumber(fileHeader.Size))

				saveFunc := L.NewFunction(func(L *lua.LState) int {
					destPath := L.CheckString(2)

					targetPath := destPath
					if !filepath.IsAbs(targetPath) {
						targetPath = filepath.Join(env.BaseDir, targetPath)
					}
					absTarget, err := filepath.Abs(targetPath)
					if err != nil {
						L.Push(lua.LBool(false))
						L.Push(lua.LString(err.Error()))
						return 2
					}

					if !appSettings.UnsafeLua {
						absUploads, _ := filepath.Abs(env.UploadsPath)
						if absTarget != absUploads && !strings.HasPrefix(absTarget, absUploads+string(filepath.Separator)) {
							env.appLog("warn", getScript(L), "security", "Blocked attempt to write file outside uploads directory: "+destPath)
							L.Push(lua.LBool(false))
							L.Push(lua.LString("access denied: path escapes secure directory"))
							return 2
						}
					}

					os.MkdirAll(filepath.Dir(absTarget), os.ModePerm)

					srcFile, err := fileHeader.Open()
					if err != nil {
						L.Push(lua.LBool(false))
						L.Push(lua.LString(err.Error()))
						return 2
					}
					defer srcFile.Close()

					destFile, err := os.Create(absTarget)
					if err != nil {
						L.Push(lua.LBool(false))
						L.Push(lua.LString(err.Error()))
						return 2
					}
					defer destFile.Close()

					_, err = io.Copy(destFile, srcFile)
					if err != nil {
						L.Push(lua.LBool(false))
						L.Push(lua.LString(err.Error()))
						return 2
					}

					L.Push(lua.LBool(true))
					return 1
				})
				fileTable.RawSetString("save", saveFunc)
				filesTable.RawSetString(key, fileTable)
			}
		}
	}
	reqTable.RawSetString("files", filesTable)

	resTable := L.NewTable()
	resTable.RawSetString("status", lua.LNumber(200))
	resTable.RawSetString("headers", L.NewTable())
	resTable.RawSetString("cookies", L.NewTable())

	fileFunc := L.NewFunction(func(L *lua.LState) int {
		pathStr := L.CheckString(2)
		filename := L.OptString(3, filepath.Base(pathStr))

		if !appSettings.UnsafeLua {
			absBase, _ := filepath.Abs(env.PublicPath)
			absTarget, _ := filepath.Abs(filepath.Join(env.PublicPath, pathStr))
			if !strings.HasPrefix(absTarget, absBase) {
				L.RaiseError("access denied: path escapes secure directory")
				return 0
			}
			pathStr = absTarget
		}

		resTbl := L.CheckTable(1)
		resTbl.RawSetString("_file_path", lua.LString(pathStr))
		resTbl.RawSetString("_file_name", lua.LString(filename))
		return 0
	})
	resTable.RawSetString("file", fileFunc)

	callTop := L.GetTop()
	err = L.CallByParam(lua.P{
		Fn:      methodFunc,
		NRet:    lua.MultRet,
		Protect: true,
	}, reqTable, resTable)

	if err != nil {
		env.appLog("error", route.FilePath, "execution", err.Error())
		http.Error(w, "500 Internal Server Error: execution failed", 500)
		return
	}

	resHeaders := resTable.RawGetString("headers").(*lua.LTable)
	nRet := L.GetTop() - callTop

	var postHook *lua.LFunction
	var bodyVal lua.LValue

	if nRet > 0 {
		last := L.Get(callTop + nRet)
		if last.Type() == lua.LTFunction {
			postHook = last.(*lua.LFunction)
			nRet--
		}

		if nRet > 0 {
			first := L.Get(callTop + 1)
			if first.Type() == lua.LTString {
				bodyVal = first
				if resHeaders.RawGetString("Content-Type") == lua.LNil {
					resHeaders.RawSetString("Content-Type", lua.LString("text/html"))
				}
			} else if first.Type() == lua.LTTable && first != resTable {
				b, _ := json.Marshal(luaValueToInterface(first))
				bodyVal = lua.LString(string(b))
				if resHeaders.RawGetString("Content-Type") == lua.LNil {
					resHeaders.RawSetString("Content-Type", lua.LString("application/json"))
				}
			}
		}
	}

	if bodyVal != nil {
		resTable.RawSetString("body", bodyVal)
	}

	if cookiesTbl, ok := resTable.RawGetString("cookies").(*lua.LTable); ok {
		cookiesTbl.ForEach(func(k, v lua.LValue) {
			cookie := &http.Cookie{Name: k.String(), Path: "/"}
			if v.Type() == lua.LTString {
				cookie.Value = v.String()
			} else if tbl, ok := v.(*lua.LTable); ok {
				if val := tbl.RawGetString("delete"); val.Type() == lua.LTBool && val == lua.LTrue {
					cookie.MaxAge = -1
				} else {
					if val := tbl.RawGetString("value"); val.Type() != lua.LTNil {
						cookie.Value = val.String()
					}
					if val := tbl.RawGetString("path"); val.Type() != lua.LTNil {
						cookie.Path = val.String()
					}
					if val := tbl.RawGetString("http_only"); val.Type() == lua.LTBool {
						cookie.HttpOnly = bool(val.(lua.LBool))
					}
					if val := tbl.RawGetString("secure"); val.Type() == lua.LTBool {
						cookie.Secure = bool(val.(lua.LBool))
					}
					if val := tbl.RawGetString("max_age"); val.Type() == lua.LTNumber {
						cookie.MaxAge = int(val.(lua.LNumber))
					}
					if val := tbl.RawGetString("same_site"); val.Type() == lua.LTString {
						switch strings.ToLower(val.String()) {
						case "lax":
							cookie.SameSite = http.SameSiteLaxMode
						case "strict":
							cookie.SameSite = http.SameSiteStrictMode
						case "none":
							cookie.SameSite = http.SameSiteNoneMode
						}
					}
				}
			}
			http.SetCookie(w, cookie)
		})
	}

	launchPostHook := func() {
		if postHook != nil {
			handoff = true
			L.RemoveContext() 
			go func(state *lua.LState, hook *lua.LFunction, lTop int, rPath string) {
				defer func() {
					state.RemoveContext()
					state.SetTop(lTop)
					env.LuaPool.Put(state)
				}()

				bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer bgCancel()
				state.SetContext(bgCtx)

				state.Push(hook)
				if err := state.PCall(0, 0, nil); err != nil {
					env.appLog("error", rPath, "post-hook", err.Error())
				}
			}(L, postHook, top, route.FilePath)
		}
	}

	if filePath := resTable.RawGetString("_file_path"); filePath.Type() == lua.LTString {
		fileName := resTable.RawGetString("_file_name").String()
		w.Header().Set("Content-Disposition", "attachment; filename=\""+fileName+"\"")
		http.ServeFile(w, r, filePath.String())
		launchPostHook()
		return
	}

	resHeaders.ForEach(func(k, v lua.LValue) {
		w.Header()[k.String()] = []string{v.String()}
	})

	status := int(resTable.RawGetString("status").(lua.LNumber))
	w.WriteHeader(status)

	if finalBody := resTable.RawGetString("body"); finalBody.Type() == lua.LTString {
		w.Write([]byte(finalBody.String()))
	}

	launchPostHook()
}

func startServer(env *Environment, port int) {
	mux := http.NewServeMux()

	adminStatic := adminLockoutMiddleware(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/admin/")
		path = strings.TrimPrefix(path, "/")
		if path == "" {
			path = "admin.html"
		}
		if b, err := embedFS.ReadFile(path); err == nil {
			var contentType string
			switch filepath.Ext(path) {
			case ".html":
				contentType = "text/html"
			case ".css":
				contentType = "text/css"
			case ".js":
				contentType = "application/javascript"
			default:
				contentType = "text/plain"
			}
			w.Header().Set("Content-Type", contentType)
			w.Write(b)
			return
		}
		http.StripPrefix("/admin/", http.FileServer(http.FS(embedFS))).ServeHTTP(w, r)
	})
	mux.Handle("/admin/", adminStatic)

	mux.HandleFunc("/api/auth/login", adminLockoutMiddleware(env.handleAdminLogin))
	mux.HandleFunc("/api/auth/check", env.adminMiddleware(env.handleAuthCheck))
	if !env.IsStaging {
		mux.HandleFunc("/api/settings", env.adminMiddleware(env.handleAdminSettings))
		mux.HandleFunc("/api/backup", env.adminMiddleware(env.handleAdminBackupREST))
		mux.HandleFunc("/api/backup/download", env.adminMiddleware(env.handleAdminBackupDownload))
		mux.HandleFunc("/api/backup/restore", env.adminMiddleware(env.handleAdminBackupRestore))
		mux.HandleFunc("/api/keys", env.adminMiddleware(env.handleAdminKeys))
		mux.HandleFunc("/api/staging/sync", env.adminMiddleware(env.handleStagingSync))
	}
	mux.HandleFunc("/api/logs", env.adminMiddleware(env.handleAdminLogs))

	mux.HandleFunc("/api/crons", env.adminMiddleware(env.handleAdminCrons))
	mux.HandleFunc("/api/schedules/run", env.adminMiddleware(env.handleAdminSchedulesRun))
	mux.HandleFunc("/api/collections", env.adminMiddleware(env.handleAdminData))
	mux.HandleFunc("/api/collections/", env.adminMiddleware(env.handleAdminData))
	mux.HandleFunc("/api/files", env.adminMiddleware(env.handleAdminFiles))
	mux.HandleFunc("/api/files/rename", env.adminMiddleware(env.handleAdminFilesRename))

	mux.HandleFunc("/", env.corsAndRateLimitMiddleware(func(w http.ResponseWriter, r *http.Request) {
		cleanPath := filepath.Clean(filepath.FromSlash(r.URL.Path))
		targetPath := filepath.Join(env.PublicPath, cleanPath)

		absBase, err1 := filepath.Abs(env.PublicPath)
		absTarget, err2 := filepath.Abs(targetPath)

		if err1 == nil && err2 == nil && (absTarget == absBase || strings.HasPrefix(absTarget, absBase+string(filepath.Separator))) {
			if info, err := os.Stat(absTarget); err == nil {
				if !info.IsDir() {
					http.ServeFile(w, r, absTarget)
					return
				}
				indexTarget := filepath.Join(absTarget, "index.html")
				if idxInfo, idxErr := os.Stat(indexTarget); idxErr == nil && !idxInfo.IsDir() {
					if !strings.HasSuffix(r.URL.Path, "/") {
						http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
						return
					}
					http.ServeFile(w, r, indexTarget)
					return
				}
			}
		}

		env.mogoHandler(w, r)
	}))

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting Mogo %s API on http://localhost%s", map[bool]string{false: "Production", true: "Staging"}[env.IsStaging], addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func startStagingServer() {
	stagingMu.Lock()
	defer stagingMu.Unlock()
	
	if stagingStarted {
		return
	}

	if StagingEnv == nil {
		StagingEnv = NewEnvironment(filepath.Join(rootDir, "staging"), true)
	}
	StagingEnv.initDB()
	StagingEnv.initRoutes()
	StagingEnv.initLuaPool()
	StagingEnv.initCron()
	
	go StagingEnv.watchRoutes()

	stagingPort := appSettings.StagingPort
	if stagingPort <= 0 {
		stagingPort = 8090
	}
	
	stagingStarted = true
	go startServer(StagingEnv, stagingPort)
}

func main() {
	rootDir = "."
	cliPort := 0
	cliStagingPort := 0
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--dir=") {
			rootDir = strings.TrimPrefix(arg, "--dir=")
		}
		if strings.HasPrefix(arg, "--port=") {
			if p, err := strconv.Atoi(strings.TrimPrefix(arg, "--port=")); err == nil {
				cliPort = p
			}
		}
		if strings.HasPrefix(arg, "--staging-port=") {
			if p, err := strconv.Atoi(strings.TrimPrefix(arg, "--staging-port=")); err == nil {
				cliStagingPort = p
			}
		}
	}

	ProdEnv = NewEnvironment(rootDir, false)
	loadSettings()

	if cliStagingPort != 0 {
		appSettings.StagingPort = cliStagingPort
	}
	
	// Apply the CLI port override and fallbacks BEFORE initializing the DB
	if cliPort != 0 {
		appSettings.Port = cliPort
	}
	if appSettings.Port <= 0 {
		appSettings.Port = 8080
	}

	ProdEnv.initDB()

	for _, arg := range os.Args[1:] {
		if arg == "--new-key" {
			generateNewMasterKey()
			os.Exit(0)
		}
	}

	ProdEnv.initRoutes()
	ProdEnv.initCron()
	
	go ProdEnv.watchRoutes()

	go startServer(ProdEnv, appSettings.Port)

	if appSettings.StagingEnabled {
		startStagingServer()
	}

	select {}
}
