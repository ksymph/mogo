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
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	lua "github.com/yuin/gopher-lua"
	_ "modernc.org/sqlite"
)

//go:embed admin.html style.css prism-code.js
var embedFS embed.FS

var (
	rootDir     string
	dataPath    string
	publicPath  string
	routesPath  string
	scriptsPath string
)

// -----------------------------------------------------------------------------
// 0. SETTINGS & CONFIG
// -----------------------------------------------------------------------------
type Settings struct {
	BackupDestDir   string `json:"backup_dest_dir"`
	BackupFull      bool   `json:"backup_full"`
	BackupSched     string `json:"backup_sched"`
	BackupSchedMeta string `json:"backup_sched_meta"`

	AllowedOrigins      string  `json:"allowed_origins"`
	RateLimitRPS        float64 `json:"rate_limit_rps"`
	RateLimitBurst      int     `json:"rate_limit_burst"`
	AdminMaxRetries int `json:"admin_max_retries"`

	LogRetentionDays int  `json:"log_retention_days"`
	UnsafeLua        bool `json:"unsafe_lua"`
	Port             int  `json:"port"`
}

var appSettings Settings

func loadSettings() {
	os.MkdirAll(dataPath, os.ModePerm)
	os.MkdirAll(scriptsPath, os.ModePerm)
	os.MkdirAll(routesPath, os.ModePerm)
	os.MkdirAll(publicPath, os.ModePerm)

	appSettings.RateLimitRPS = 100
	appSettings.RateLimitBurst = 200
	appSettings.AdminMaxRetries = 5
	appSettings.LogRetentionDays = 30
	appSettings.Port = 8080

	b, err := os.ReadFile(filepath.Join(dataPath, "settings.json"))
	if err == nil {
		json.Unmarshal(b, &appSettings)
	}

	initLuaPool()
}

func saveSettings(s Settings) {
	b, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(filepath.Join(dataPath, "settings.json"), b, 0644)
	appSettings = s
	initLuaPool() // Re-initialize pool to apply UnsafeLua changes
}

// -----------------------------------------------------------------------------
// 1. DATABASE, CACHE & LIMITERS
// -----------------------------------------------------------------------------
var dbConn *sql.DB
var logDBConn *sql.DB
var schemaCache sync.Map
var scheduler gocron.Scheduler

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

func initDB() {
	var err error
	dbPath := filepath.Join(dataPath, "database.sqlite")
	dbConn, err = sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	initSystemTables()
	initMasterKey()

	logDBPath := filepath.Join(dataPath, "log.sqlite")
	logDBConn, err = sql.Open("sqlite", logDBPath+"?_journal_mode=WAL")
	if err != nil {
		log.Fatalf("Failed to open Log database: %v", err)
	}
	_, err = logDBConn.Exec(`CREATE TABLE IF NOT EXISTS _logs (
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

func appLog(level, scriptPath, origin, message string) {
	if logDBConn == nil {
		return
	}
	_, err := logDBConn.Exec(`INSERT INTO _logs (level, script_path, origin, message) VALUES (?, ?, ?, ?)`, level, scriptPath, origin, message)
	if err != nil {
		log.Printf("Failed to write to app log: %v", err)
	}
}

func initSystemTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS _collections (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			type TEXT DEFAULT 'base',
			created TEXT,
			updated TEXT
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
		`CREATE TABLE IF NOT EXISTS _api_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			key TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			permissions TEXT DEFAULT '{}',
			created TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS _cron_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			schedule TEXT NOT NULL,
			schedule_meta TEXT,
			script_path TEXT NOT NULL,
			active BOOLEAN DEFAULT 1,
			created TEXT
		);`,
	}
	for _, q := range queries {
		if _, err := dbConn.Exec(q); err != nil {
			log.Fatalf("System Table Init Error: %v", err)
		}
	}

	dbConn.Exec("ALTER TABLE _cron_jobs ADD COLUMN schedule_meta TEXT")
	dbConn.Exec("ALTER TABLE _schema ADD COLUMN position INTEGER DEFAULT 0")
}

func initMasterKey() {
	var count int
	dbConn.QueryRow("SELECT COUNT(*) FROM _api_keys").Scan(&count)
	if count == 0 {
		b := make([]byte, 16)
		crand.Read(b)
		newKey := "sk_" + hex.EncodeToString(b)

		allPerms := `{"collections":{"*":["item","schema"]},"routes":true,"schedules":true,"public":true,"settings":true,"keys":true,"logs":true}`
		dbConn.Exec("INSERT INTO _api_keys (key, name, permissions, created) VALUES (?, ?, ?, ?)",
			newKey, "admin", allPerms, time.Now().Format("2006-01-02 15:04:05"))

		fmt.Println("\n=================================================================")
		fmt.Printf(" INITIAL API KEY: %s\n", newKey)
		fmt.Printf(" Auto-login link: http://localhost:8080/admin/?key=%s\n", newKey)
		fmt.Println("=================================================================")
	}
}

func generateNewMasterKey() {
	b := make([]byte, 16)
	crand.Read(b)
	newKey := "sk_" + hex.EncodeToString(b)

	allPerms := `{"collections":{"*":["item","schema"]},"routes":true,"schedules":true,"public":true,"settings":true,"keys":true,"logs":true}`
	dbConn.Exec("INSERT INTO _api_keys (key, name, permissions, created) VALUES (?, ?, ?, ?)",
		newKey, "cli-generated", allPerms, time.Now().Format("2006-01-02 15:04:05"))

	fmt.Println("\n=================================================================")
	fmt.Printf(" NEW API KEY GENERATED: %s\n", newKey)
	fmt.Println("=================================================================")
}

// -----------------------------------------------------------------------------
// 1.5. CRON JOBS
// -----------------------------------------------------------------------------
func initCron() {
	if scheduler != nil {
		scheduler.Shutdown()
	}

	s, err := gocron.NewScheduler()
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}
	scheduler = s

	s.NewJob(
		gocron.DailyJob(1, gocron.NewAtTimes(gocron.NewAtTime(0, 0, 0))),
		gocron.NewTask(func() {
			retention := appSettings.LogRetentionDays
			if retention <= 0 {
				retention = 30
			}
			if logDBConn != nil {
				_, err := logDBConn.Exec(fmt.Sprintf("DELETE FROM _logs WHERE timestamp < datetime('now', '-%d days')", retention))
				if err != nil {
					appLog("error", "system", "cron", "Failed to prune logs: "+err.Error())
				}
			}
		}),
	)

	rows, err := dbConn.Query("SELECT id, schedule, schedule_meta, script_path FROM _cron_jobs WHERE active = 1")
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
			var id int
			var schedule, scriptPath string
			var scheduleMeta sql.NullString
			rows.Scan(&id, &schedule, &scheduleMeta, &scriptPath)

			fullPath := filepath.Join(scriptsPath, scriptPath)

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

			_, err := scheduler.NewJob(
				jobDef,
				gocron.NewTask(runCronScript, fullPath),
			)
			if err != nil {
				log.Printf("Failed to load cron job %d: %v", id, err)
			}
		}
	}

	if appSettings.BackupSched != "" && appSettings.BackupSched != "Disabled" {
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

		_, err := scheduler.NewJob(
			jobDef,
			gocron.NewTask(func() {
				createBackup(appSettings.BackupDestDir, appSettings.BackupFull)
			}),
		)
		if err != nil {
			log.Printf("Failed to load backup cron job: %v", err)
		}
	}

	scheduler.Start()
}

func runCronScript(scriptPath string) {
	L := luaPool.Get().(*lua.LState)
	defer luaPool.Put(L)
	top := L.GetTop()
	defer L.SetTop(top)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	L.SetContext(ctx)
	defer L.RemoveContext()

	fn, err := L.LoadFile(scriptPath)
	if err != nil {
		log.Printf("Cron error loading %s: %v", scriptPath, err)
		return
	}

	env := L.NewTable()
	mt := L.NewTable()
	L.SetField(mt, "__index", L.Get(lua.GlobalsIndex))
	L.SetMetatable(env, mt)
	fn.Env = env

	L.Push(fn)
	if err := L.PCall(0, 0, nil); err != nil {
		log.Printf("Cron error running %s: %v", scriptPath, err)
	}
}

func createBackup(destDir string, full bool) error {
	if destDir == "" {
		destDir = filepath.Join(dataPath, "backups")
	}
	os.MkdirAll(destDir, os.ModePerm)
	timestamp := time.Now().Format("2006-01-02_15-04-05")

	if !full {
		src := filepath.Join(dataPath, "database.sqlite")
		dst := filepath.Join(destDir, timestamp+".sqlite")
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(dst)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	}

	dst := filepath.Join(destDir, timestamp+".zip")
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	w := zip.NewWriter(out)
	defer w.Close()
	destDirAbs, _ := filepath.Abs(destDir)

	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		absPath, _ := filepath.Abs(path)

		if strings.HasPrefix(absPath, destDirAbs) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if info.IsDir() && info.Name() == ".git" {
			return filepath.SkipDir
		}
		if info.IsDir() {
			return nil
		}

		fh, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		fh.Name = filepath.ToSlash(path)
		fh.Method = zip.Deflate

		f, err := w.CreateHeader(fh)
		if err != nil {
			return err
		}

		in, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer in.Close()

		_, err = io.Copy(f, in)
		return err
	})
	return err
}

type SchemaField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

func getCollectionSchema(collection string) []SchemaField {
	if cached, ok := schemaCache.Load(collection); ok {
		return cached.([]SchemaField)
	}
	var schema []SchemaField = []SchemaField{}
	q := "SELECT s.field, s.type, s.required FROM _schema s JOIN _collections c ON s.collection_id = c.id WHERE c.name = ? ORDER BY s.position ASC, s.id ASC"
	rows, err := dbConn.Query(q, collection)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var field, typ string
			var required bool
			rows.Scan(&field, &typ, &required)
			schema = append(schema, SchemaField{Name: field, Type: typ, Required: required})
		}
	}
	schemaCache.Store(collection, schema)
	return schema
}

func createCollectionInDB(name string, schema []SchemaField) error {
	var exists int
	dbConn.QueryRow("SELECT COUNT(*) FROM _collections WHERE name = ?", name).Scan(&exists)
	if exists > 0 {
		return fmt.Errorf("collection already exists")
	}

	res, err := dbConn.Exec("INSERT INTO _collections (name, created, updated) VALUES (?, ?, ?)",
		name, time.Now().Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"))
	if err != nil {
		return err
	}
	collectionID, _ := res.LastInsertId()

	sqlFields := []string{"id INTEGER PRIMARY KEY AUTOINCREMENT", "created TEXT", "updated TEXT"}
	for i, field := range schema {
		sqlType := "TEXT"
		switch field.Type {
		case "number":
			sqlType = "REAL"
		case "bool":
			sqlType = "BOOLEAN"
		}
		dbConn.Exec("INSERT INTO _schema (collection_id, field, type, required, position) VALUES (?, ?, ?, ?, ?)",
			collectionID, field.Name, field.Type, field.Required, i)
		sqlFields = append(sqlFields, fmt.Sprintf("%s %s", field.Name, sqlType))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", name, strings.Join(sqlFields, ", "))
	_, err = dbConn.Exec(createSQL)
	schemaCache.Delete(name)
	return err
}

func updateCollectionInDB(name string, schema []SchemaField) error {
	var collectionID int
	err := dbConn.QueryRow("SELECT id FROM _collections WHERE name = ?", name).Scan(&collectionID)
	if err != nil {
		return fmt.Errorf("collection not found")
	}

	existingSchema := make(map[string]string)
	rows, err := dbConn.Query("SELECT field, type FROM _schema WHERE collection_id = ?", collectionID)
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
			dbConn.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", name, field.Name, sqlType))
			dbConn.Exec("INSERT INTO _schema (collection_id, field, type, required, position) VALUES (?, ?, ?, ?, ?)",
				collectionID, field.Name, field.Type, field.Required, i)
		} else {
			dbConn.Exec("UPDATE _schema SET type = ?, required = ?, position = ? WHERE collection_id = ? AND field = ?", field.Type, field.Required, i, collectionID, field.Name)
		}
	}

	for existingField := range existingSchema {
		if !newFields[existingField] {
			dbConn.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", name, existingField))
			dbConn.Exec("DELETE FROM _schema WHERE collection_id = ? AND field = ?", collectionID, existingField)
		}
	}

	dbConn.Exec("UPDATE _collections SET updated = ? WHERE id = ?", time.Now().Format("2006-01-02 15:04:05"), collectionID)
	schemaCache.Delete(name)
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
	ip := strings.Split(r.RemoteAddr, ":")[0]
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

func corsAndRateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
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

func adminMiddleware(next http.HandlerFunc) http.HandlerFunc {
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
		var permissions string
		err := dbConn.QueryRow("SELECT permissions FROM _api_keys WHERE key = ?", key).Scan(&permissions)
		if err == nil {
			recordAdminAttempt(ip, true)
			r.Header.Set("X-Admin-Perms", permissions)
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

func handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method Not Allowed", 405)
		return
	}
	var creds struct {
		APIKey string `json:"api_key"`
	}
	json.NewDecoder(r.Body).Decode(&creds)

	ip := getIP(r)
	var permissions string
	err := dbConn.QueryRow("SELECT permissions FROM _api_keys WHERE key = ?", creds.APIKey).Scan(&permissions)
	if err == nil {
		recordAdminAttempt(ip, true)
		setAuthCookie(w, creds.APIKey)
		w.Write([]byte(`{"success":true}`))
		return
	}
	
	recordAdminAttempt(ip, false)
	http.Error(w, "Invalid API Key", 401)
}

func handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"permissions": r.Header.Get("X-Admin-Perms")})
}

func handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		json.NewEncoder(w).Encode(appSettings)
	} else if r.Method == "POST" {
		var s Settings
		json.NewDecoder(r.Body).Decode(&s)
		saveSettings(s)
		initCron()
		w.Write([]byte(`{"success":true}`))
	}
}

func handleAdminBackup(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "settings") {
		http.Error(w, "Forbidden", 403)
		return
	}
	if r.Method == "POST" {
		err := createBackup(appSettings.BackupDestDir, appSettings.BackupFull)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}
}

func handleAdminLogs(w http.ResponseWriter, r *http.Request) {
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
		logs, err := queryDB(logDBConn, query, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var total int
		countQuery := "SELECT COUNT(*) FROM _logs WHERE " + whereClause
		logDBConn.QueryRow(countQuery, args...).Scan(&total)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"logs":  logs,
			"total": total,
		})
	} else if r.Method == "DELETE" {
		_, err := logDBConn.Exec("DELETE FROM _logs")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}
}

func handleAdminKeys(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "keys") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		rows, _ := dbConn.Query("SELECT id, name, key, permissions, created FROM _api_keys ORDER BY id DESC")
		defer rows.Close()
		results := []map[string]any{}
		for rows.Next() {
			var id int
			var name, key, perms, created string
			rows.Scan(&id, &name, &key, &perms, &created)
			var pMap map[string]any
			json.Unmarshal([]byte(perms), &pMap)
			results = append(results, map[string]any{"id": id, "name": name, "key": key, "permissions": pMap, "created": created})
		}
		json.NewEncoder(w).Encode(results)
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

		pBytes, _ := json.Marshal(req.Permissions)
		permsStr := string(pBytes)

		if req.ID > 0 {
			dbConn.Exec("UPDATE _api_keys SET name=?, permissions=? WHERE id=?", req.Name, permsStr, req.ID)
			w.Write([]byte(`{"success":true}`))
			return
		}

		b := make([]byte, 16)
		crand.Read(b)
		newKey := "sk_" + hex.EncodeToString(b)
		dbConn.Exec("INSERT INTO _api_keys (key, name, permissions, created) VALUES (?, ?, ?, ?)",
			newKey, req.Name, permsStr, time.Now().Format("2006-01-02 15:04:05"))

		w.Write([]byte(`{"success":true, "key":"` + newKey + `"}`))
		return
	}
	if r.Method == "DELETE" {
		id := r.URL.Query().Get("id")
		dbConn.Exec("DELETE FROM _api_keys WHERE id = ?", id)
		w.Write([]byte(`{"success":true}`))
		return
	}
}

func handleAdminCrons(w http.ResponseWriter, r *http.Request) {
	if !hasPermission(r, "schedules") {
		http.Error(w, "Forbidden", 403)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		rows, _ := dbConn.Query("SELECT id, name, schedule, schedule_meta, script_path, active, created FROM _cron_jobs ORDER BY id DESC")
		defer rows.Close()
		results := []map[string]any{}
		for rows.Next() {
			var id, active int
			var scheduleMeta sql.NullString
			var name, schedule, scriptPath, created string
			rows.Scan(&id, &name, &schedule, &scheduleMeta, &scriptPath, &active, &created)
			results = append(results, map[string]any{
				"id":            id,
				"name":          name,
				"schedule":      schedule,
				"schedule_meta": scheduleMeta.String,
				"script_path":   scriptPath,
				"active":        active == 1,
				"created":       created,
			})
		}
		json.NewEncoder(w).Encode(results)
		return
	}
	if r.Method == "POST" {
		var req struct {
			ID           int    `json:"id"`
			Name         string `json:"name"`
			Schedule     string `json:"schedule"`
			ScheduleMeta string `json:"schedule_meta"`
			ScriptPath   string `json:"script_path"`
			Active       bool   `json:"active"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		activeInt := 0
		if req.Active {
			activeInt = 1
		}

		if req.ID > 0 {
			dbConn.Exec("UPDATE _cron_jobs SET name=?, schedule=?, schedule_meta=?, script_path=?, active=? WHERE id=?",
				req.Name, req.Schedule, req.ScheduleMeta, req.ScriptPath, activeInt, req.ID)
		} else {
			dbConn.Exec("INSERT INTO _cron_jobs (name, schedule, schedule_meta, script_path, active, created) VALUES (?, ?, ?, ?, ?, ?)",
				req.Name, req.Schedule, req.ScheduleMeta, req.ScriptPath, activeInt, time.Now().Format("2006-01-02 15:04:05"))
		}
		initCron()
		w.Write([]byte(`{"success":true}`))
		return
	}
	if r.Method == "DELETE" {
		id := r.URL.Query().Get("id")
		dbConn.Exec("DELETE FROM _cron_jobs WHERE id = ?", id)
		initCron()
		w.Write([]byte(`{"success":true}`))
		return
	}
}

func handleAdminData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api/collections")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if path == "" || path == "/" {
		if r.Method == "GET" {
			rows, _ := dbConn.Query("SELECT * FROM _collections")
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
					row["schema"] = getCollectionSchema(cName)
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
			if err := createCollectionInDB(req.Name, req.Schema); err != nil {
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

					schema := getCollectionSchema(collection)
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
					schema := getCollectionSchema(collection)
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
			dbConn.QueryRow("SELECT COUNT(*) FROM "+collection+" WHERE "+whereClause, args...).Scan(&total)

			query := "SELECT * FROM " + collection + " WHERE " + whereClause + " ORDER BY id DESC"
			items, err := queryDB(dbConn, query, args...)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"items": items, "total": total})
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
				data["created"] = time.Now().Format("2006-01-02 15:04:05")
			}
			if _, ok := data["updated"]; !ok {
				data["updated"] = time.Now().Format("2006-01-02 15:04:05")
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
			if _, err := dbConn.Exec(q, args...); err != nil {
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
			if err := updateCollectionInDB(collection, req.Schema); err != nil {
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
					dbConn.Exec("DELETE FROM "+collection+" WHERE id = ?", idStr)
				}
				w.Write([]byte(`{"success":true}`))
				return
			}
			if !hasCollectionPermission(r, collection, "schema") {
				http.Error(w, "Forbidden", 403)
				return
			}
			dbConn.Exec("DROP TABLE " + collection)
			dbConn.Exec("DELETE FROM _schema WHERE collection_id = (SELECT id FROM _collections WHERE name = ?)", collection)
			dbConn.Exec("DELETE FROM _collections WHERE name = ?", collection)
			schemaCache.Delete(collection)
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
			data["updated"] = time.Now().Format("2006-01-02 15:04:05")

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
			if _, err := dbConn.Exec(q, args...); err != nil {
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
			dbConn.Exec("DELETE FROM "+collection+" WHERE id = ?", id)
			w.Write([]byte(`{"success":true}`))
			return
		}
	}
}

func handleAdminFiles(w http.ResponseWriter, r *http.Request) {
	base := r.URL.Query().Get("base")
	baseDir := publicPath
	permKey := "public"

	if base == "routes" {
		baseDir = routesPath
		permKey = "routes"
	}
	if base == "schedules" {
		baseDir = scriptsPath
		permKey = "schedules"
	}

	if !hasPermission(r, permKey) {
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
			initRoutes()
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
			initRoutes()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true}`))
	}
}

// -----------------------------------------------------------------------------
// 4. LUA EXTENSIONS & ENGINE
// -----------------------------------------------------------------------------
func injectDB(L *lua.LState) {
	dbTbl := L.NewTable()
	dbMeta := L.NewTable()
	L.SetField(dbMeta, "__index", L.NewFunction(func(L *lua.LState) int {
		colName := L.CheckString(2)
		colTbl := L.NewTable()
		colTbl.RawSetString("_name", lua.LString(colName))

		// db.<collection>:table()
		colTbl.RawSetString("table", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()

			// Cache to prevent duplicate tables / circular references
			tableCache := make(map[string]*lua.LTable)

			// Valid collections to resolve relations
			colSet := make(map[string]bool)
			rows, err := dbConn.Query("SELECT name FROM _collections")
			if err == nil {
				for rows.Next() {
					var n string
					rows.Scan(&n)
					colSet[n] = true
				}
				rows.Close()
			}

			// Pre-fetch schemas
			schemas := make(map[string][]SchemaField)
			getSchema := func(c string) []SchemaField {
				if s, ok := schemas[c]; ok {
					return s
				}
				s := getCollectionSchema(c)
				schemas[c] = s
				return s
			}

			var buildRow func(colName string, row map[string]interface{}) *lua.LTable
			var getRow func(colName string, idStr string) *lua.LTable

			getRow = func(colName string, idStr string) *lua.LTable {
				cacheKey := colName + ":" + idStr
				if tbl, ok := tableCache[cacheKey]; ok {
					return tbl
				}
				res, err := queryDB(dbConn, fmt.Sprintf("SELECT * FROM %s WHERE id = ?", colName), idStr)
				if err != nil || len(res) == 0 {
					return nil
				}
				return buildRow(colName, res[0])
			}

			buildRow = func(colName string, row map[string]interface{}) *lua.LTable {
				idStr := fmt.Sprintf("%v", row["id"])
				cacheKey := colName + ":" + idStr

				if existing, ok := tableCache[cacheKey]; ok {
					return existing
				}

				tbl := L.NewTable()
				tableCache[cacheKey] = tbl // Register early to handle circular references securely

				schema := getSchema(colName)

				for k, v := range row {
					if v == nil {
						continue
					}
					var fType string
					for _, sf := range schema {
						if sf.Name == k {
							fType = sf.Type
							break
						}
					}

					if fType != "" && colSet[fType] {
						// It's a relation
						vStr := strings.TrimSpace(fmt.Sprintf("%v", v))
						if vStr != "" && vStr != "nil" {
							var idList []interface{}
							isArray := strings.HasPrefix(vStr, "[")

							if isArray {
								if err := json.Unmarshal([]byte(vStr), &idList); err != nil {
									idList = []interface{}{vStr} // Fallback to raw string array
									isArray = false
								}
							} else {
								idList = []interface{}{vStr}
							}

							if isArray {
								relArr := L.NewTable()
								arrIdx := 1
								for _, relId := range idList {
									relTbl := getRow(fType, fmt.Sprintf("%v", relId))
									if relTbl != nil {
										relArr.RawSetInt(arrIdx, relTbl)
										arrIdx++
									}
								}
								tbl.RawSetString(k, relArr)
							} else {
								relTbl := getRow(fType, fmt.Sprintf("%v", idList[0]))
								if relTbl != nil {
									tbl.RawSetString(k, relTbl)
								} else {
									tbl.RawSetString(k, toLValue(L, v)) // Fallback if record not found
								}
							}
						} else {
							tbl.RawSetString(k, toLValue(L, v))
						}
					} else {
						// Normal field
						tbl.RawSetString(k, toLValue(L, v))
					}
				}
				return tbl
			}

			results, err := queryDB(dbConn, fmt.Sprintf("SELECT * FROM %s ORDER BY id ASC", cName))
			if err != nil {
				L.Push(L.NewTable())
				return 1
			}

			arr := L.NewTable()
			for i, r := range results {
				rowTbl := buildRow(cName, r)
				if rowTbl != nil {
					arr.RawSetInt(i+1, rowTbl)
				}
			}

			L.Push(arr)
			return 1
		}))

		// db.<collection>:find(query)
		colTbl.RawSetString("find", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			queryTbl := L.OptTable(2, L.NewTable())

			whereVals := []any{}
			whereCols := []string{"1=1"}
			if queryMap, ok := luaValueToInterface(queryTbl).(map[string]any); ok {
				for k, v := range queryMap {
					whereCols = append(whereCols, fmt.Sprintf("%s = ?", k))
					whereVals = append(whereVals, v)
				}
			}

			q := fmt.Sprintf("SELECT * FROM %s WHERE %s ORDER BY id ASC", cName, strings.Join(whereCols, " AND "))
			results, err := queryDB(dbConn, q, whereVals...)
			if err != nil {
				L.Push(L.NewTable())
				return 1
			}

			arr := L.NewTable()
			for i, r := range results {
				arr.RawSetInt(i+1, mapToLTable(L, r))
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

			now := time.Now().Format("2006-01-02 15:04:05")
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
			res, err := dbConn.Exec(q, args...)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			id, _ := res.LastInsertId()
			data["id"] = id
			L.Push(mapToLTable(L, data))
			return 1
		}))

		// db.<collection>:update(query, { $set = {...} })
		colTbl.RawSetString("update", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			queryMap, _ := luaValueToInterface(L.OptTable(2, L.NewTable())).(map[string]any)
			updateMap, _ := luaValueToInterface(L.OptTable(3, L.NewTable())).(map[string]any)

			if setObj, ok := updateMap["$set"].(map[string]any); ok {
				updateMap = setObj
			}
			updateMap["updated"] = time.Now().Format("2006-01-02 15:04:05")

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
			_, err := dbConn.Exec(q, args...)
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
			_, err := dbConn.Exec(q, whereVals...)
			if err != nil {
				L.Push(lua.LBool(false))
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(lua.LBool(true))
			return 1
		}))

		// Implement callable metatable so any collection (or "exec") can safely handle raw SQL routing
		colMeta := L.NewTable()
		L.SetField(colMeta, "__call", L.NewFunction(func(L *lua.LState) int {
			arg1 := L.Get(2)
			var query string
			var startArg int

			// Determine if called as db:exec(...) or db.exec(...) based on first arg type
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
				results, err := queryDB(dbConn, query, args...)
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
				res, err := dbConn.Exec(query, args...)
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

var routes []Route
var routesMu sync.RWMutex

func initRoutes() {
	paramRegex := regexp.MustCompile(`\[([^/]+)\]`)
	var newRoutes []Route
	filepath.Walk(routesPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".lua" {
			return nil
		}
		relPath, _ := filepath.Rel(routesPath, path)
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

	routesMu.Lock()
	routes = newRoutes
	routesMu.Unlock()
}

func matchRoute(urlPath string) (*Route, map[string]string) {
	routesMu.RLock()
	defer routesMu.RUnlock()

	for _, r := range routes {
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

var luaPool *sync.Pool

func createLuaState() *lua.LState {
	L := lua.NewState(lua.Options{
		SkipOpenLibs: true,
	})

	// Load explicitly safe core libraries
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

	if !appSettings.UnsafeLua {
		// Strip dangerous OS functions
		osTbl := L.GetGlobal("os").(*lua.LTable)
		dangerous := []string{"execute", "exit", "remove", "rename", "setenv", "getenv", "tmpname"}
		for _, op := range dangerous {
			L.SetField(osTbl, op, lua.LNil)
		}
	} else {
		// Load unsafe libraries
		L.Push(L.NewFunction(lua.OpenIo))
		L.Push(lua.LString(lua.IoLibName))
		L.Call(1, 0)

		L.Push(L.NewFunction(lua.OpenDebug))
		L.Push(lua.LString(lua.DebugLibName))
		L.Call(1, 0)
	}

	// Inject Mogo API
	injectDB(L)
	injectMgoAPI(L)

	return L
}

func initLuaPool() {
	luaPool = &sync.Pool{
		New: func() any {
			return createLuaState()
		},
	}
}

func injectMgoAPI(L *lua.LState) {
	logMod := L.NewTable()
	L.SetField(logMod, "info", L.NewFunction(func(L *lua.LState) int {
		appLog("info", getScript(L), "lua", L.CheckString(1))
		return 0
	}))
	L.SetField(logMod, "warn", L.NewFunction(func(L *lua.LState) int {
		appLog("warn", getScript(L), "lua", L.CheckString(1))
		return 0
	}))
	L.SetField(logMod, "error", L.NewFunction(func(L *lua.LState) int {
		appLog("error", getScript(L), "lua", L.CheckString(1))
		return 0
	}))
	L.SetField(logMod, "get", L.NewFunction(func(L *lua.LState) int {
		limit := 1
		argIdx := 1
		// Handle colon syntax `log:get(5)` where the module table is the first argument
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
		if logDBConn != nil {
			rows, err := logDBConn.Query("SELECT id, timestamp, level, origin, script_path, message FROM _logs ORDER BY id DESC LIMIT ?", limit)
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

	httpMod := L.NewTable()
	L.SetField(httpMod, "get", L.NewFunction(luaHttpGet))
	L.SetField(httpMod, "request", L.NewFunction(luaHttpRequest))
	L.SetGlobal("http", httpMod)
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
		headersVal.(*lua.LTable).ForEach(func(k, v lua.LValue) { req.Header.Set(k.String(), v.String()) })
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

func luaHttpGet(L *lua.LState) int { return luaHttpRequestReal(L, "GET") }
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

func mogoHandler(w http.ResponseWriter, r *http.Request) {
	route, params := matchRoute(r.URL.Path)
	if route == nil {
		http.Error(w, "404 Not Found", 404)
		return
	}

	L := luaPool.Get().(*lua.LState)
	defer luaPool.Put(L)

	top := L.GetTop()
	defer L.SetTop(top)

	// Timeouts for requests to prevent infinite loops (DOS protection)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	L.SetContext(ctx)
	defer L.RemoveContext()

	fn, err := L.LoadFile(route.FilePath)
	if err != nil {
		appLog("error", route.FilePath, "router", err.Error())
		http.Error(w, "500 Internal Server Error", 500)
		return
	}

	// Environment isolation to prevent leaking global state between pooled requests
	env := L.NewTable()
	mt := L.NewTable()
	L.SetField(mt, "__index", L.Get(lua.GlobalsIndex))
	L.SetMetatable(env, mt)
	fn.Env = env

	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		appLog("error", route.FilePath, "execution", err.Error())
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

	// Build req table
	reqTable := L.NewTable()
	reqTable.RawSetString("method", lua.LString(r.Method))
	reqTable.RawSetString("path", lua.LString(r.URL.Path))

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
			headerTable.RawSetString(strings.ToLower(k), lua.LString(v[0]))
		}
	}
	reqTable.RawSetString("headers", headerTable)

	cookiesTable := L.NewTable()
	for _, c := range r.Cookies() {
		cookiesTable.RawSetString(c.Name, lua.LString(c.Value))
	}
	reqTable.RawSetString("cookies", cookiesTable)

	// Body Parsing
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

					// Security Jail check
					if !appSettings.UnsafeLua {
						absBase, _ := filepath.Abs(publicPath)
						absTarget, _ := filepath.Abs(filepath.Join(publicPath, destPath))
						if !strings.HasPrefix(absTarget, absBase) {
							L.Push(lua.LBool(false))
							L.Push(lua.LString("access denied: path escapes secure directory"))
							return 2
						}
						destPath = absTarget
					}

					srcFile, err := fileHeader.Open()
					if err != nil {
						L.Push(lua.LBool(false))
						L.Push(lua.LString(err.Error()))
						return 2
					}
					defer srcFile.Close()

					destFile, err := os.Create(destPath)
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

	// Build res table
	resTable := L.NewTable()
	resTable.RawSetString("status", lua.LNumber(200))
	resTable.RawSetString("headers", L.NewTable())
	resTable.RawSetString("cookies", L.NewTable())

	fileFunc := L.NewFunction(func(L *lua.LState) int {
		pathStr := L.CheckString(2)
		filename := L.OptString(3, filepath.Base(pathStr))

		// Security Jail check
		if !appSettings.UnsafeLua {
			absBase, _ := filepath.Abs(publicPath)
			absTarget, _ := filepath.Abs(filepath.Join(publicPath, pathStr))
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
		appLog("error", route.FilePath, "execution", err.Error())
		http.Error(w, "500 Internal Server Error: execution failed", 500)
		return
	}

	resHeaders := resTable.RawGetString("headers").(*lua.LTable)
	nRet := L.GetTop() - callTop

	// Process implicit Returns
	if nRet > 0 {
		first := L.Get(callTop + 1)
		if nRet == 1 {
			if first.Type() == lua.LTString {
				resTable.RawSetString("body", first)
				if resHeaders.RawGetString("Content-Type") == lua.LNil {
					resHeaders.RawSetString("Content-Type", lua.LString("text/html"))
				}
			} else if first.Type() == lua.LTTable {
				b, _ := json.Marshal(luaValueToInterface(first))
				resTable.RawSetString("body", lua.LString(string(b)))
				if resHeaders.RawGetString("Content-Type") == lua.LNil {
					resHeaders.RawSetString("Content-Type", lua.LString("application/json"))
				}
			}
		} else if nRet >= 2 {
			second := L.Get(callTop + 2)
			if first.Type() == lua.LTNumber && second.Type() == lua.LTString {
				resTable.RawSetString("status", first)
				resTable.RawSetString("body", second)
				if resHeaders.RawGetString("Content-Type") == lua.LNil {
					resHeaders.RawSetString("Content-Type", lua.LString("text/html"))
				}
			}
		}
	}

	// Set Cookies
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

	// Trigger File Download
	if filePath := resTable.RawGetString("_file_path"); filePath.Type() == lua.LTString {
		fileName := resTable.RawGetString("_file_name").String()
		w.Header().Set("Content-Disposition", "attachment; filename=\""+fileName+"\"")
		http.ServeFile(w, r, filePath.String())
		return
	}

	// Apply Headers
	resHeaders.ForEach(func(k, v lua.LValue) {
		w.Header().Set(k.String(), v.String())
	})

	// Dispatch Response Status and Body
	status := int(resTable.RawGetString("status").(lua.LNumber))
	w.WriteHeader(status)

	if bodyVal := resTable.RawGetString("body"); bodyVal.Type() == lua.LTString {
		w.Write([]byte(bodyVal.String()))
	}
}

func main() {
	rootDir = "."
	cliPort := 0
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--dir=") {
			rootDir = strings.TrimPrefix(arg, "--dir=")
		}
		if strings.HasPrefix(arg, "--port=") {
			if p, err := strconv.Atoi(strings.TrimPrefix(arg, "--port=")); err == nil {
				cliPort = p
			}
		}
}

	dataPath = filepath.Join(rootDir, "data")
	publicPath = filepath.Join(rootDir, "public")
	routesPath = filepath.Join(rootDir, "routes")
	scriptsPath = filepath.Join(rootDir, "scripts")

	loadSettings()
	initDB()

	for _, arg := range os.Args[1:] {
		if arg == "--new-key" {
			generateNewMasterKey()
			os.Exit(0)
		}
	}

	initRoutes()
	initCron()

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
	http.Handle("/admin/", adminStatic)

	http.HandleFunc("/api/auth/login", adminLockoutMiddleware(handleAdminLogin))
	http.HandleFunc("/api/auth/check", adminMiddleware(handleAuthCheck))
	http.HandleFunc("/api/settings", adminMiddleware(handleAdminSettings))
	http.HandleFunc("/api/backup", adminMiddleware(handleAdminBackup))
	http.HandleFunc("/api/keys", adminMiddleware(handleAdminKeys))
	http.HandleFunc("/api/logs", adminMiddleware(handleAdminLogs))

	http.HandleFunc("/api/crons", adminMiddleware(handleAdminCrons))
	http.HandleFunc("/api/collections", adminMiddleware(handleAdminData))
	http.HandleFunc("/api/collections/", adminMiddleware(handleAdminData))
	http.HandleFunc("/api/files", adminMiddleware(handleAdminFiles))

	http.HandleFunc("/", corsAndRateLimitMiddleware(func(w http.ResponseWriter, r *http.Request) {
		// Clean the requested path and securely bind it to the designated files directory
		cleanPath := filepath.Clean(filepath.FromSlash(r.URL.Path))
		targetPath := filepath.Join(publicPath, cleanPath)

		absBase, err1 := filepath.Abs(publicPath)
		absTarget, err2 := filepath.Abs(targetPath)

		// Ensure the requested file doesn't escape the secure directory
		if err1 == nil && err2 == nil && (absTarget == absBase || strings.HasPrefix(absTarget, absBase+string(filepath.Separator))) {
			if info, err := os.Stat(absTarget); err == nil {
				// If it's a regular file, serve it directly
				if !info.IsDir() {
					http.ServeFile(w, r, absTarget)
					return
				}

				// If it's a directory, check if it contains an index.html file
				indexTarget := filepath.Join(absTarget, "index.html")
				if idxInfo, idxErr := os.Stat(indexTarget); idxErr == nil && !idxInfo.IsDir() {
					// Enforce trailing slash on directory indices
					if !strings.HasSuffix(r.URL.Path, "/") {
						http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
						return
					}
					http.ServeFile(w, r, indexTarget)
					return
				}
			}
		}

		// Fallback to Lua routes if no static file matched
		mogoHandler(w, r)
	}))

	finalPort := appSettings.Port
	if cliPort != 0 {
		finalPort = cliPort
	}
	if finalPort <= 0 {
		finalPort = 8080
	}

	addr := fmt.Sprintf(":%d", finalPort)
	log.Printf("Starting Mogo Admin API on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
