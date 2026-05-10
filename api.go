package main

import (
	crand "crypto/rand"
	"database/sql"
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
	"time"
)

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
	} else if req.Base == "middleware" {
		baseDir = env.MiddlewarePath
		permKey = "routes"
	}

	if permKey == "uploads" {
		p := getPermissions(r)
		if p == nil || p["collections"] == nil {
			http.Error(w, "Forbidden", 403)
			return
		}
	} else if !hasPermission(r, permKey) {
		http.Error(w, "Forbidden", 403)
		return
	}

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

	if err := os.MkdirAll(filepath.Dir(newFullPath), os.ModePerm); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create destination directories: %v", err), 500)
		return
	}

	if err := os.Rename(oldFullPath, newFullPath); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

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

	ProdEnv.ConfigDBConn.Close()
	ProdEnv.DataDBConn.Close()
	ProdEnv.LogDBConn.Close()
	if StagingEnv != nil {
		StagingEnv.ConfigDBConn.Close()
		StagingEnv.DataDBConn.Close()
		StagingEnv.LogDBConn.Close()
	}

	pathsToClear := map[string][]string{
		"content":  {"data/data.sqlite", "uploads"},
		"template": {"data/config.sqlite", "data/settings.json", "routes", "scripts", "public", "middleware"},
		"complete": {"data/config.sqlite", "data/data.sqlite", "data/settings.json", "routes", "scripts", "public", "uploads", "middleware"},
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

	if err := unzip(zipPath, rootDir); err != nil {
		log.Printf("CRITICAL: Restore failed during unzip: %v", err)
		http.Error(w, "Failed to unzip backup: "+err.Error(), 500)
		return
	}

	loadSettings()
	ProdEnv.initDB()
	ProdEnv.initRoutes()
	ProdEnv.initCron()

	stagingMu.Lock()
	stagingStarted = false
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

		os.RemoveAll(targetEnv.MiddlewarePath)
		copyDir(sourceEnv.MiddlewarePath, targetEnv.MiddlewarePath)
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

		var collections []struct {
			Name             string
			MiddlewareScript string
		}
		rows, _ := sourceEnv.ConfigDBConn.Query("SELECT name, middleware_script FROM _collections ORDER BY id ASC")
		if rows != nil {
			for rows.Next() {
				var c struct {
					Name             string
					MiddlewareScript string
				}
				var ms sql.NullString
				rows.Scan(&c.Name, &ms)
				c.MiddlewareScript = ms.String
				collections = append(collections, c)
			}
			rows.Close()
		}

		for _, c := range collections {
			schema := sourceEnv.getCollectionSchema(c.Name)
			targetEnv.createCollectionInDB(c.Name, schema, c.MiddlewareScript)
		}

		targetDBPath := filepath.Join(targetEnv.DataPath, "data.sqlite")
		conn, err := sourceEnv.DataDBConn.Conn(r.Context())
		if err == nil {
			defer conn.Close()
			_, err = conn.ExecContext(r.Context(), fmt.Sprintf("ATTACH DATABASE '%s' AS target_db", targetDBPath))
			if err == nil {
				for _, c := range collections {
					conn.ExecContext(r.Context(), fmt.Sprintf("INSERT INTO target_db.%s SELECT * FROM main.%s", c.Name, c.Name))
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
				Name             string        `json:"name"`
				Schema           []SchemaField `json:"schema"`
				MiddlewareScript string        `json:"middleware_script"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if !hasCollectionPermission(r, req.Name, "schema") {
				http.Error(w, "Forbidden", 403)
				return
			}
			if err := env.createCollectionInDB(req.Name, req.Schema, req.MiddlewareScript); err != nil {
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
			cache := make(map[string]any)
			var expandedItems []map[string]any

			for _, item := range items {
				activePath := make(map[string]bool)
				expanded := env.expandRecord(collection, item, cache, activePath, colSet, false, nil)
				if m, ok := expanded.(map[string]any); ok {
					expandedItems = append(expandedItems, m)
				}
			}

			if expandedItems == nil {
				expandedItems = []map[string]any{}
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
				Schema           []SchemaField `json:"schema"`
				MiddlewareScript string        `json:"middleware_script"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			if err := env.updateCollectionInDB(collection, req.Schema); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			if req.MiddlewareScript != "" {
				env.ConfigDBConn.Exec("UPDATE _collections SET middleware_script = ? WHERE name = ?", req.MiddlewareScript, collection)
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
	} else if base == "middleware" {
		baseDir = env.MiddlewarePath
		permKey = "routes"
	}

	if permKey == "uploads" {
		p := getPermissions(r)
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

		if strings.HasPrefix(contentType, "multipart/form-data") {
			if err := r.ParseMultipartForm(32 << 20); err != nil {
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
