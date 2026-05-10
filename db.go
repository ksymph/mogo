package main

import (
	crand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	lua "github.com/yuin/gopher-lua"
	_ "modernc.org/sqlite"
)

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
	env.ConfigDBConn.Exec("ALTER TABLE _collections ADD COLUMN middleware_script TEXT DEFAULT ''")
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

func (env *Environment) expandRecord(colName string, row map[string]any, cache map[string]any, activePath map[string]bool, colSet map[string]bool, forLua bool, L *lua.LState) any {
	idStr := fmt.Sprintf("%v", row["id"])
	cacheKey := colName + ":" + idStr

	if existing, ok := cache[cacheKey]; ok {
		if activePath[cacheKey] && !forLua {
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
		if sf.Type == "json" {
			if forLua {
				val := row[sf.Name]
				if val == nil || val == "" {
					continue
				}
				vStr := strings.TrimSpace(fmt.Sprintf("%v", val))
				if vStr != "" && vStr != "nil" {
					var parsed any
					if err := json.Unmarshal([]byte(vStr), &parsed); err == nil {
						out[sf.Name] = parsed
					}
				}
			}
		} else if colSet[sf.Type] {
			val := row[sf.Name]
			if val == nil {
				continue
			}

			var relId int64
			switch v := val.(type) {
			case int64:
				relId = v
			case float64:
				relId = int64(v)
			case string:
				parsed, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					continue
				}
				relId = parsed
			default:
				continue
			}

			if relId <= 0 {
				continue
			}

			relIdStr := strconv.FormatInt(relId, 10)
			relCacheKey := sf.Type + ":" + relIdStr

			var expandedItem any
			if existing, ok := cache[relCacheKey]; ok {
				if activePath[relCacheKey] && !forLua {
					expandedItem = map[string]any{"id": relId}
				} else {
					expandedItem = existing
				}
			} else {
				relRows, err := queryDB(env.DataDBConn, fmt.Sprintf("SELECT * FROM %s WHERE id = ?", sf.Type), relId)
				if err == nil && len(relRows) > 0 {
					relExpandedAny := env.expandRecord(sf.Type, relRows[0], cache, activePath, colSet, forLua, L)
					expandedItem = relExpandedAny

					if forLua && L != nil {
						relHooks := env.loadMiddlewareHooks(L, sf.Type)
						if relHooks != nil && relHooks.RawGetString("item").Type() == lua.LTFunction {
							visited := make(map[string]lua.LValue)
							itemLua := toLValueCyclic(L, relExpandedAny, visited)
							rets, err := env.callMiddlewareHook(L, relHooks, "item", itemLua)

							if err != nil {
								env.logMiddlewareError(L, sf.Type, err)
								expandedItem = nil
							} else if len(rets) > 0 {
								if rets[0] == lua.LNil {
									errMsg := "unknown error"
									if len(rets) > 1 && rets[1].Type() == lua.LTString {
										errMsg = rets[1].String()
									}
									env.logMiddlewareError(L, sf.Type, errMsg)
									expandedItem = nil
								} else {
									expandedItem = rets[0]
									cache[relCacheKey] = expandedItem
								}
							}
						}
					}
				} else {
					expandedItem = relId
				}
			}

			if expandedItem != nil {
				out[sf.Name] = expandedItem
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
	if lv, ok := val.(lua.LValue); ok {
		return lv
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

func (env *Environment) createCollectionInDB(name string, schema []SchemaField, middlewareScript string) error {
	var exists int
	env.ConfigDBConn.QueryRow("SELECT COUNT(*) FROM _collections WHERE name = ?", name).Scan(&exists)
	if exists > 0 {
		return fmt.Errorf("collection already exists")
	}

	res, err := env.ConfigDBConn.Exec("INSERT INTO _collections (name, created, updated, middleware_script) VALUES (?, ?, ?, ?)",
		name, time.Now().Unix(), time.Now().Unix(), middlewareScript)
	if err != nil {
		return err
	}
	collectionID, _ := res.LastInsertId()

	colSet := env.getCollectionSet()

	sqlFields := []string{"id INTEGER PRIMARY KEY AUTOINCREMENT", "created INTEGER", "updated INTEGER"}
	for i, field := range schema {
		sqlType := "TEXT"
		switch field.Type {
		case "number":
			sqlType = "REAL"
		case "bool":
			sqlType = "BOOLEAN"
		default:
			if _, ok := colSet[field.Type]; ok {
				sqlType = "INTEGER"
			}
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

	colSet := env.getCollectionSet()
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
			default:
				if _, ok := colSet[field.Type]; ok {
					sqlType = "INTEGER"
				}
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

func (env *Environment) loadMiddlewareHooks(L *lua.LState, collectionName string) *lua.LTable {
	var scriptFile string
	err := env.ConfigDBConn.QueryRow("SELECT middleware_script FROM _collections WHERE name = ?", collectionName).Scan(&scriptFile)
	if err != nil || scriptFile == "" {
		return nil
	}
	scriptPath := filepath.Join(env.MiddlewarePath, scriptFile)
	info, statErr := os.Stat(scriptPath)
	if statErr != nil {
		return nil
	}
	modTime := info.ModTime()

	if entry, ok := env.middlewareCache.Load(scriptPath); ok {
		cached := entry.(*middlewareCacheEntry)
		if cached.ModTime.Equal(modTime) {
			if cached.Err != nil {
				return nil
			}
			if cached.Proto != nil {
				fn := L.NewFunctionFromProto(cached.Proto)
				L.Push(fn)
				if err := L.PCall(0, 1, nil); err != nil {
					return nil
				}
				ret := L.Get(-1)
				L.Pop(1)
				if ret.Type() == lua.LTTable {
					return ret.(*lua.LTable)
				}
				return nil
			}
		}
	}

	fn, err := L.LoadFile(scriptPath)
	if err != nil {
		env.middlewareCache.Store(scriptPath, &middlewareCacheEntry{Err: err, ModTime: modTime})
		return nil
	}
	proto := fn.Proto
	env.middlewareCache.Store(scriptPath, &middlewareCacheEntry{Proto: proto, ModTime: modTime})

	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		return nil
	}
	ret := L.Get(-1)
	L.Pop(1)
	if ret.Type() != lua.LTTable {
		return nil
	}
	return ret.(*lua.LTable)
}

func (env *Environment) callMiddlewareHook(L *lua.LState, hooks *lua.LTable, hookName string, args ...lua.LValue) ([]lua.LValue, error) {
	hookFn := hooks.RawGetString(hookName)
	if hookFn.Type() != lua.LTFunction {
		return nil, nil
	}

	topBefore := L.GetTop()

	L.Push(hookFn)
	for _, a := range args {
		L.Push(a)
	}
	err := L.PCall(len(args), lua.MultRet, nil)
	if err != nil {
		return nil, fmt.Errorf("[middleware - %s] %s", hookName, err.Error())
	}

	topAfter := L.GetTop()
	nret := topAfter - topBefore

	var rets []lua.LValue
	for i := 1; i <= nret; i++ {
		rets = append(rets, L.Get(topBefore+i))
	}
	L.Pop(nret)

	return rets, nil
}

func (env *Environment) logMiddlewareError(L *lua.LState, col string, err any) {
	msg := fmt.Sprintf("[%s] item hook error: %v", col, err)
	if L != nil {
		ctx := L.Context()
		if ctx != nil {
			if rl, ok := ctx.Value(ctxKeyLogger{}).(*RequestLogger); ok {
				rl.Log("error", getScript(L), "middleware", msg)
				return
			}
		}
		env.appLog("error", getScript(L), "middleware", msg)
	} else {
		env.appLog("error", "system", "middleware", msg)
	}
}
