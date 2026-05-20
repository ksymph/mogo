package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
)

func (env *Environment) injectDB(L *lua.LState) {
	dbTbl := L.NewTable()
	dbMeta := L.NewTable()
	L.SetField(dbMeta, "__index", L.NewFunction(func(L *lua.LState) int {
		colName := L.CheckString(2)
		colTbl := L.NewTable()
		colTbl.RawSetString("_name", lua.LString(colName))

		colTbl.RawSetString("get", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()

			hooks := env.loadMiddlewareHooks(L, cName)

			whereVals := []any{}
			whereCols := []string{"1=1"}

			if L.GetTop() >= 2 && L.Get(2).Type() == lua.LTTable {
				queryTbl := L.CheckTable(2)
				if queryMap, ok := luaValueToInterface(queryTbl).(map[string]any); ok {
					for k, v := range queryMap {
						whereCols = append(whereCols, fmt.Sprintf("`%s` = ?", k))
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
			if strings.Contains(strings.ToUpper(sortCol), " ASC") || strings.Contains(strings.ToUpper(sortCol), " DESC") {
				idx := strings.LastIndex(sortCol, " ")
				if idx > 0 {
					sortCol = "`" + sortCol[:idx] + "`" + sortCol[idx:]
				}
			} else {
				sortCol = "`" + sortCol + "` ASC"
			}
			orderClause = "ORDER BY " + sortCol
		}

			if reverseSort {
				parts := strings.Split(orderClause[9:], ",")
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

			q := fmt.Sprintf("SELECT * FROM `%s` WHERE %s %s %s", cName, strings.Join(whereCols, " AND "), orderClause, limitClause)

			colSet := env.getCollectionSet()
			results, err := queryDB(env.DataDBConn, q, whereVals...)
			if err != nil {
				env.logLuaError(L, err.Error())
				L.Push(L.NewTable())
				return 1
			}

			cache := make(map[string]any)
			visitedLua := make(map[string]lua.LValue)
			arr := L.NewTable()
			errorsTbl := L.NewTable()
			hasErrors := false
			itemCount := 0

			for _, r := range results {
				activePath := make(map[string]bool)
				expanded := env.expandRecord(cName, r, cache, activePath, colSet, true, L)

				if lv, ok := expanded.(lua.LValue); ok {
					itemCount++
					arr.RawSetInt(itemCount, lv)
					continue
				}

				expandedMap := expanded.(map[string]any)

				if hooks != nil && hooks.RawGetString("item").Type() == lua.LTFunction {
					visited := make(map[string]lua.LValue)
					itemLua := toLValueCyclic(L, expandedMap, visited)
					rets, err := env.callMiddlewareHook(L, hooks, "item", itemLua)

					if err != nil {
						env.logMiddlewareError(L, cName, err)

						errObj := L.NewTable()
						errObj.RawSetString("id", toLValue(L, r["id"]))
						errObj.RawSetString("err", lua.LString(err.Error()))
						errorsTbl.Append(errObj)
						hasErrors = true
						continue
					} else if len(rets) > 0 {
						if rets[0] == lua.LNil {
							errMsg := "unknown error"
							if len(rets) > 1 && rets[1].Type() == lua.LTString {
								errMsg = rets[1].String()
							}
							env.logMiddlewareError(L, cName, errMsg)

							errObj := L.NewTable()
							errObj.RawSetString("id", toLValue(L, r["id"]))
							errObj.RawSetString("err", lua.LString(errMsg))
							errorsTbl.Append(errObj)
							hasErrors = true
							continue
						} else {
							cache[cName+":"+fmt.Sprintf("%v", r["id"])] = rets[0]
							itemCount++
							arr.RawSetInt(itemCount, rets[0])
						}
					} else {
						lv := toLValueCyclic(L, expandedMap, visitedLua)
						cache[cName+":"+fmt.Sprintf("%v", r["id"])] = lv
						itemCount++
						arr.RawSetInt(itemCount, lv)
					}
				} else {
					lv := toLValueCyclic(L, expandedMap, visitedLua)
					cache[cName+":"+fmt.Sprintf("%v", r["id"])] = lv
					itemCount++
					arr.RawSetInt(itemCount, lv)
				}
			}

			if hasErrors {
				if itemCount == 0 {
					L.Push(lua.LNil)
				} else {
					L.Push(arr)
				}
				L.Push(errorsTbl)
				return 2
			}

			L.Push(arr)
			return 1
		}))

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

			hooks := env.loadMiddlewareHooks(L, cName)

			if hooks != nil {
				if hooks.RawGetString("pre_insert").Type() == lua.LTFunction {
					dataLua := toLValue(L, data)
					rets, err := env.callMiddlewareHook(L, hooks, "pre_insert", dataLua)
					if err != nil {
						L.Push(lua.LNil)
						L.Push(lua.LString(err.Error()))
						return 2
					}
					if len(rets) >= 1 && rets[0] != lua.LNil {
						newData, ok := luaValueToInterface(rets[0]).(map[string]any)
						if !ok {
							L.Push(lua.LNil)
							L.Push(lua.LString("[middleware - pre_insert] returned invalid data"))
							return 2
						}
						data = newData
					} else {
						errMsg := "[middleware - pre_insert] aborted"
						if len(rets) >= 2 && rets[1].Type() == lua.LTString {
							errMsg = "[middleware - pre_insert] " + rets[1].String()
						}
						L.Push(lua.LNil)
						L.Push(lua.LString(errMsg))
						return 2
					}
				}
			}

			cols, placeholders, args := []string{}, []string{}, []any{}
			for k, v := range data {
				cols = append(cols, "`"+k+"`")
				placeholders = append(placeholders, "?")
				switch val := v.(type) {
				case map[string]any, []any:
					args = append(args, formatLuaTable(val))
				default:
					args = append(args, v)
				}
			}
			q := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", cName, strings.Join(cols, ","), strings.Join(placeholders, ","))
			res, err := env.DataDBConn.Exec(q, args...)
			if err != nil {
				env.logLuaError(L, err.Error())
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			id, _ := res.LastInsertId()

			if hooks != nil && hooks.RawGetString("post_insert").Type() == lua.LTFunction {
				items, _ := queryDB(env.DataDBConn, "SELECT * FROM `"+cName+"` WHERE id = ?", id)
				if len(items) == 0 {
					L.Push(lua.LNil)
					L.Push(lua.LString("[middleware - post_insert] could not retrieve item"))
					return 2
				}
				itemLua := toLValue(L, items[0])
				rets, err := env.callMiddlewareHook(L, hooks, "post_insert", itemLua)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				if len(rets) == 0 {
					L.Push(lua.LNumber(id))
				} else {
					L.Push(rets[0])
				}
				return 1
			}

			L.Push(lua.LNumber(id))
			return 1
		}))

		colTbl.RawSetString("update", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			queryMap, _ := luaValueToInterface(L.OptTable(2, L.NewTable())).(map[string]any)
			updateMap, _ := luaValueToInterface(L.OptTable(3, L.NewTable())).(map[string]any)

			updateMap["updated"] = time.Now().Unix()

			hooks := env.loadMiddlewareHooks(L, cName)

			whereVals, whereCols := []any{}, []string{"1=1"}
			for k, v := range queryMap {
				whereCols = append(whereCols, fmt.Sprintf("`%s` = ?", k))
				whereVals = append(whereVals, v)
			}
			selectQ := fmt.Sprintf("SELECT id FROM `%s` WHERE %s", cName, strings.Join(whereCols, " AND "))
			rows, err := env.DataDBConn.Query(selectQ, whereVals...)
			if err != nil {
				env.logLuaError(L, err.Error())
				L.Push(lua.LBool(false))
				L.Push(lua.LString(err.Error()))
				return 2
			}
			var ids []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err == nil {
					ids = append(ids, id)
				}
			}
			rows.Close()

			results := L.NewTable()
			for _, id := range ids {
				resultObj := L.NewTable()
				resultObj.RawSetString("id", lua.LNumber(id))

				origItems, err := queryDB(env.DataDBConn, "SELECT * FROM `"+cName+"` WHERE id = ?", id)
				if err != nil || len(origItems) == 0 {
					resultObj.RawSetString("result", lua.LBool(false))
					resultObj.RawSetString("err", lua.LString("item not found"))
					results.Append(resultObj)
					continue
				}
				origItem := origItems[0]

				mergedData := updateMap
				if hooks != nil && hooks.RawGetString("pre_update").Type() == lua.LTFunction {
					origLua := toLValue(L, origItem)
					dataLua := toLValue(L, updateMap)
					rets, err := env.callMiddlewareHook(L, hooks, "pre_update", origLua, dataLua)
					if err != nil {
						resultObj.RawSetString("result", lua.LBool(false))
						resultObj.RawSetString("err", lua.LString(err.Error()))
						results.Append(resultObj)
						continue
					}
					if len(rets) >= 1 && rets[0] != lua.LNil {
						if len(rets) >= 2 && rets[1].Type() == lua.LTTable {
							newData, _ := luaValueToInterface(rets[1]).(map[string]any)
							mergedData = newData
						} else if len(rets) >= 2 && rets[1] == lua.LNil {
							errMsg := "[middleware - pre_update] aborted"
							if len(rets) >= 3 && rets[2].Type() == lua.LTString {
								errMsg = "[middleware - pre_update] " + rets[2].String()
							}
							resultObj.RawSetString("result", lua.LBool(false))
							resultObj.RawSetString("err", lua.LString(errMsg))
							results.Append(resultObj)
							continue
						}
					} else {
						errMsg := "[middleware - pre_update] aborted"
						if len(rets) >= 2 && rets[1].Type() == lua.LTString {
							errMsg = "[middleware - pre_update] " + rets[1].String()
						}
						resultObj.RawSetString("result", lua.LBool(false))
						resultObj.RawSetString("err", lua.LString(errMsg))
						results.Append(resultObj)
						continue
					}
				}

				setCols, setVals := []string{}, []any{}
				for k, v := range mergedData {
					if k == "id" || k == "created" {
						continue
					}
					setCols = append(setCols, fmt.Sprintf("`%s` = ?", k))
					switch val := v.(type) {
					case map[string]any, []any:
						setVals = append(setVals, formatLuaTable(val))
					default:
						setVals = append(setVals, v)
					}
				}
				setVals = append(setVals, id)
				updateQ := fmt.Sprintf("UPDATE `%s` SET %s WHERE id = ?", cName, strings.Join(setCols, ", "))
				_, err = env.DataDBConn.Exec(updateQ, setVals...)
				if err != nil {
					env.logLuaError(L, err.Error())
					resultObj.RawSetString("result", lua.LBool(false))
					resultObj.RawSetString("err", lua.LString(err.Error()))
					results.Append(resultObj)
					continue
				}

				updatedItems, _ := queryDB(env.DataDBConn, "SELECT * FROM `"+cName+"` WHERE id = ?", id)
				if len(updatedItems) == 0 {
					resultObj.RawSetString("result", lua.LBool(false))
					resultObj.RawSetString("err", lua.LString("item gone after update"))
					results.Append(resultObj)
					continue
				}

				if hooks != nil && hooks.RawGetString("post_update").Type() == lua.LTFunction {
					updatedLua := toLValue(L, updatedItems[0])
					dataLua := toLValue(L, mergedData)
					rets, err := env.callMiddlewareHook(L, hooks, "post_update", updatedLua, dataLua)
					if err != nil {
						resultObj.RawSetString("result", lua.LBool(false))
						resultObj.RawSetString("err", lua.LString(err.Error()))
						results.Append(resultObj)
						continue
					}
					if len(rets) == 0 {
						resultObj.RawSetString("result", lua.LBool(true))
					} else {
						resultObj.RawSetString("result", rets[0])
					}
				} else {
					resultObj.RawSetString("result", lua.LBool(true))
				}
				results.Append(resultObj)
			}
			L.Push(results)
			return 1
		}))

		colTbl.RawSetString("delete", L.NewFunction(func(L *lua.LState) int {
			cName := L.CheckTable(1).RawGetString("_name").String()
			queryMap, _ := luaValueToInterface(L.OptTable(2, L.NewTable())).(map[string]any)

			hooks := env.loadMiddlewareHooks(L, cName)

			whereVals, whereCols := []any{}, []string{"1=1"}
			for k, v := range queryMap {
				whereCols = append(whereCols, fmt.Sprintf("`%s` = ?", k))
				whereVals = append(whereVals, v)
			}
			selectQ := fmt.Sprintf("SELECT id FROM `%s` WHERE %s", cName, strings.Join(whereCols, " AND "))
			rows, err := env.DataDBConn.Query(selectQ, whereVals...)
			if err != nil {
				env.logLuaError(L, err.Error())
				L.Push(lua.LBool(false))
				L.Push(lua.LString(err.Error()))
				return 2
			}
			var ids []int64
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err == nil {
					ids = append(ids, id)
				}
			}
			rows.Close()

			results := L.NewTable()
			for _, id := range ids {
				resultObj := L.NewTable()
				resultObj.RawSetString("id", lua.LNumber(id))

				origItems, err := queryDB(env.DataDBConn, "SELECT * FROM `"+cName+"` WHERE id = ?", id)
				if err != nil || len(origItems) == 0 {
					resultObj.RawSetString("result", lua.LBool(false))
					resultObj.RawSetString("err", lua.LString("item not found"))
					results.Append(resultObj)
					continue
				}
				origItem := origItems[0]

				if hooks != nil && hooks.RawGetString("pre_delete").Type() == lua.LTFunction {
					origLua := toLValue(L, origItem)
					rets, err := env.callMiddlewareHook(L, hooks, "pre_delete", origLua)
					if err != nil {
						resultObj.RawSetString("result", lua.LBool(false))
						resultObj.RawSetString("err", lua.LString(err.Error()))
						results.Append(resultObj)
						continue
					}
					if len(rets) >= 1 && (rets[0] == lua.LNil || (rets[0].Type() == lua.LTBool && rets[0] == lua.LFalse)) {
						errMsg := "[middleware - pre_delete] aborted"
						if len(rets) >= 2 && rets[1].Type() == lua.LTString {
							errMsg = "[middleware - pre_delete] " + rets[1].String()
						}
						resultObj.RawSetString("result", lua.LBool(false))
						resultObj.RawSetString("err", lua.LString(errMsg))
						results.Append(resultObj)
						continue
					}
				}

				_, err = env.DataDBConn.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", cName), id)
				if err != nil {
					env.logLuaError(L, err.Error())
					resultObj.RawSetString("result", lua.LBool(false))
					resultObj.RawSetString("err", lua.LString(err.Error()))
					results.Append(resultObj)
					continue
				}

				if hooks != nil && hooks.RawGetString("post_delete").Type() == lua.LTFunction {
					origLua := toLValue(L, origItem)
					rets, err := env.callMiddlewareHook(L, hooks, "post_delete", origLua)
					if err != nil {
						resultObj.RawSetString("result", lua.LBool(false))
						resultObj.RawSetString("err", lua.LString(err.Error()))
						results.Append(resultObj)
						continue
					}
					if len(rets) == 0 {
						resultObj.RawSetString("result", lua.LBool(true))
					} else {
						resultObj.RawSetString("result", rets[0])
					}
				} else {
					resultObj.RawSetString("result", lua.LBool(true))
				}
				results.Append(resultObj)
			}
			L.Push(results)
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
					env.logLuaError(L, err.Error())
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
					env.logLuaError(L, err.Error())
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

func (env *Environment) logLuaError(L *lua.LState, message string) {
	ctx := L.Context()
	if ctx != nil {
		if rl, ok := ctx.Value(ctxKeyLogger{}).(*RequestLogger); ok {
			rl.Log("error", getScript(L), "lua", message)
			return
		}
	}
	env.appLog("error", getScript(L), "system", "[lua] "+message)
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
	createLogFn := func(level string) *lua.LFunction {
		return L.NewFunction(func(L *lua.LState) int {
			ctx := L.Context()
			if ctx != nil {
				if rl, ok := ctx.Value(ctxKeyLogger{}).(*RequestLogger); ok {
					rl.Log(level, getScript(L), "lua", L.CheckString(1))
					return 0
				}
			}
			env.appLog(level, getScript(L), "system", "[lua] "+L.CheckString(1))
			return 0
		})
	}

	L.SetField(logMod, "info", createLogFn("info"))
	L.SetField(logMod, "warn", createLogFn("warn"))
	L.SetField(logMod, "error", createLogFn("error"))
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
	if val == nil {
		return lua.LNil
	}
	if lv, ok := val.(lua.LValue); ok {
		return lv
	}
	switch v := val.(type) {
	case string:
		return lua.LString(v)
	case float64:
		return lua.LNumber(v)
	case int:
		return lua.LNumber(v)
	case int64:
		return lua.LNumber(v)
	case bool:
		return lua.LBool(v)
	case map[string]interface{}:
		return mapToLTable(L, v)
	case []interface{}:
		return sliceToLTable(L, v)
	default:
		return lua.LString(fmt.Sprintf("%v", val))
	}
}
