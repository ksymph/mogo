package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	lua "github.com/yuin/gopher-lua"
)

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
			if event.Op&fsnotify.Create == fsnotify.Create {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					watcher.Add(event.Name)
				}
			}
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

func (env *Environment) mogoHandler(w http.ResponseWriter, r *http.Request) {
	route, params := env.matchRoute(r.URL.Path)
	if route == nil {
		http.Error(w, "404 Not Found", 404)
		return
	}

	L := env.LuaPool.Get().(*lua.LState)
	top := L.GetTop()

	rl := &RequestLogger{Limit: appSettings.MaxLogCountPerReq, IP: getIP(r), Env: env}
	ctx := context.WithValue(r.Context(), ctxKeyLogger{}, rl)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
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
		rl.Log("error", route.FilePath, "router", err.Error())
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
		rl.Log("error", route.FilePath, "execution", err.Error())
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
							rl.Log("warn", getScript(L), "security", "Blocked attempt to write file outside uploads directory: "+destPath)
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
		rl.Log("error", route.FilePath, "execution", err.Error())
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

				bgCtx := context.WithValue(context.Background(), ctxKeyLogger{}, rl)
				bgCtx, bgCancel := context.WithTimeout(bgCtx, 5*time.Minute)
				defer bgCancel()
				state.SetContext(bgCtx)

				state.Push(hook)
				if err := state.PCall(0, 0, nil); err != nil {
					rl.Log("error", rPath, "post-hook", err.Error())
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
