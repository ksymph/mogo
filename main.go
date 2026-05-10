package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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
