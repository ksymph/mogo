package main

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	lua "github.com/yuin/gopher-lua"
)

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
					env.appLog("error", "system", "cron", "[system] Failed to prune logs: "+err.Error())
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
					time.Duration(parseIntField(meta["s"]))*time.Second +
					time.Duration(parseIntField(meta["ms"]))*time.Millisecond
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
				time.Duration(parseIntField(meta["s"]))*time.Second +
				time.Duration(parseIntField(meta["ms"]))*time.Millisecond
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

	rl := &RequestLogger{Limit: appSettings.MaxLogCountPerReq, IP: "cron", Env: env}
	ctx := context.WithValue(context.Background(), ctxKeyLogger{}, rl)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	L.SetContext(ctx)
	defer L.RemoveContext()

	fn, err := L.LoadFile(scriptPath)
	if err != nil {
		rl.Log("error", scriptPath, "execution", err.Error())
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
		rl.Log("error", scriptPath, "execution", err.Error())
		log.Printf("Cron error running %s: %v", scriptPath, err)
		return err
	}

	return nil
}

func zipSource(w *zip.Writer, source, prefix string) error {
	info, err := os.Stat(source)
	if err != nil {
		return nil
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
		backupType = "complete"
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
		pathsToBackup[filepath.Join(rootDir, "middleware")] = "middleware"
		pathsToBackup[filepath.Join(rootDir, ".gitignore")] = ".gitignore"
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
		pathsToBackup[filepath.Join(rootDir, "middleware")] = "middleware"
		pathsToBackup[filepath.Join(rootDir, ".gitignore")] = ".gitignore"
	}

	for source, prefix := range pathsToBackup {
		if err := zipSource(w, source, prefix); err != nil {
			log.Printf("Error adding %s to backup: %v", source, err)
		}
	}

	w.Close()
	outFile.Close()

	if appSettings.BackupRetention > 0 {
		files, err := os.ReadDir(destDir)
		if err != nil {
			return nil
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
