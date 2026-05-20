# Mogo — Agent Guide

## Project

Minimal backend-in-a-box: Go + SQLite + Lua scripting. Single module, no monorepo.

## Build & Run

```
go build -o mogo .          # build binary
go run main.go              # run directly
```

Flags: `--port=8080`, `--staging-port=8090`, `--dir=`, `--new-key`.

No lint/typecheck config or CI — none needed. No tests exist.

## Architecture

- **Single file tree**: `main.go` → CLI + HTTP server startup. `config.go` → Environment + Settings. `router.go` → Lua route matching. `lua.go` → Lua state management, DB API. `db.go` → SQLite schema, middleware hooks. `security.go` → auth, rate limiting. `jobs.go` → cron, backup. `api.go` → admin REST handlers.
- **Two environments**: `ProdEnv` (port 8080, persistent) and optional `StagingEnv` (port 8090, syncable from admin).
- **Three SQLite DBs per env** (in `data/`): `config.sqlite`, `data.sqlite`, `log.sqlite`. All pure Go (`modernc.org/sqlite` — no CGO needed).
- **Settings**: `data/settings.json`, includes API keys, ports, paths, flags.
- **Lua**: `github.com/yuin/gopher-lua`. Safe by default (`os.*` restricted, `io`/`debug` removed). Enable "Unsafe Lua" in admin to unlock them.
- **Routes**: `.lua` files in `routes/`. Auto-mapped to URL paths. Hot-reloaded via fsnotify. Route script must return a table of method handlers (`GET`, `POST`, `ANY`, etc.).
- **Dynamic URL params**: `routes/users/[id].lua` → `req.params.id = "123"`.
- **Admin panel**: `admin.html`, `style.css`, `prism-code.js` embedded via `//go:embed`.
- **First run** auto-generates master API key (prefix `sk_`), prints to console + auto-login link.

## Key Conventions

- `db:get({field=val}, limit, sort_by)` — negative limit reverses sort; 0 = all results.
- `db:update()` and `db:delete()` process **per-item** (not bulk) to support middleware — returns array of result objects.
- Raw SQL (`db("SELECT ...")`) **bypasses** middleware.
- Middleware lives in `middleware/`, assigned per-collection via admin.
- Backup types: `complete`, `content`, `template`.
- Route `res.cookies` supports both simple string values and advanced tables with `value`, `http_only`, `secure`, `max_age`, `same_site`, `delete`.
- Static files in `public/` are checked before dynamic routes.

## Important Gotchas

- Lua state pool (`sync.Pool`) is recreated on settings save — any in-flight Lua states from the old pool are unaffected.
- Route scripts run in a restricted environment: `os.execute`, `os.exit`, `os.remove`, `os.rename`, `os.setenv`, `os.getenv`, `os.tmpname` are nil'd unless Unsafe Lua is on.
- Route handler timeout is 15s; post-hook callback timeout is 5min.
- Cron scripts timeout after 10min.
- `res:file()` is jail'd to `public/` unless Unsafe Lua is enabled.
- `req.files[key]:save(path)` is jail'd to `uploads/` unless Unsafe Lua is enabled.
- `_api_keys` collection access from admin API is explicitly denied.
- Admin login uses cookie `mogo_token` or `Authorization: Bearer <key>` header.
- Rate limiting is token-bucket per IP (default 100 RPS / 200 burst).
- Admin IP lockout after `admin_max_retries` failed attempts (default 5).

## Util Scripts

`util/` contains helper Lua modules: `escape.lua`, `inspect.lua`, `sha256.lua`, `templite.lua`. Loadable by scripts.
