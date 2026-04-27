# Mogo

Mogo is a minimal backend-in-a-box written in Go. It comes wtih a built-in SQLite database, a web-based admin panel, and Lua scripting for dynamic API routing and scheduled jobs. It was born from a desire for writing logic and custom routing with [PocketBase](https://github.com/pocketbase/pocketbase) to be smoother; with Mogo, you can write and manage an entire application from an admin panel with a simple Lua API, no recompiling, SSH-ing or interfacing with Go needed.

It pairs naturally with [htmx](https://htmx.org/) and the HATEOAS approach, though it can be used to make traditional JSON APIs too. Mogo is best suited for prototypes and small-scale applications: blogs with comments, leaderboards, internal tools, or any CRUD app where you want to tinker without any build steps or other infrastructure overhead.

## Quick Start
```bash
go mod download
go run main.go
```
or build it:
```bash
go build -o mogo .
```
On first run, Mogo will generate a master API key with all permissions and print it to console. Go to http://localhost:8080/admin and copy-paste the key in, or use the generated auto login link.

Note: included in the `util/` folder of this repo are some assorted utility scripts.

### CLI Flags

*   `--port=8080`: Set the HTTP port (overrides port set in settings; default 8080)
*   `--staging-port=8090`: Set port for staging environment (overrides port in settings; default 8090)
*   `--dir=/path/to/dir`: Set a custom root directory for Mogo's folders
*   `--new-key`: Generate and display a new API key with all permissions, then exit

### Directory Structure
Mogo automatically creates the following directories in your project root:
*   `data/`: Holds your SQLite databases (`database.sqlite`, `log.sqlite`) and backups
*   `routes/`: Lua scripts mapped to HTTP endpoints
*   `public/`: Files placed here are served statically at the site root (note: Mogo serves static files before dynamic routes)
*   `scripts/`: Lua scripts intended to be run as scheduled jobs
*   `uploads/`: Default destination folder for client file uploads
*   `middleware/`: Lua scripts that hook into and intercept database operations

## Lua API Reference

### 1. Routing
Scripts inside the `routes/` directory automatically map to URLs.
*   `routes/api/hello.lua` -> `GET /api/hello`
*   `routes/users/[id].lua` -> `GET /users/123` (Provides `id = 123` in parameters)

A route script must return a table of HTTP methods:
```lua
return {
  GET = function(req)
    return "Hello World!"
  end,
  POST = function(req)
    return { received = req.body }
  end
}
```
*(Use the `ANY` key to match all HTTP methods not explicitly defined).*

### 2. The `req` Object
The `req` table contains all information about the incoming HTTP request.

*   **`req.method`** *(string)*: e.g., `"GET"`, `"POST"`
*   **`req.path`** *(string)*: e.g., `"/api/hello"`
*   **`req.query`** *(table)*: URL query parameters (`?name=John` -> `req.query.name`)
*   **`req.params`** *(table)*: Dynamic route parameters (`[id].lua` -> `req.params.id`)
*   **`req.headers`** *(table)*: Request headers, e.g. `Content-Type == "text/html"`
*   **`req.cookies`** *(table)*: Request cookies
*   **`req.body`** *(string/table)*: The parsed JSON body, form data, or raw string
*   **`req.ip`** *(string)*: Request origin IP
*   **`req.files`** *(table)*: Uploaded multipart files
    *   `req.files.myFile.filename`: Name of sent file
    *   `req.files.myFile.size`: File size in bytes
    *   `req.files.myFile.save(destPath)`: Save the file to disk, returns `true` on success *(note: uploads must be in `uploads/` folder unless Unsafe Lua is enabled).*

### 3. Sending responses
The simplest way of sending a response is by returning a value; if a string, it sends as HTML, and if a table, it sends as JSON, with all the necessary headers. For more advanced usage, you can manipulate the response object `res` directly (though note that it doesn't need to be returned). You can combine the two approaches, e.g. `res.status = 404` then `return "404: file not found` would send as HTML with status code 404 (`res` manipulations override any returned data).

You can also return a callback to be executed after the response is sent. It should be returned as the last argument (i.e. second if returning text/table, first and only if manipulating `res` directly).

**Response object:**
*   **`res.status`** *(number)*: HTTP status code (default `200`)
*   **`res.headers`** *(table)*: Response headers
*   **`res.cookies`** *(table)*: Set cookies
    *   Simple: `res.cookies.session = "123"`
    *   Advanced: `res.cookies.session = { value="123", http_only=true, secure=true, max_age=3600 }`
    *   Delete: `res.cookies.session = { delete=true }`
*   **`res:file(path, [filename])`**: Send a file to be downloaded by the client

**Examples:**
```lua
return "<h1>Hello, world!</h1>"
-- 200 OK, text/html

return { user = "Alice", age = 25 }, function() log.info "added user" end
-- 200 OK, application/json

res.status = 403
res.body = "403 Forbidden"
return function() log.info("Tried to access " .. req.path) end
```

### 4. Database API (`db`)
Mogo has a simple CRUD API for collections. Relations are automatically resolved into nested Lua tables when fetched. Also, for advanced SQL manipulation, raw SQL statements can be executed by calling `db` as a function.

*   **`db.<collection>:get([query], [limit], [sort_by])`**: Returns array of items matching the given query table, or all if no/empty table is provided. To reverse sort order, use a negative limit; pass in 0 to get all results; negative zero does work. Note that even if only one is returned, it's in an array. Sorts by ID and ascending by default.
    *  `db.users:get{ firstName = "John", lastName = "Doe" }`
    *  `db.users:get({ active = true }, 0, "created" )`
    *  `local user = db.users:get({ last_name = "Doe" }, 1 )[1]`
*   **`db.<collection>:insert(data)`**: Inserts a row and returns the new record; `id`, `created`, and `updated` are auto-generated; returns new item id
    *   `uid = db.users:insert({ firstName = "Alice", age = 30 })`
*   **`db.<collection>:update(query, { foo = "bar", baz = nil })`**: Updates items matching the query. Because updates are processed per-item (to support middleware hooks), this returns an array of result objects indicating the success/failure of each affected record: `{{id = 1, result = true}, {id = 2, result = false, err = "..."}}`.
    *   `results = db.users:update({ role = "user" }, { active = false })`
*   **`db.<collection>:delete(query)`**: Deletes matching records. Like updates, this processes per-item and returns an array of result objects.
*   **Raw SQL**: Call the `db` object as a function for direct SQL queries. Returns a table of results for `SELECT`/`PRAGMA`, and `true, rowsAffected` for mutations. Note that system SQL tables are: `_api_keys`, `_collections`, `_cron_jobs`, `_schema`. **Raw SQL queries bypass collection middleware.**
    *   `local users = db("SELECT * FROM users WHERE age > ?", 18)`
*   **Relations**: Relational fields are automatically expanded as nested tables
    *   `local user = db.messages({}, 1, "created")[1].user -- user.id == 1`

### 5. Collection Middleware
You can attach Lua scripts to collections to intercept, validate, or modify database operations. These scripts live in the `middleware/` directory and are assigned to collections via the Admin Dashboard's Collection Manager.

A middleware script should return a table containing any of the following lifecycle hooks:

```lua
return {
  -- Runs before a record is inserted.
  -- You must return the data table to proceed, or `nil, "Error message"` to abort.
  pre_insert = function(data)
    if not data.email or data.email == "" then
      return nil, "Email is required"
    end
    data.status = "pending"
    return data
  end,

  -- Runs after successful insertion.
  -- Return a custom value to override what `db:insert()` returns to the caller,
  -- or return nothing/nil to default to returning the new record's ID.
  post_insert = function(item)
    log.info("New user registered: " .. item.email)
  end,

  -- Runs before an existing record is updated.
  -- Returns `true, modified_data` to proceed, or `nil, "Error"` to abort.
  pre_update = function(item, data)
    if data.role == "admin" and item.role ~= "admin" then
      return nil, "Cannot elevate role to admin"
    end
    data.last_modified_by = "system"
    return true, data 
  end,

  -- Runs after an update.
  post_update = function(item, applied_data)
    -- ...
  end,

  -- Runs before an item is deleted. Return `true` to allow, `nil, "Error"` to prevent.
  pre_delete = function(item)
    if item.role == "admin" then return nil, "Cannot delete admins" end
    return true
  end,

  -- Runs after a successful deletion.
  post_delete = function(item)
    log.info("Deleted user: " .. item.id)
  end,

  -- Formatter hook. Runs on every item retrieved via `db:get()`
  -- as well as when this collection is expanded as a nested relation.
  item = function(record)
    record.password_hash = nil -- hide sensitive fields from the rest of the application
    record.full_name = record.first_name .. " " .. record.last_name
    return record
  end
}
```
**Important Middleware Notes:**
* Because `update` and `delete` hooks require inspecting the existing data, the Lua `db:update()` and `db:delete()` methods process records sequentially (one by one), and **do not** run inside a bulk SQL transaction. 
* Middleware hooks are **bypassed** if you execute raw SQL strings via `db("SELECT/UPDATE/DELETE...")`. 

### 6. HTTP Client (`http`)
Make outbound HTTP requests from your scripts.

*   **`http(method, url, [opts])`**

**opts:** A table accepting `{ headers = { ["Authorization"] = "Bearer token" }, body = "data" }`
**Returns:** A table containing `{ status, body, headers }`

```lua
local res = http("GET", "https://api.github.com/users/octocat")
if res.status == 200 then
    return res.body
end
```

### 7. Logging (`log`)
Logs are saved to `log.sqlite` and are viewable in the Admin UI. Logs are automatically deleted after the days set in settings (default 30).

*   **`log.info(msg)`**
*   **`log.warn(msg)`**
*   **`log.error(msg)`**
*   **`log:get([limit])`**: Returns array of last `limit` logs, or last log if no limit given. Logs have fields `{id, timestamp, level, origin, script, message}`

### 8. Standard Libraries & Unsafe Lua
By default, Mogo restricts the Lua environment for safety. Standard libraries `table`, `string`, `math`, `coroutine` are available, but `os` is restricted (dangerous functions like `os.execute` and `os.remove` are stripped), and `io` is unavailable. 

If you need complete access to the host system from your scripts, you can enable **"Allow Unsafe Lua"** in the Admin Settings. This exposes the complete `io`, and `debug` libraries and removes directory jailing for file operations (`res:file()` and `req.files.save()`).
