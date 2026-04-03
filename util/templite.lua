-- usage: tpl(template, environment, sandbox)
--   template: string containing:
--       <% expression %>    - a value to be inserted html-escaped, e.g. <% "<h1>hello world</h1>" %> -> &lt;h1&gt;Hello world&lt;/h1&gt;, or <% foo %> -> bar (passed in from environment)
--       <%= expression %>   - non-html-escaped, e.g. <%= "<h1>hello world</h1> %> -> <h1>hello world</h1>
--       <? lua code ?>      - executes raw Lua code (no automatic write; use write() (escaped) or rwrite (unescaped) to output)
--   environment: table of variables available inside the template, e.g. { foo = "bar" }
--   sandbox: if true, disables access to global _G
-- returns the rendered string

return function(str, environment, sandbox)
	local env = setmetatable(
		environment or {}, 
		sandbox and nil or { __index = _G }
	)
	local code = [[
		local result = ''
		local function rwrite(s) result = result .. tostring(s or '') end
		local function write(s)
			result = result .. tostring(s or ''):gsub("[\">/<'&]", {
				["&"] = "&amp;",
				["<"] = "&lt;",
				[">"] = "&gt;",
				['"'] = "&quot;",
				["'"] = "&#39;",
				["/"] = "&#47;"
			})
		end
		rwrite[=[
	]]
	code = code .. str:
		gsub("[][]=[][]", ']=]rwrite"%1"rwrite[=['):
		gsub("<%%=", "]=]rwrite("):
		gsub("<%%", "]=]write("):
		gsub("%%>", ")rwrite[=["):
		gsub("<%?", "]=] "):
		gsub("%?>", " rwrite[=[")
	code = code .. "]=] return result"

	local func = loadstring(code, "template", "t", env)
	return func()
end
