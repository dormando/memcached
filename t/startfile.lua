-- xTODO: sets of zones behind a prefix.
-- xTODO: zones with local vs other failover.
-- xTODO: failover on get
-- xTODO: all zone sync on set
-- TODO: fallback cache for broken/overloaded zones.

-- local zone could/should be fetched from environment or local file.
-- doing so allows all configuration files to be identical, simplifying consistency checks.
local my_zone = 'z1'

function mcp_config_selectors(oldss)

    -- alias mcp.server for convenience.
    -- important to alias global variables in routes where speed is concerned.
    local srv = mcp.server
    -- local zones = { 'z1', 'z2', 'z3' }

    -- IPs are "127" . "zone" . "pool" . "srv"
    local pfx = 'fooz1'
    local fooz1 = {
        srv(pfx .. 'srv1', '127.1.1.1', 11212, 1),
        srv(pfx .. 'srv2', '127.1.1.2', 11212, 1),
        srv(pfx .. 'srv3', '127.1.1.3', 11212, 1),
    }
    pfx = 'fooz2'
    local fooz2 = {
        srv(pfx .. 'srv1', '127.2.1.1', 11212, 1),
        srv(pfx .. 'srv2', '127.2.1.2', 11212, 1),
        srv(pfx .. 'srv3', '127.2.1.3', 11212, 1),
    }
    pfx = 'fooz3'
    local fooz3 = {
        srv(pfx .. 'srv1', '127.3.1.1', 11212, 1),
        srv(pfx .. 'srv2', '127.3.1.2', 11212, 1),
        srv(pfx .. 'srv3', '127.3.1.3', 11212, 1),
    }

    pfx = 'barz1'
    local barz1 = {
        srv(pfx .. 'srv1', '127.1.2.1', 11212, 1),
        srv(pfx .. 'srv2', '127.1.2.1', 11212, 1),
        srv(pfx .. 'srv3', '127.1.2.1', 11212, 1),
    }
    pfx = 'barz2'
    local barz2 = {
        srv(pfx .. 'srv1', '127.2.2.2', 11212, 1),
        srv(pfx .. 'srv2', '127.2.2.2', 11212, 1),
        srv(pfx .. 'srv3', '127.2.2.2', 11212, 1),
    }
    pfx = 'barz3'
    local barz3 = {
        srv(pfx .. 'srv1', '127.3.2.3', 11212, 1),
        srv(pfx .. 'srv2', '127.3.2.3', 11212, 1),
        srv(pfx .. 'srv3', '127.3.2.3', 11212, 1),
    }

    -- fallback cache for any zone
    pfx = 'fallz1'
    local fallz1 = {
        srv(pfx .. 'srv1', '127.0.2.1', 11212, 1),
    }
    pfx = 'fallz2'
    local fallz2 = {
        srv(pfx .. 'srv1', '127.0.2.2', 11212, 1),
    }
    pfx = 'fallz3'
    local fallz3 = {
        srv(pfx .. 'srv1', '127.0.2.3', 11212, 1),
    }

    local main_zones = {
        foo = { z1 = fooz1, z2 = fooz2, z3 = fooz3 },
        bar = { z1 = barz1, z2 = barz2, z3 = barz3 },
        fall = { z1 = fallz1, z2 = fallz2, z3 = fallz3 },
    }

    -- FIXME: should we copy the table to keep the pool tables around?
    -- does the hash selector hold a reference to the pool (but only available in main config?)

    -- convert the pools into hash selectors.
    -- TODO: is this a good place to add prefixing/hash editing?
    for _, subs in pairs(main_zones) do
        for k, v in pairs(subs) do
            subs[k] = mcp.hash_selector(mcp.hash_murmur3, v)
        end
    end

    return main_zones
end

-- WORKER CODE:

-- need to redefine main_zones using fetched selectors?

-- TODO: Fallback zone here?
-- TODO: get rid of the local arg redefinitions.
function failover_factory(zones, local_zone)
    local near_zone = zones[local_zone]
    local far_zones = {}
    -- NOTE: could shuffle/sort to re-order zone retry order
    -- or use 'next(far_zones, idx)' via a stored upvalue here
    for k, v in pairs(zones) do
        if k ~= local_zone then
            far_zones[k] = v
        end
    end
    return function(r)
        local res = near_zone(r)
        if res:ok() == false then
            for _, zone in pairs(far_zones) do
                res = zone(r)
                if res:ok() then
                    break
                end
            end
        end
        return res -- send result back to client
    end
end

function prefix_factory(pattern, list, default)
    local p = pattern
    local l = list
    local d = default
    return function(r)
        local route = l[string.match(r:key(), p)]
        if route == nil then
            return d(r)
        end
        return route(r)
    end
end

-- TODO: Check tail call requirements?
function command_factory(map, default)
    local m = map
    local d = default
    return function(r)
        local f = map[r:command()]
        if f == nil then
            print("default command")
            return d(r)
        end
        print("override command")
        return f(r)
    end
end

-- TODO: is the return value the average? anything special?
function walkall_factory(pool)
    local p = {}
    -- convert the pool into a list of servers.
    -- TODO: __pairs can be used for selectors to recover their server objects.
    -- TODO: a shuffle could be useful here.
    for _, v in pairs(pool) do
        table.insert(p, v)
    end
    local x = #p -- FIXME: did #n get accelerated?
    return function(r)
        local res
        for i=1,x,1 do
            res = p[i](r)
        end
        return res
    end
end

local Request = {__name = "mcp.lua_request"}
--local CommandMap = {
--    get = mcp.REQUEST_GET,
--    set = mcp.REQUEST_SET,
--    delete = mcp.REQUEST_DELETE,
--}

-- TODO: optimize: alias string.gmatch, loop for table insert
-- TODO: note if key or string modified and rebuild string during __tostring.
function Request:new(r)
    r = {req = r, tokens = {}}
    -- tokenize the request
    -- TODO: could optimize by using a more complete parser.
    -- else requests with lots of tokens (meta commands) are unecessarily
    -- slow.
    for t in string.gmatch(r.req, "%S+") do
        table.insert(r.tokens, t)
    end
    r.cmd = CommandMap[r.tokens[1]]
    self.__index = self
    setmetatable(r, self)
    return r
end

function Request:key(k)
    if k then
        self.k = k
    else
        if self.k then
            return self.k
        else
            return self.tokens[2]
        end
    end
end

function Request:command()
    return self.cmd
end

function Request:__tostring()
    return self.req
end

function mcp_config_routes(main_zones)
    -- generate the prefix routes from zones.
    local prefixes = {}
    for pfx, z in pairs(main_zones) do
        local failover = failover_factory(z, my_zone)
        local all = walkall_factory(main_zones[pfx])
        local map = {}
        map[mcp.CMD_SET] = all
        map[mcp.CMD_DELETE] = all
        prefixes[pfx] = command_factory(map, failover)
    end

    -- TODO: could also wrap routetop with a final failover:
    -- modify the exptime for sets.
    local routetop = prefix_factory("^/(%a+)/", prefixes, function(r) return "NO ROUTE\r\n" end)
    -- internally run parser at top of tree
    -- also wrap the request string with a convenience object until the C bits
    -- are attached to the internal parser.
    --mcp.attach(mcp.REQUEST_ANY, function (r) return routetop(Request:new(r)) end)
    mcp.attach(mcp.CMD_ANY, function (r) return routetop(r) end)
end