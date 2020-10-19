/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Functions for handling the proxy layer. wraps text protocols
 */

#include <string.h>
#include <stdlib.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include "memcached.h"
#include "proto_proxy.h"
#include "proto_text.h"
#include "murmur3_hash.h"

// FIXME: do include dir properly.
#include "vendor/mcmc/mcmc.h"

#define ENDSTR "END\r\n"
#define ENDLEN sizeof(ENDSTR)-1

#define MCP_THREAD_UPVALUE 1
#define MCP_ATTACH_UPVALUE 2

static void process_proxy_command(conn *c, char *command, size_t cmdlen);
static void dump_stack(lua_State *L);
static void mcp_queue_io(conn *c, lua_State *l, lua_State *Lc);
static void proxy_server_handler(const int fd, const short which, void *arg);

typedef uint32_t (*hash_selector_func)(const void *key, size_t len);
typedef struct {
    hash_selector_func func;
} mcp_hashfunc_t;

static mcp_hashfunc_t mcplib_hashfunc_murmur3 = { MurmurHash3_x86_32 };

typedef struct _io_pending_proxy_t io_pending_proxy_t;

// TODO: array of clients based on connection limit.
#define MAX_IPLEN 45
#define MAX_PORTLEN 6
typedef struct {
    char ip[MAX_IPLEN+1];
    char port[MAX_PORTLEN+1];
    double weight;
    void *client; // mcmc client
    io_pending_proxy_t *req_stack_head;
    io_pending_proxy_t *req_stack_tail;
    char *rbuf; // TODO: from thread's rbuf cache.
    struct event event; // libevent
    bool connecting; // in the process of an asynch connection.
    bool can_write; // recently got a WANT_WRITE or are connecting.
} mcp_server_t;

typedef struct {
    mcmc_resp_t resp;
    int status; // status code from mcmc_read()
    item *it; // for buffering large responses.
    char *buf; // response line + potentially value.
    size_t blen;
} mcp_resp_t;

// re-cast an io_pending_t into this more descriptive structure.
// the first few items _must_ match the original struct.
struct _io_pending_proxy_t {
    io_queue_t *q;
    conn *c;
    mc_resp *resp;  // original struct ends here

    struct _io_pending_proxy_t *next; // request chain for batch submission.
    struct _io_pending_proxy_t *server_next; // sub-chain when queued on a server.
    int coro_ref; // lua registry reference to the coroutine
    lua_State *coro; // pointer directly to the coroutine
    mcp_server_t *server; // backend server to request from
    const char *req; // request string
    size_t reqlen; // real length of the request
    mcp_resp_t *client_resp; // reference (currently pointing to a lua object)
    bool flushed; // whether we've fully written this request to a backend.
};

// -------------- EXTERNAL FUNCTIONS

// Initialize the VM for an individual worker thread.
// TODO: global config thread system.
void proxy_thread_init(LIBEVENT_THREAD *thr) {
    lua_State *L = luaL_newstate();
    thr->L = L;
    luaL_openlibs(L);
    proxy_register_libs(thr, L);

    // load/compile the function into our instance.
    // TODO: need real error function.
    if (luaL_dofile(L, settings.proxy_startfile)) {
        fprintf(stderr, "Failed to run lua initializer for worker: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    // config should define functions:
    // - mcp_config_selectors
    //   - returns table containing new selectors
    //   - or nil to not overwrite.
    // - mcp_config_routes(ss)
    //   - uses mcp.attach() to define new functions to call
    // TODO: we fetch and call these here now,
    // but they will move. :P
    lua_getglobal(L, "mcp_config_selectors");

    // TODO: check if it worked?
    lua_pushnil(L); // no "old" config yet.
    if (lua_pcall(L, 1, 1, 0) != LUA_OK) {
        fprintf(stderr, "Failed to execute mcp_config_selectors: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    // selector lib is top on stack.
    lua_getglobal(L, "mcp_config_routes");
    lua_rotate(L, -2, -1); // move the return value from config_selectors in front of the function.
    if (lua_pcall(L, 1, 1, 0) != LUA_OK) {
        fprintf(stderr, "Failed to execute mcp_config_routes: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }
}

static void _flush_pending_writes(mcp_server_t *s) {
    io_pending_proxy_t *p = s->req_stack_head;

    // Told to check queue with nothing in it; connect-ahead?
    if (p == NULL) {
        return;
    }
    int flags = EV_TIMEOUT;
    // TODO: since this is worker-inline only, we're pulling the event base
    // from the requests. when bg pool owns servers this will change.
    struct timeval tmp_time = {5,0}; // FIXME: temporary hard coded response timeout.
    struct event_base *base = NULL;

    while (p) {
        io_pending_proxy_t *next_p = p->server_next;
        base = p->c->thread->base;
        if (!p->flushed) {
            int status = mcmc_send_request(s->client, p->req, p->reqlen, 1);

            if (status == MCMC_WANT_WRITE) {
                s->can_write = false; // not necessary but for consistency.
                flags |= EV_WRITE;
                return; // can't continue for now.
            } else if (status != MCMC_OK) {
                // TODO: error propagation.
                return;
            }

            flags |= EV_READ;
        }

        p = next_p;
    }

    // set the event.
    // FIXME: function.
    if (event_initialized(&s->event) == 0 || event_pending(&s->event, EV_READ|EV_WRITE, NULL) == 0) {
        event_assign(&s->event, base, mcmc_fd(s->client),
                flags, proxy_server_handler, s);
        event_add(&s->event, &tmp_time);
    }

}

// The libevent callback handler.
static void proxy_server_handler(const int fd, const short which, void *arg) {
    mcp_server_t *s = arg;

    if (which & EV_READ) {
        // FIXME: get the buffer from thread?
        if (s->rbuf == NULL) {
            s->rbuf = malloc(READ_BUFFER_SIZE);
        }
        // TODO: check rbuf.

        io_pending_proxy_t *p = s->req_stack_head;
        while (p) {
            // hold the next ptr because we lose access to *p
            io_pending_proxy_t *next_p = p->server_next;
            mcmc_resp_t tmp_resp; // helper for testing for GET's END marker.
            if (s->req_stack_tail == p) {
                assert(next_p == NULL);
                s->req_stack_tail = NULL;
            }
            mcp_resp_t *r = p->client_resp;
            int res = 1;

            r->status = mcmc_read(s->client, s->rbuf, READ_BUFFER_SIZE, &r->resp);
            if (r->status != MCMC_OK) {
                // TODO: ??? reduce io_pending and break?
                // TODO: check for WANT_READ and re-add the event.
            }

            // we actually don't care about anything other than the value length
            // for now.
            // TODO: if vlen != vlen_read, pull an item and copy the data.
            int extra_space = 0;
            switch (r->resp.type) {
                case MCMC_RESP_GET:
                    // We're in GET mode. we only support one key per
                    // GET in the proxy backends, so we need to later check
                    // for an END.
                    extra_space = ENDLEN;
                    break;
                case MCMC_RESP_END:
                    // this is a MISS from a GET request
                    // or final handler from a STAT request.
                    assert(r->resp.vlen == 0);
                    break;
                case MCMC_RESP_META:
                    // we can handle meta responses easily since they're self
                    // contained.
                    break;
                // TODO: No-op response?
                default:
                    // unhandled :(
                    // TODO: set the status code properly?
                    res = 0;
                    fprintf(stderr, "UNHANDLED: %d\n", r->resp.type);
                    break;
            }

            if (res) {
                // r->resp.reslen + r->resp.vlen is the total length of the response.
                // TODO: need to associate a buffer with this response...
                // for now lets abuse write_and_free on mc_resp and simply malloc the
                // space we need, stuffing it into the resp object.
                // how will lua be able to generate a fake response tho?
                // TODO: if item is large enough (or not fully read), allocate
                // an item and copy into it or try an immediate read.
                // need to stop and re-schedule the event if not enough data.

                size_t tlen = r->resp.reslen + r->resp.vlen;
                char *buf = malloc(tlen + extra_space);
                // TODO: check buf. but also avoid the malloc via slab alloc.

                if (r->resp.vlen == r->resp.vlen_read) {
                    // TODO: some mcmc func for pulling the whole buffer?
                    memcpy(buf, s->rbuf, tlen);
                } else {
                    // TODO: mcmc func for pulling the res off the buffer?
                    memcpy(buf, s->rbuf, r->resp.reslen);
                    // got a partial read on the value, pull in the rest.
                    int read = 0;
                    int status = mcmc_read_value(s->client, buf+r->resp.reslen, r->resp.vlen, &read);
                    if (status == MCMC_OK) {
                        // all done copying data.
                    } else if (status == MCMC_WANT_READ) {
                        // TODO: remember *buf and &read's offsets.
                        // TODO: stream larger values' chunks?
                    } else {
                        // TODO: error handling.
                    }
                }
                r->buf = buf;
                r->blen = tlen;
            } else {
                // TODO: no response read?
            }

            int remain = 0;
            char *newbuf = NULL;
            // Check the type again in case we need to do post-processing, or
            // loop.
            switch (r->resp.type) {
                case MCMC_RESP_GET:
                    // we need to advance the buffer and ensure the next data
                    // in the stream is "END\r\n"
                    // if not, the stack is desynced and we lose it.
                    newbuf = mcmc_buffer_consume(s->client, &remain);

                    if (remain > ENDLEN) {
                        // enough bytes in the buffer for our potential END
                        // marker, so lets avoid an unnecessary memmove.
                    } else if (remain != 0) {
                        memmove(s->rbuf, newbuf, remain);
                        newbuf = s->rbuf;
                    }

                    if (mcmc_read(s->client, newbuf, READ_BUFFER_SIZE-remain, &tmp_resp) != MCMC_OK) {
                        // TODO: something?
                    } else if (tmp_resp.type != MCMC_RESP_END) {
                        // TODO: protocol is desynced, need to dump queue.
                    } else {
                        // response is good.
                        // FIXME: copy what the server actually sent?
                        memcpy(r->buf+r->blen, ENDSTR, ENDLEN);
                        r->blen += 5;
                    }

                    break;
                default:
                    break;
            }

            // have to do the q->count-- and == 0 and redispatch_conn()
            // stuff here. The moment we call that write we
            // don't own *p anymore.
            // FIXME: io_pending needs a lock.
            p->q->count--;
            if (p->q->count == 0) {
                redispatch_conn(p->c);
            }
            // set the p here. if we break below the head will be correct.
            p = next_p;

            // TODO: mcmc_buffer_consume() - if leftover, keep processing
            // IO's.
            // if no more data in buffer, need to re-set stack head and re-set
            // event.
            remain = 0;
            newbuf = mcmc_buffer_consume(s->client, &remain);
            if (remain != 0) {
                // data trailing in the buffer, for a different request.
                memmove(s->rbuf, newbuf, remain);
            } else {
                break;
            }
        }
        s->req_stack_head = p;
        // TODO: need to re-add the read event if the stack isn't NULL
    }

    // allow dequeuing anything ready to be read before we process EV_TIMEOUT;
    // though it might not be possible for both to fire.
    if (which & EV_TIMEOUT) {
        // TODO: walk stack and set timeout status on each object.
        // then return.
    }

    if (which & EV_WRITE) {
        s->can_write = true;
        if (s->connecting) {
            int err = 0;
            // We were connecting, now ensure we're properly connected.
            if (mcmc_check_nonblock_connect(s->client, &err) != MCMC_OK) {
                // TODO: for now we kill the stack. need to retry a few times
                // first.
                // TODO: will need a mechanism for max retries, waits between
                // fast-fails, and failing the stack equivalent to a timeout.
            }

        }
        // TODO: walk stack until we've flushed everything.
        // need indicators on written amounts.
        _flush_pending_writes(s);
    }

}

// ctx_stack is a stack of io_pending_proxy_t's.
// TODO: once we have background thread[s] for handling backend servers
// this will need to do a lot more work.
// For right now, each worker thread owns independent server objects.
// Flow:
// - run requests directly
// - use p->c's event base to give the server an event handler callback
// - re-stack IO's, in order the responses will be checked, onto the server
// - that's it for here.
// TODO: after moving to the dedicated thread, we can look for batching
// opportunities. need an mcmc iovec interface first though.
void proxy_submit_cb(void *ctx, void *ctx_stack) {
    io_pending_proxy_t *p = ctx_stack;
    struct timeval tmp_time = {5,0}; // FIXME: temporary hard coded response timeout.

    while (p) {
        mcp_server_t *s = p->server;

        // If we're not in the process of connecting, we can immediately issue
        // the request.
        if (s->can_write) {
            // TODO: check for connected, reconnect if necessary.
            int status = mcmc_send_request(s->client, p->req, p->reqlen, 1);

            if (status == MCMC_WANT_WRITE) {
                // avoid syscalls for any other queued requests.
                s->can_write = false;
                // s->client is tracking the amount of data already sent on
                // this request. we need to call it again with the same
                // arguments later.
            } else if (status != MCMC_OK) {
                // TODO: real error propagation.
                // should just need to mark the request as done, io_pending--, etc.
                fprintf(stderr, "Failed to send request to memcached: %s:%s\n", s->ip, s->port);
                // FIXME: p = p->next; continue; ?
                return;
            } else {
                p->flushed = true;
            }
        }

        // FIXME: chicken and egg.
        // can't check if pending if the structure is was calloc'ed (sigh)
        // don't want to double test here. should be able to event_assign but
        // not add anything during initialization, but need the owner thread's
        // event base.
        if (event_initialized(&s->event) == 0 || event_pending(&s->event, EV_READ|EV_WRITE, NULL) == 0) {
            // if we can't write, we could be connecting.
            // TODO: always checking for READ in case some commands were sent
            // successfully. The flags could be tracked on *s and reset in the
            // handler, perhaps?
            int flags = s->can_write ? EV_READ|EV_TIMEOUT : EV_READ|EV_WRITE|EV_TIMEOUT;
            event_assign(&s->event, p->c->thread->base, mcmc_fd(s->client),
                    flags, proxy_server_handler, s);
            event_add(&s->event, &tmp_time);
        }

        // stack IO using secondary next ptr. need to guarantee FIFO.
        // this may not always be safe; IO's are "stuck" until processed out
        // from a server object. will that always be true? if not, we need to
        // allocate an array on the server object to hold pending items.
        if (s->req_stack_head == NULL) {
            s->req_stack_head = p;
        }
        if (s->req_stack_tail == NULL) {
            s->req_stack_tail = p;
        } else {
            s->req_stack_tail->server_next = p;
            s->req_stack_tail = p;
            p->server_next = NULL;
        }

        p = p->next;
    }

    return;
}

// this resumes every yielded coroutine (and re-resumes if necessary).
// called from the worker thread after responses have been pulled from the
// network.
// Flow:
// - the response object should already be on the coroutine stack.
// - fix up the stack.
// - lua_resume()
// - if LUA_YIELD, we need to swap out the pending IO from its mc_resp then call for a queue
// again.
// - if LUA_OK finalize the response and return
// - else set error into mc_resp.
// TODO: can this abstract function encompass the original call too?
//   - would need to account for an existing io_pending but otherwise fine?
void proxy_complete_cb(void *ctx, void *ctx_stack) {
    io_pending_proxy_t *p = ctx_stack;

    while (p) {
        int nresults = 0;
        mc_resp *resp = p->resp;
        lua_State *Lc = p->coro;

        // in order to resume we need to remove the objects that were
        // originally returned
        // what's currently on the top of the stack is what we want to keep.
        lua_rotate(Lc, 1, 1);
        // We kept the original results from the yield so lua would not
        // collect them in the meantime. We can drop those now.
        lua_pop(Lc, lua_gettop(Lc)-1);

        int cores = lua_resume(Lc, NULL, 1, &nresults);
        size_t rlen = 0;

        if (cores == LUA_OK) {
            int type = lua_type(Lc, -1);
            if (type == LUA_TUSERDATA) {
                mcp_resp_t *r = luaL_checkudata(Lc, -1, "mcp.response");
                // TODO: utilize r->it.
                if (r->buf) {
                    // response set from C.
                    // FIXME: write_and_free() ? it's a bit wrong for here.
                    resp->write_and_free = r->buf;
                    resp_add_iov(resp, r->buf, r->blen);
                    r->buf = NULL;
                } else if (lua_getiuservalue(Lc, -1, 1) != LUA_TNONE) {
                    // response set into lua via an internal.
                    const char *s = lua_tolstring(Lc, -1, &rlen);
                    size_t l = rlen > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : rlen;
                    memcpy(resp->wbuf, s, l);
                    resp_add_iov(resp, resp->wbuf, l);
                    lua_pop(Lc, 1);
                }
            } else if (type == LUA_TSTRING) {
                // response is a raw string from lua.
                const char *s = lua_tolstring(Lc, -1, &rlen);
                size_t l = rlen > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : rlen;
                memcpy(resp->wbuf, s, l);
                resp_add_iov(resp, resp->wbuf, l);
                lua_pop(Lc, 1);
            } else {
                memcpy(resp->wbuf, "ERROR\r\n", 7);
                resp_add_iov(resp, resp->wbuf, 7);
            }

        } else if (cores == LUA_YIELD) {
            // TODO: need to remove/free and re-create io-pending, or extend
            // mcp_queue_io() to overwrite an existing IO.
            // if we're freeing/etc then we need to cuddle p->next in the
            // while loop.
            printf("C: yield from completed: %d\n", nresults);
            dump_stack(Lc);
        } else {
            // error?
            fprintf(stderr, "Failed to run coroutine: %s\n", lua_tostring(Lc, -1));
            // TODO: send generic ERROR and stop here. Also I know the length
            // is wrong :)
            memcpy(resp->wbuf, "SERVER_ERROR lua failure\r\n", 15);
            resp_add_iov(resp, resp->wbuf, 15);
        }

        p = p->next;
    }
    return;
}

// called from the worker thread as an mc_resp is being freed.
// must let go of the coroutine reference if there is one.
// caller frees the pending IO.
void proxy_finalize_cb(io_pending_t *pending) {
    io_pending_proxy_t *p = (io_pending_proxy_t *)pending;

    // release our coroutine reference.
    // TODO: coroutines are reusable in latest lua. we can stack this onto a freelist
    // after a lua_resetthread(Lc) call.
    if (p->coro_ref) {
        luaL_unref(p->coro, LUA_REGISTRYINDEX, p->coro_ref);
    }
    return;
}

int try_read_command_proxy(conn *c) {
    char *el, *cont;

    if (c->rbytes == 0)
        return 0;

    el = memchr(c->rcurr, '\n', c->rbytes);
    if (!el) {
        if (c->rbytes > 1024) {
            /*
             * We didn't have a '\n' in the first k. This _has_ to be a
             * large multiget, if not we should just nuke the connection.
             */
            char *ptr = c->rcurr;
            while (*ptr == ' ') { /* ignore leading whitespaces */
                ++ptr;
            }

            if (ptr - c->rcurr > 100 ||
                (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {

                conn_set_state(c, conn_closing);
                return 1;
            }

            // ASCII multigets are unbound, so our fixed size rbuf may not
            // work for this particular workload... For backcompat we'll use a
            // malloc/realloc/free routine just for this.
            if (!c->rbuf_malloced) {
                if (!rbuf_switch_to_malloc(c)) {
                    conn_set_state(c, conn_closing);
                    return 1;
                }
            }
        }

        return 0;
    }
    cont = el + 1;
    // TODO: we don't want to cut the \r\n here. lets see how lua handles
    // non-terminated strings?
    /*if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
        el--;
    }
    *el = '\0';*/

    assert(cont <= (c->rcurr + c->rbytes));

    c->last_cmd_time = current_time;
    process_proxy_command(c, c->rcurr, cont - c->rcurr);

    c->rbytes -= (cont - c->rcurr);
    c->rcurr = cont;

    assert(c->rcurr <= (c->rbuf + c->rsize));

    return 1;

}

static void process_proxy_command(conn *c, char *command, size_t cmdlen) {
    LIBEVENT_THREAD *thr = c->thread;
    lua_State *L = thr->L;

    assert(c != NULL);

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d %s\n", c->sfd, command);

    /*
     * for commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */

    // Prep the response object for this query.
    // TODO: Kill this line and dynamically pull them instead?
    if (!resp_start(c)) {
        conn_set_state(c, conn_closing);
        return;
    }

    int nresults = 0;
    // start a coroutine.
    // TODO: This can pull from a cache.
    lua_newthread(L);
    lua_State *Lc = lua_tothread(L, -1);
    // leave the thread first on the stack, so we can reference it if needed.
    // pull the lua hook function onto the stack.
    lua_rawgeti(Lc, LUA_REGISTRYINDEX, thr->proxy_attach_ref);

    // TODO: we ship the raw request string to lua. Once the API is figured
    // out this should be C bits instead so we can reuse memory.
    // FIXME: do we have to push this before chopping the \r\n?
    lua_pushlstring(Lc, command, cmdlen);

    // call the function via coroutine.
    int cores = lua_resume(Lc, NULL, 1, &nresults);
    mc_resp *resp = c->resp;
    size_t rlen = 0;

    if (cores == LUA_OK) {
        // figure out if we have a response object, raw string, or what.
        int type = lua_type(Lc, -1);

        if ((type == LUA_TUSERDATA && lua_getiuservalue(Lc, -1, 1) != LUA_TNONE) || type == LUA_TSTRING) {
            // FIXME: checkudata, testudata? anything to directly check this?
            const char *s = lua_tolstring(Lc, -1, &rlen);
            size_t l = rlen > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : rlen;
            memcpy(resp->wbuf, s, l);
            resp_add_iov(resp, resp->wbuf, l);
            lua_pop(Lc, 1);
        } else {
            memcpy(resp->wbuf, "ERROR\r\n", 7);
            resp_add_iov(resp, resp->wbuf, 7);
        }
    } else if (cores == LUA_YIELD) {
        // NOTE: this is returning "self" somehow. not sure if it matters.
        dump_stack(Lc);
        // This holds a reference to Lc so it can be resumed on this thread
        // later. Lc itself holds references to server/request data.
        mcp_queue_io(c, L, Lc);
    } else {
        // error?
        fprintf(stderr, "Failed to run coroutine: %s\n", lua_tostring(Lc, -1));
        // TODO: send generic ERROR and stop here.
        memcpy(resp->wbuf, "SERVER_ERROR lua failure\r\n", 15);
        resp_add_iov(resp, resp->wbuf, 15);
    }

    /*printf("main thread stack:\n");
    dump_stack(L);
    printf("\ncoroutine thread stack:\n");
    dump_stack(Lc);*/
}

// analogue for storage_get_item(); add a deferred IO object to the current
// connection's response object. stack enough information to write to the
// server on the submit callback, and enough to resume the lua state on the
// completion callback.
static void mcp_queue_io(conn *c, lua_State *L, lua_State *Lc) {
    io_queue_t *q = conn_io_queue_get(c, IO_QUEUE_PROXY);
    mc_resp *resp = c->resp;

    // Top of the Lc stack should be server. Will hold onto this for now.
    mcp_server_t *s = luaL_checkudata(Lc, -1, "mcp.server");

    // Then the request object.
    luaL_checktype(Lc, -2, LUA_TTABLE);
    size_t reqlen = 0;
    const char *req = luaL_tolstring(Lc, -2, &reqlen);

    // Then we push a response object, which we'll re-use later.
    // reserve one uservalue for a lua-supplied response.
    mcp_resp_t *r = lua_newuserdatauv(Lc, sizeof(mcp_resp_t), 1);
    // TODO: check *r
    r->buf = NULL;
    r->blen = 0;

    luaL_getmetatable(Lc, "mcp.response");
    lua_setmetatable(Lc, -2);

    io_pending_proxy_t *p = do_cache_alloc(c->thread->io_cache);
    // FIXME: can this fail?

    // this is a re-cast structure, so assert that we never outsize it.
    assert(sizeof(io_pending_t) >= sizeof(io_pending_proxy_t));
    memset(p, 0, sizeof(io_pending_proxy_t));
    // set up back references.
    p->q = q;
    p->c = c;
    p->resp = resp;
    p->client_resp = r;
    p->flushed = false;
    resp->io_pending = (io_pending_t *)p;

    // top of the main thread should be our coroutine.
    // lets grab a reference to it and pop so it doesn't get gc'ed.
    p->coro_ref = luaL_ref(L, LUA_REGISTRYINDEX);

    // we'll drop the pointer to the coro on here to save some CPU
    // on re-fetching it later. The pointer shouldn't change.
    p->coro = Lc;

    // The direct server object. Lc is holding the reference in the stack
    p->server = s;

    // The stringified request. This is also referencing into the coroutine
    // stack, which should be safe from gc.
    p->req = req;
    p->reqlen = reqlen;

    // link into the batch chain.
    p->next = q->stack_ctx;
    q->stack_ctx = p;
    q->count++;

    return;
}

static void dump_stack(lua_State *L) {
    int top = lua_gettop(L);
    int i = 1;
    printf("--TOP OF STACK [%d]\n", top);
    for (; i < top + 1; i++) {
        int type = lua_type(L, i);
        // lets find the metatable of this userdata to identify it.
        if (lua_getmetatable(L, i) != 0) {
            lua_pushstring(L, "__name");
            if (lua_rawget(L, -2) != LUA_TNIL) {
                printf("--|%d| [%s] (%s)\n", i, lua_typename(L, type), lua_tostring(L, -1));
                lua_pop(L, 2);
                continue;
            }
            lua_pop(L, 2);
        }
        printf("--|%d| [%s]\n", i, lua_typename(L, type));
    }
    printf("-----------------\n");
}

// func prototype example:
// static int fname (lua_State *L)
// normal library open:
// int luaopen_mcp(lua_State *L) { }

// resp:ok()
static int mcplib_response_ok(lua_State *L) {
    mcp_resp_t *r = luaL_checkudata(L, -1, "mcp.response");

    if (r->status == MCMC_OK) {
        lua_pushboolean(L, 1);
    } else {
        lua_pushboolean(L, 0);
    }

    return 1;
}

static int mcplib_server(lua_State *L) {
    const char *ip = luaL_checkstring(L, -3); // FIXME: checklstring?
    const char *port = luaL_checkstring(L, -2);
    double weight = luaL_checknumber(L, -1);

    // This might shift to internal objects?
    mcp_server_t *s = lua_newuserdatauv(L, sizeof(mcp_server_t), 0);
    // TODO: check s
    
    strncpy(s->ip, ip, MAX_IPLEN);
    strncpy(s->port, port, MAX_PORTLEN);
    s->weight = weight;
    s->rbuf = NULL;
    s->req_stack_head = NULL;
    s->req_stack_tail = NULL;
    s->connecting = false;
    s->can_write = false;

    // initialize libevent.
    memset(&s->event, 0, sizeof(s->event));

    // initialize the client
    s->client = malloc(mcmc_size(MCMC_OPTION_BLANK));
    // TODO: connect elsewhere? Any reason not to immediately shoot off a
    // connect?
    int status = mcmc_connect(s->client, s->ip, s->port, MCMC_OPTION_NONBLOCK);
    if (status == MCMC_CONNECTED) {
        // FIXME: is this possible? do we ever want to allow blocking
        // connections?
        fprintf(stderr, "Unexpectedly connected to backend early: %s:%s\n", s->ip, s->port);
        // FIXME: propagate error.
    } else if (status == MCMC_CONNECTING) {
        s->connecting = true;
        s->can_write = false;
    } else {
        fprintf(stderr, "Failed to connect to memcached: %s:%s\n", s->ip, s->port);
        // FIXME: propagate error.
    }

    luaL_getmetatable(L, "mcp.server");
    lua_setmetatable(L, -2); // set metatable to userdata.

    //printf("created a new server\n");
    return 1;
}

// TODO: hash func should take an initializer
// so we can hash a few things together.
// also, 64bit? 32bit? either?
// TODO: hash selectors should hash to a list of "Things"
// things can be anything callable: lua funcs, hash selectors, servers.
typedef struct {
    int ref; // luaL_ref reference.
    mcp_server_t *srv;
} mcp_hash_selector_srv_t;
typedef struct {
    hash_selector_func func;
    int pool_size;
    mcp_hash_selector_srv_t pool[];
} mcp_hash_selector_t;

// ss = mcp.hash_selector(hashfunc, pool)
static int mcplib_hash_selector(lua_State *L) {
    // TODO: need some hash funcs.
    luaL_checktype(L, -2, LUA_TLIGHTUSERDATA);
    luaL_checktype(L, -1, LUA_TTABLE);
    int n = luaL_len(L, -1); // get length of array table

    // TODO: this'll have to change to a pointer, since that pointer will get
    // shuffled off somewhere else.
    mcp_hash_selector_t *ss = lua_newuserdatauv(L, sizeof(mcp_hash_selector_t) + sizeof(mcp_hash_selector_srv_t) * n, 0);
    // TODO: check ss.
    ss->pool_size = n;

    luaL_setmetatable(L, "mcp.hash_selector");

    // TODO: ensure to increment refcounts for servers.
    // remember lua arrays are 1 indexed.
    // TODO: we need a second array with luaL_ref()'s to each of the servers.
    // or an array of structs which hold ptr's.
    for (int x = 1; x <= n; x++) {
        mcp_hash_selector_srv_t *s = &ss->pool[x-1];
        lua_geti(L, -2, x); // get next server into the stack.
        // TODO: do we leak memory if we bail here?
        // the stack should clear, then release the userdata + etc?
        s->srv = luaL_checkudata(L, -1, "mcp.server");
        s->ref = luaL_ref(L, LUA_REGISTRYINDEX); // references and pops object.
    }

    mcp_hashfunc_t *hf = lua_touserdata(L, -3);
    ss->func = hf->func;

    //printf("created new hash selector\n");
    return 1;
}

// hashfunc(request) -> server(request)
// needs key from request object.
// TODO: fetching key from an array index via rawgeti is probably a lot faster
// than the function call :/
static int mcplib_hash_selector_call(lua_State *L) {
    // internal args are the hash selector (self)
    mcp_hash_selector_t *ss = luaL_checkudata(L, -2, "mcp.hash_selector");
    // then the request object (table)
    luaL_checktype(L, -1, LUA_TTABLE);

    lua_getfield(L, -1, "key"); // pull the key function onto the stack
    if (!lua_isfunction(L, -1)) {
        // FIXME: error
    }
    lua_pushvalue(L, -2); // copy the table for self arg

    // call the function to retrieve the key
    if (lua_pcall(L, 1, 1, 0) != LUA_OK) {
        // FIXME: error
    }

    // top of stack should be the key now.
    size_t len = 0;
    const char *key = lua_tolstring(L, -1, &len);

    uint32_t hash = ss->func(key, len);
    int ref = ss->pool[hash % ss->pool_size].ref;
    lua_pop(L, 1); // pop the key. top should now be request again.

    // put the selected server onto the stack.
    lua_rawgeti(L, LUA_REGISTRYINDEX, ref);

    // now yield request, server up.
    return lua_yield(L, 2);
}

// mcp.attach(mcp.HOOK_NAME, function|userdata)
// fill hook structure: if lua function, use luaL_ref() to store the func
// if it a userdata of the proper type, set its C function + data pointers
// for direct callback.
// TODO: only takes lua functions for now.
static int mcplib_attach(lua_State *L) {
    // Pull the original worker thread out of the shared mcplib upvalue.
    LIBEVENT_THREAD *t = lua_touserdata(L, lua_upvalueindex(MCP_THREAD_UPVALUE));

    int hook = luaL_checkinteger(L, -2);
    if (lua_isuserdata(L, -1)) {
        // TODO: not super sure how to create the API here.
        // it needs/should identify to a specific userdata type so we can
        // generically recover the function and data pointer.
    } else if (lua_isfunction(L, -1)) {
        t->proxy_hook = hook;

        if (t->proxy_attach_ref) {
            luaL_unref(L, LUA_REGISTRYINDEX, t->proxy_attach_ref);
        }

        // pops the function from the stack and leaves us a ref. for later.
        t->proxy_attach_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
        // TODO: throw error.
    }

    return 0;
}

// TODO: temporary defines.
// should we call these MCP_* and fix them up in the register function?
#define MCP_OK 0
#define REQUEST_ANY 1
#define REQUEST_GET 2
#define REQUEST_SET 3
#define REQUEST_DELETE 4

static void proxy_register_defines(lua_State *L) {
#define MCP_D(x) \
    lua_pushinteger(L, x); \
    lua_setfield(L, -2, #x);

    MCP_D(MCP_OK);
    MCP_D(REQUEST_ANY);
    MCP_D(REQUEST_GET);
    MCP_D(REQUEST_SET);
    MCP_D(REQUEST_DELETE);

#undef MCP_D
}

// Creates and returns the top level "mcp" module
int proxy_register_libs(LIBEVENT_THREAD *t, void *ctx) {
    lua_State *L = ctx;
    // TODO: stash into a table with weak references?
    // then if no pools/code has references still, can ditch?
    // TODO: __gc
    // TODO: __call - called when... called like a function!
    const struct luaL_Reg mcplib_server_m [] = {
        {"set", NULL},
        {NULL, NULL}
    };

    // TODO: __gc
    const struct luaL_Reg mcplib_request_m[] = {
        {"command", NULL},
        {"key", NULL},
        {NULL, NULL}
    };

    // TODO: __gc
    const struct luaL_Reg mcplib_response_m[] = {
        {"ok", mcplib_response_ok},
        {NULL, NULL}
    };

    // TODO: __gc
    const struct luaL_Reg mcplib_hash_selector_m[] = {
        {"__call", mcplib_hash_selector_call},
        {NULL, NULL}
    };

    const struct luaL_Reg mcplib_f [] = {
        {"hash_selector", mcplib_hash_selector},
        {"server", mcplib_server},
        {"attach", mcplib_attach},
        {NULL, NULL}
    };

    // TODO: function + loop.
    luaL_newmetatable(L, "mcp.server");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_server_m, 0); // register methods
    lua_pop(L, 1);

    luaL_newmetatable(L, "mcp.request");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_request_m, 0); // register methods
    lua_pop(L, 1);

    luaL_newmetatable(L, "mcp.response");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_response_m, 0); // register methods
    lua_pop(L, 1);

    // TODO: We'll need to add methods for at least __gc stuff.
    luaL_newmetatable(L, "mcp.hash_selector");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_hash_selector_m, 0); // register methods
    lua_pop(L, 1); // drop the hash selector metatable

    // create main library table.
    //luaL_newlib(L, mcplib_f);
    // TODO: luaL_newlibtable() just pre-allocs the exact number of things
    // here.
    // can replace with createtable and add the num. of the constant
    // definitions.
    luaL_newlibtable(L, mcplib_f);
    proxy_register_defines(L);

    // hash function for selectors.
    // have to wrap the function in a struct because function pointers aren't
    // pointer pointers :)
    lua_pushlightuserdata(L, &mcplib_hashfunc_murmur3);
    lua_setfield(L, -2, "hash_murmur3");

    lua_pushlightuserdata(L, (void *)t); // upvalue for original thread
    lua_newtable(L); // upvalue for mcp.attach() table.

    luaL_setfuncs(L, mcplib_f, 2); // 2 upvalues.

    lua_setglobal(L, "mcp"); // set the lib table to mcp global.
    //printf("lua libs initialized\n");
    //dump_stack(L);
    return 1;
}
