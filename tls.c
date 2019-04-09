#include "memcached.h"

#ifdef TLS

#include "tls.h"
#include <string.h>
#include <sysexits.h>
#include <sys/param.h>

#ifndef MAXPATHLEN
#define MAXPATHLEN 4096
#endif

static pthread_mutex_t ssl_ctx_lock = PTHREAD_MUTEX_INITIALIZER;

const unsigned MAX_ERROR_MSG_SIZE = 128;

#define MIN(a,b) (((a)<(b))?(a):(b))

void SSL_LOCK() {
    pthread_mutex_lock(&(ssl_ctx_lock));
}

void SSL_UNLOCK(void) {
    pthread_mutex_unlock(&(ssl_ctx_lock));
}

/*
 * Reads decrypted data from the underlying BIO read buffers,
 * which reads from the socket.
 */
ssize_t ssl_read(conn *c, void *buf, size_t count) {
    assert (c != NULL);
    /* TODO : document the state machine interactions for SSL_read with
        non-blocking sockets/ SSL re-negotiations
    */
    return SSL_read(c->ssl, buf, count);
}

/*
 * SSL sendmsg implementation. Perform a SSL_write.
 */
ssize_t ssl_sendmsg(conn *c, struct msghdr *msg, int flags) {
    assert (c != NULL);
    size_t bytes, to_copy;
    int i;
    bytes = 0;
    for (i = 0; i < msg->msg_iovlen; ++i)
        bytes += msg->msg_iov[i].iov_len;

    // ssl_wbuf is pointing to the buffer allocated in the worker thread.
    assert(c->ssl_wbuf);
    // TODO: allocate a fix buffer in crawler/logger if they start using
    // the sendmsg method. Also, set c->ssl_wbuf  when the side thread
    // start owning the connection and reset the pointer in
    // conn_worker_readd.
    // Currntly this connection would not be served by a different thread
    // than the one it's assigned.
    assert(c->thread->thread_id == (unsigned long)pthread_self());

    bytes = MIN(bytes, settings.ssl_wbuf_size);
    to_copy = bytes;
    char *bp = c->ssl_wbuf;
    for (i = 0; i < msg->msg_iovlen; ++i) {
        size_t copy = MIN (to_copy, msg->msg_iov[i].iov_len);
        memcpy((void*)bp, (void*)msg->msg_iov[i].iov_base, copy);
        bp +=  copy;
        to_copy -= copy;
        if (to_copy == 0)
            break;
    }
    /* TODO : document the state machine interactions for SSL_write with
        non-blocking sockets/ SSL re-negotiations
    */
    return SSL_write(c->ssl, c->ssl_wbuf, bytes);
}

/*
 * Writes data to the underlying BIO write buffers,
 * which encrypt and write them to the socket.
 */
ssize_t ssl_write(conn *c, void *buf, size_t count) {
    assert (c != NULL);
    return SSL_write(c->ssl, buf, count);
}

/*
 * Loads server certificates to the SSL context and validate them.
 * @return whether certificates are successfully loaded and verified or not.
 * @param error_msg contains the error when unsuccessful.
 */
static bool load_server_certificates(char *error_msg) {
    bool success = true;
    SSL_LOCK();
    if (!SSL_CTX_use_certificate_chain_file(settings.ssl_ctx,
        settings.ssl_chain_cert)) {
        sprintf(error_msg, "Error loading the certificate chain : %s",
            settings.ssl_chain_cert);
        success = false;
    } else if (!SSL_CTX_use_PrivateKey_file(settings.ssl_ctx, settings.ssl_key,
                                        settings.ssl_keyform)) {
        sprintf(error_msg, "Error loading the key : %s", settings.ssl_key);
        success = false;
    } else if (!SSL_CTX_check_private_key(settings.ssl_ctx)) {
        sprintf(error_msg, "Error validating the certificate");
        success = false;
    } else {
        settings.ssl_last_cert_refresh_time = current_time;
    }
    SSL_UNLOCK();
    return success;
}

/*
 * Verify SSL settings and initiates the SSL context.
 */
int ssl_init(void) {
    assert(settings.ssl_enabled);
    // SSL context for the process. All connections will share one
    // process level context.
    settings.ssl_ctx = SSL_CTX_new(TLS_server_method());
    // Clients should use at least TLSv1.2
    int flags = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 |
                SSL_OP_NO_TLSv1 |SSL_OP_NO_TLSv1_1;
    SSL_CTX_set_options(settings.ssl_ctx, flags);

    // The server certificate, private key and validations.
    char error_msg[MAXPATHLEN + MAX_ERROR_MSG_SIZE];
    if (!load_server_certificates(error_msg)) {
        if (settings.verbose) {
            fprintf(stderr, "%s\n", error_msg);
        }
        exit(EX_USAGE);
    }

    // The verification mode of client certificate, default is SSL_VERIFY_PEER.
    SSL_CTX_set_verify(settings.ssl_ctx, settings.ssl_verify_mode, NULL);
    if (settings.ssl_ciphers && !SSL_CTX_set_cipher_list(settings.ssl_ctx,
                                                    settings.ssl_ciphers)) {
        if (settings.verbose) {
            fprintf(stderr, "Error setting the provided cipher(s) : %s\n",
                    settings.ssl_ciphers);
        }
        exit(EX_USAGE);
    }
    // List of acceptable CAs for client certificates.
    if (settings.ssl_ca_cert)
    {
        SSL_CTX_set_client_CA_list(settings.ssl_ctx,
            SSL_load_client_CA_file(settings.ssl_ca_cert));
        if (!SSL_CTX_load_verify_locations(settings.ssl_ctx,
                            settings.ssl_ca_cert, NULL)) {
            if (settings.verbose) {
                fprintf(stderr, "Error loading the client CA cert (%s)\n",
                        settings.ssl_ca_cert);
            }
            exit(EX_USAGE);
        }
    }
    settings.ssl_last_cert_refresh_time = current_time;
    return 0;
}

/*
 * This method is registered with each SSL connection and abort the SSL session
 * if a client initiates a renegotiation.
 * TODO : Proper way to do this is to set SSL_OP_NO_RENEGOTIATION
 *       using the SSL_CTX_set_options but that option only available in
 *       openssl 1.1.0h or above.
 */
void ssl_callback(const SSL *s, int where, int ret) {
    SSL* ssl = (SSL*)s;
    if (SSL_in_before(ssl)) {
        if (settings.verbose) {
            fprintf(stderr, "%d: SSL renegotiation is not supported, "
                    "closing the connection\n", SSL_get_fd(ssl));
        }
        SSL_set_shutdown(ssl, SSL_SENT_SHUTDOWN | SSL_RECEIVED_SHUTDOWN);
        return;
    }
}

void refresh_certs(void *c) {
    assert(c);
    conn *con = (conn*)c;
    char error_msg[MAXPATHLEN + MAX_ERROR_MSG_SIZE];
    if (load_server_certificates(error_msg)) {
        out_string(con, "OK");
    } else {
        out_string(con, error_msg);
    }
}
#endif
