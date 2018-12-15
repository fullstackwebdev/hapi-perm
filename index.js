'use strict';
/* eslint-disable id-length, prefer-promise-reject-errors, arrow-body-style */

const Timeout = require('await-timeout');
const { r } = require('rethinkdb-ts');
const pkg = require('./package.json');

const register = async (server, options) => {
    const queryTimeoutMs = options.queryTimeout * 1000 || 5000;
    options.pingInterval = options.pingInterval || 1;

    let conn;
    async function reconnect() {
        if (options.useConnPool) {
            conn = await r.connectPool(options);
        }
        else {
            try {
                conn = await r.connect(options);
            }
            catch (error) {
                console.error(error.cause);
                throw error;
            }
        }
    }
    // Hapi Inject doesn't wait for plugins to load nor sends 'start' event, so we try to connect
    await reconnect();

    server.events.on('start', async () => {
        reconnect();
    });

    server.decorate('server', 'r', r);

    server.decorate('server', 'db', async (query) => {
        let result;

        if (options.useConnPool) {
            if (!conn.isHealthy) {
                await reconnect();
            }
            try {
            // A .wait is broken in rethinkdb
            // Run query, timeout
                const timer = new Timeout();
                try {
                    result = await Promise.race([
                        query.run(), // Connection pool
                        timer.set(queryTimeoutMs)
                            .then(() => Promise.reject('timeout'))
                    ]);
                }
                finally {
                    timer.clear();
                }
            }
            catch (err) {
                if (err === 'timeout') {
                    throw new Error('Timeout exceeded for query');
                }
                else {
                    throw err;
                }
            }
        }
        else {
            try {
                const timer = new Timeout();
                try {
                    result = await Promise.race([
                        r.expr(query).run(conn), // Non-connection pool
                        timer.set(queryTimeoutMs)
                            .then(() => Promise.reject('timeout'))
                    ]);
                }
                finally {
                    timer.clear();
                }
            }
            catch (err) {
                if (err === 'timeout') {
                    throw new Error('Timeout exceeded for query');
                }
                else {
                    throw err;
                }
            }
        }

        return result;
    });

    server.decorate('server', 'dbCursor', async (query) => {
        let cursor;
        try {
            const timer = new Timeout();
            try {
                cursor = await Promise.race([
                    query.getCursor(),
                    timer.set(queryTimeoutMs)
                        .then(() => Promise.reject('timeout'))
                ]);
            }
            finally {
                timer.clear();
            }
        }
        catch (err) {
            if (err === 'timeout') {
                throw new Error('Timeout exceeded for query');
            }
            else {
                console.error(err);
                throw err;
            }
        }
        return {
            cursor
        };
    });
    server.events.on('stop', () => {
        if (options.useConnPool) {
            r.getPoolMaster().drain();
        }
    });
};

module.exports.plugin = {
    register,
    pkg
};

module.exports.r = r;
