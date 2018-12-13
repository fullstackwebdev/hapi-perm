'use strict';
/* eslint-disable id-length, prefer-promise-reject-errors, arrow-body-style */

const Timeout = require('await-timeout');
const { r } = require('rethinkdb-ts');
const pkg = require('./package.json');

const register = async (server, options) => {
    const queryTimeoutMs = options.queryTimeout * 1000 || 2000;
    options.pingInterval = options.pingInterval || 2;

    let conn = await r.connectPool(options);

    server.events.on('start', async () => {
        try {
            conn = await r.connectPool(options);
        }
        catch (err) {
            console.error(err);
            throw err;
        }
    });

    server.decorate('server', 'r', r);

    server.decorate('server', 'db', async (query) => {
        let result;
        try {
            // A .wait is broken in rethinkdb
            // Run query, timeout
            const timer = new Timeout();
            try {
                result = await Promise.race([
                    query.run(),
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
                        .then(() => Promise.reject('Timeout'))
                ]);
            }
            finally {
                timer.clear();
            }
        }
        catch (err) {
            if (err === 'Timeout') {
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
        conn.drain();
    });
};

module.exports.plugin = {
    register,
    pkg
};

module.exports.r = r;
