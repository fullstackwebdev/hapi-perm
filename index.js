'use strict';

const { r } = require('rethinkdb-ts');
const pkg = require('./package.json');
const Timeout = require('await-timeout');

const register = async (server, options) => {
    const queryTimeoutMs = options.queryTimeout * 1000 || 2000;

    await r.connectPool(options);

    server.decorate('server', 'db', async (query) => {

        let result;
        try {
            // .wait is broken in rethinkdb 
            // Run query, timeout 
            const timer = new Timeout();
            try {
                result = await Promise.race([
                    r.expr(query).run(),
                    timer.set(queryTimeoutMs)
                        .then(() => Promise.reject('Timeout'))
                ]);
            } finally {
                timer.clear();
            }
        } catch (error) {
            if(error === 'Timeout') {
                console.log('Query Timeout! Database connection is', conn.open);
                console.error(error);
                throw new Error('Timeout exceeded for query');  // CRITICAL TODO: THIS IS NOT CAUGHT ANYWHERE! for decorator
            } else {
                console.log('Critical uncaught error');
                console.error(error);
                throw error;  // CRITICAL TODO: THIS IS NOT CAUGHT ANYWHERE! for decorator!
            }
        }
        return result;
    });

    server.decorate('server', 'dbCursor', async (query) => {
        let cursor;
        try {
            // .wait is broken in rethinkdb 
            // Run query, timeout 
            const timer = new Timeout();
            try {
                cursor = await Promise.race([
                    // From rethinkdb-ts README.md
                    /* 
                    ... No { cursor: true } option, for getting a cursor use .getCursor(runOptions) instead of .run(runOptions)
.run() will coerce streams to array by default feeds will return a cursor like rethinkdbdash

                    */
                    r.expr(query).getCursor(),  // Note: not .run()
                    timer.set(queryTimeoutMs)
                        .then(() => Promise.reject('Timeout'))
                ]);
            } finally {
                timer.clear();
            }
        } catch (error) {
            if(error === 'Timeout') {
                console.log('Query Timeout! Database connection is', conn.open);
                console.error(error);
                throw new Error('Timeout exceeded for query');  // CRITICAL TODO: THIS IS NOT CAUGHT ANYWHERE! for decorator
            } else {
                console.log('Critical uncaught error');
                console.error(error);
                throw error;  // CRITICAL TODO: THIS IS NOT CAUGHT ANYWHERE! for decorator!
            }
        }
        return {
            cursor,
            done = () => {}
        };
    });
    server.events.on('stop', () => {
        conn.close();
    });
};

module.exports.plugin = {
    register,
    pkg
};
