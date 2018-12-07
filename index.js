'use strict';

const { r } = require('rethinkdb-ts');
const pkg = require('./package.json');
const Timeout = require('await-timeout');

const register = async (server, options) => {
    const queryTimeoutMs = options.queryTimeout * 1000 || 2000;
    options.pingInterval = options.pingInterval || 2;

    let conn = await r.connectPool(options);
    
    server.events.on('start', async () => {
        try {
            await r.connectPool(options);
        } catch (error) {
            console.error(error);
            console.log('real error');
        }
        console.log('Started', r.open);
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
                        .then(() => Promise.reject('Timeout'))
                ]);
            } finally {
                timer.clear();
            }
        } catch (error) {
            if (error === 'Timeout') {
                throw new Error('Timeout exceeded for query');
            } else {
                throw error;
            }
        }
        return result;
    });

    server.decorate('server', 'dbCursor', async (query) => {
        let cursor;
        try {
            // A .wait is broken in rethinkdb 
            // Run query, timeout 
            const timer = new Timeout();
            try {
                cursor = await Promise.race([
                    query.getCursor(),
                    timer.set(queryTimeoutMs)
                        .then(() => Promise.reject('Timeout'))
                ]);
            } finally {
                timer.clear();
            }
        } catch (error) {
            if (error === 'Timeout') {
                throw new Error('Timeout exceeded for query');
            } else {
                console.log('Critical uncaught error');
                console.error(error);
                throw error; 
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
