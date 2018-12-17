'use strict';
/* eslint-disable id-length, no-else-return */
const { r } = require('rethinkdb-ts');
const pTimeout = require('p-timeout');
const pkg = require('./package.json');

const register = async (server, options) => {
    const queryTimeoutMs = options.queryTimeout * 1000 || 5000;
    options.pingInterval = options.pingInterval || 1;

    let conn;
    const reconnect = async () => {
        if (options.useConnPool) {
            conn = await r.connectPool(options);
        }
        else {
            try {
                conn = await r.connect(options);
            }
            catch (err) {
                console.error(err.cause);
                throw err;
            }
        }
    };
    // Hapi Inject doesn't wait for plugins to load nor sends 'start' event, so we try to connect
    await reconnect();

    server.events.on('start', async () => {
        reconnect();
    });

    server.decorate('server', 'r', r);

    server.decorate('server', 'db', async (query) => {
        let result;

        if (!conn.isHealthy) {
            await reconnect();
        }
        try {
            result = pTimeout(
                (() => {
                    if (options.useConnPool) {
                        return query.run();
                    }
                    else {
                        return r.expr(query).run(conn);
                    }
                })(), queryTimeoutMs);
        }
        catch (err) {
            if (err.name === 'TimeoutError') {
                throw new Error('Timeout exceeded for query');
            }
            else {
                throw err;
            }
        }

        return result;
    });

    server.events.on('stop', () => {
        if (options.useConnPool) {
            r.getPoolMaster().drain();
        }
        else {
            conn.close();
        }
    });
};

module.exports.plugin = {
    register,
    pkg
};

module.exports.r = r;
