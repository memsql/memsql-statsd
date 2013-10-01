/*
 * This module provides a connection pool over all the aggregators in a
 * MemSQL cluster.  It needs to know the master-aggregator which it will
 * use to look up the rest of the aggregators.  It handles automatic
 * connection retrying and aggregator failover.
 */

var generic_pool = require('generic-pool');
var mapper = require('mapper');

var MysqlClient = mapper.constructor.Client;

var AggregatorsPool = module.exports = function(logger, master_host, master_port, master_user, master_pass) {
    this._logger = logger;
    this._master = { host: master_host, port: master_port, user: master_user, password: master_pass };
    this._aggregator_cache = [];

    this._pool = this._create_pool();
    this._refresh_interval = setInterval(this._refresh_aggregators.bind(this), 60 * 1000);
};

AggregatorsPool.prototype.acquire = function(callback) { this._pool.acquire(callback); };
AggregatorsPool.prototype.release = function(connection) { this._pool.release(connection); };
AggregatorsPool.prototype.destroy = function(connection) { this._pool.destroy(connection); };

AggregatorsPool.prototype.close = function() {
    clearInterval(this._refresh_interval);

    // drain the pool
    var pool = this._pool;
    pool.drain(function() { pool.destroyAllNow(); });
};

AggregatorsPool.prototype._reset = function() {
    // resets the pool to an initial state
    this._aggregator_cache = [];
    this._pool.destroyAllNow();
};

AggregatorsPool.prototype._create_pool = function() {
    return generic_pool.Pool({
        name: 'mysql',
        create: this._connection_create.bind(this),
        destroy: this._connection_destroy.bind(this),
        max: 5,
        idleTimeoutMillis: 10 * 1000
    });
};

AggregatorsPool.prototype._connect_aggregator = function(aggregator) {
    this._logger.info('MemSQL: Connecting to ' + aggregator.host + ':' + aggregator.port);

    var conn = new MysqlClient(aggregator);
    conn.on('error', this.destroy.bind(this));

    conn.connect();

    if (!conn.connected) {
        return false;
    } else {
        return conn;
    }
};

AggregatorsPool.prototype._connection_create = function(callback) {
    var connect = function() {
        var aggregator = this._aggregator_cache[Math.floor(Math.random() * this._aggregator_cache.length)];

        var conn = this._connect_aggregator(aggregator);
        if (conn) { callback(null, conn); }
        else { callback(new Error('Failed to connect to aggregator ' + aggregator.host + ":" + aggregator.port)); }
    }.bind(this);

    if (this._aggregator_cache.length === 0) {
        // need at least one aggregator to connect to
        this._refresh_aggregators(function(err) {
            if (err) { callback(err); }
            else { connect(); }
        });
    } else {
        connect();
    }
};

AggregatorsPool.prototype._connection_destroy = function(connection) {
    if (connection.connected) {
        connection.disconnect();
    }
};

AggregatorsPool.prototype._refresh_aggregators = function(callback) {
    // Try to get an existing aggregator connection, if it fails fall
    // back to trying to get a new master connection.
    var update = function(conn, cleanup) {
        conn.all('SHOW AGGREGATORS;', function(err, rows) {
            if (err) {
                if (cleanup) { conn.disconnect(); }
                if (callback) { callback(new Error('Failed to get aggregator list:' + err)); }
                return this._reset();
            }

            if (err) {
                if (cleanup) { conn.disconnect(); }
                if (callback) { callback(new Error('Failed to get aggregator list:' + err)); }
                return this._reset();
            }

            this._aggregator_cache = rows.map(function(row) {
                if (row.Host === '127.0.0.1') { row.Host = conn.config.host; }
                return {
                    host: row.Host,
                    port: row.Port,
                    user: this._master.user,
                    password: this._master.password
                };
            }.bind(this));

            if (cleanup) { conn.disconnect(); }
            if (callback) { callback(null); }

        }.bind(this));
    }.bind(this);

    if (this._aggregator_cache.length === 0) {
        var conn = this._connect_aggregator(this._master);
        if (conn) { update(conn, true); }
        else { callback(new Error('Failed to connect to master @ ' + this._master.host + ":" + this._master.port)); }
    } else {
        this.acquire(function(err, conn) {
            if (err) {
                // fall back to getting a new master connection
                conn = this._connect_aggregator(this._master);
                update(conn, true);
            } else {
                update(conn);
            }
        }.bind(this));
    }
};
