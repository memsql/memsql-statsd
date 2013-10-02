/*
 * Flushes stats to MemSQL Ops (http://memsql.com).
 *
 * To enable this backend, include 'memsql-statsd' in the
 * backends configuration array:
 *
 *   backends: ['memsql-statsd']
 *
 * The backend will read the configuration options from the following
 * 'memsql' hash defined in the main statsd config file:
 *
 *  memsql : {
 *      host: "<MASTER AGGREGATOR IP/HOSTNAME>",
 *      port: <MASTER AGGREGATOR PORT>,
 *      user: "<MASTER AGGREGATOR USERNAME>",
 *      password: "<MASTER AGGREGATOR PASSWORD>",
 *      database: "<MEMSQL OPS DATABASE NAME>"
 *  }
 */

var _ = require('lodash');
var util = require('util');
var AnalyticsCache = require('./analytics_cache');
var AggregatorsPool = require('./aggregators_pool.js');
var instance = null;

var Logger = function(debug) { this._debug = debug; };
Logger.prototype.info = function(message) { util.log("MemSQL: " + message); };
Logger.prototype.debug = function(message) {
    if (this._debug) { util.debug("MemSQL: " + message); }
};

var Backend = function(startup_time, config, events) {
    this._process_config(config);

    this.logger = new Logger(!!config.debug);
    this.pool = new AggregatorsPool(this.logger, this.config.host, this.config.port, this.config.user, this.config.password);
    this.cache = new AnalyticsCache(this.logger, this.config.database, this.pool);
    this.stats = {
        exception_count: 0,
        flush_time: -1,
        flush_length: 0
    };

    events.on('flush', (this.flush.bind(this)));
    events.on('status', (this.status.bind(this)));

    return this;
};

Backend.prototype._process_config = function(statsd_config) {
    this.debug = !!statsd_config.debug;

    if (!_.has(statsd_config, 'memsql')) { throw new Error('Missing required MemSQL configuration object.'); }
    this.config = statsd_config.memsql;

    // verify configuration, and set defaults
    if (!_.has(this.config, 'host')) { throw new Error('Missing hostname in MemSQL configuration.'); }
    else if (!_.isString(this.config.host)) { throw new Error('MemSQL hostname must be a string.'); }

    if (!_.has(this.config, 'port')) { this.config.port = 3306; }
    else if (!_.isNumber(this.config.port)) { throw new Error('MemSQL port must be a number.'); }

    if (!_.has(this.config, 'user')) { this.config.user = 'root'; }
    else if (!_.isString(this.config.user)) { throw new Error('MemSQL user must be a string.'); }

    if (!_.has(this.config, 'password')) { this.config.password = ''; }
    else if (!_.isString(this.config.password)) { throw new Error('MemSQL password must be a string.'); }

    if (!_.has(this.config, 'database')) { this.config.database = 'dashboard'; }
    else if (!_.isString(this.config.database)) { throw new Error('MemSQL Ops database name must be a string.'); }
};

Backend.prototype.status = function(write_callback) {
    for (stat in this.stats) {
        if (this.stats[stat]) {
            write_callback(null, 'memsql', stat, this.stats[stat])
        }
    }
};

Backend.prototype.flush = function(timestamp, metrics) {
    var payload = [], key, value;

    // process counters
    for (key in metrics.counters) {
        value = metrics.counters[key];
        var value_per_second = metrics.counter_rates[key];

        this.cache.record([key, 'count'].join('.'), value, timestamp);
        this.cache.record([key, 'rate'].join('.'), value, timestamp);
    }

    // process timing data
    for (key in metrics.timer_data) {
        for (var timer_data_key in metrics.timer_data[key]) {
            value = metrics.timer_data[key][timer_data_key];
            if (typeof(value) === 'number') {
                this.cache.record([key, timer_data_key].join('.'), value, timestamp);
            } else {
                for (var timer_data_sub_key in value) {
                    this.cache.record(
                        [key, timer_data_key, timer_data_sub_key].join('.'),
                        value[timer_data_sub_key],
                        timestamp
                    );
                }
            }
        }
    }

    // process gauges
    for (key in metrics.gauges) {
        this.cache.record(key, metrics.gauges[key], timestamp);
    }

    // process sets
    for (key in metrics.sets) {
        this.cache.record([key, 'count'].join('.'), metrics.sets[key].values().length, timestamp);
    }

    for (key in this.stats) {
        if (this.stats[key]) {
            this.cache.record(['memsql_statsd', key].join('.'), this.stats[key], timestamp);
        }
    }

    var start_time = Date.now();
    this.cache.flush(function(err, num_flushed) {
        var now = Date.now();
        if (err) {
            this.stats.exception_count++;
            this.logger.info(err);
        } else {
            this.stats.flush_time = (Date.now() - start_time);
            this.stats.flush_length = num_flushed;
        }
    }.bind(this));
};

exports.init = function(startup_time, config, events) {
    try {
        instance = new Backend(startup_time, config, events);
        return true;
    } catch (e) {
        console.log(e);
        return false;
    }
};
