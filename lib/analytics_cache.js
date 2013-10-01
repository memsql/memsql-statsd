/*
 * This module contains the AnalyticsCache class which caches and
 * flushes data to MemSQL Ops.
 */

var _ = require('lodash');
var util = require('util');

var CLASSIFIERS = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota', 'kappa']
var ANALYTICS_COLUMNS = ['classifier', 'value', 'created'].concat(CLASSIFIERS);

var STATEMENT_TEMPLATE = util.format("INSERT INTO `%%s`.analytics (%s) VALUES ", ANALYTICS_COLUMNS.join(','));

// classifier, value, timestamp, alpha, beta, gamma, delta... rest of classifiers
var VALUES_PLACEHOLDER = '(?,?,?,?,?,?,?,?,?,?,?,?,?)';

var AnalyticsCache = function(logger, database_name, connection_pool) {
    this._logger = logger;
    this._database_name = database_name;
    this._pool = connection_pool;
    this._pending = [];
};

AnalyticsCache.prototype.record = function(key, value, timestamp) {
    var classifiers = key.split('.');

    // if there are more classifiers than classifier columns,
    // lets just throw the remainder in the last classifier column
    if (classifiers.length > CLASSIFIERS.length) {
        var extra = classifiers.splice(CLASSIFIERS.length - 1);
        classifiers[classifiers.length - 1] = extra.join('.');
    } else if (classifiers.length < CLASSIFIERS.length - 1) {
        var diff = CLASSIFIERS.length - classifiers.length;
        classifiers = classifiers.concat(new Array(diff + 1).join(' ').split('').map(function() { return ''; }));
    }

    this._pending.push([key, value, new Date(timestamp * 1000).toISOString()].concat(classifiers));
};

AnalyticsCache.prototype.flush = _.throttle(function(callback) {
    var statement_template = util.format(STATEMENT_TEMPLATE, this._database_name);

    this._pool.acquire(function(err, connection) {
        if (err) {
            return callback(err);
        }

        while (this._pending.length > 0) {
            var batch = this._pending.splice(0, 50);

            // build values array
            var statement_values = [];
            var values = [];
            for (var i = 0, l = batch.length; i < l; ++i) {
                values = values.concat(batch[i]);
                statement_values.push(VALUES_PLACEHOLDER);
            }


            var statement = statement_template + statement_values.join(',');
            connection.exec(statement, values, function(err, result) {
                if (err) { return callback(err); }
                else {
                    this._pool.release(connection);
                    callback(null, batch.length);
                }
            }.bind(this));
        }
    }.bind(this));
}, 1000);

module.exports = AnalyticsCache;
