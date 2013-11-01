/*
 * This module contains the AnalyticsCache class which caches and
 * flushes data to MemSQL Ops.
 */

var _ = require('lodash');
var util = require('util');
var crypto = require('crypto');

var CLASSIFIERS = ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota', 'kappa', 'lambda', 'mu', 'nu', 'xi', 'omicron']
var CLASSIFIERS_COLUMNS = ['id', 'classifier'].concat(CLASSIFIERS);

var ANALYTICS_INSERT = "INSERT INTO `%s`.analytics (classifier_id, value, created) VALUES ";
var CLASSIFIERS_INSERT = util.format("INSERT INTO `%%s`.classifiers (%s) VALUES %%%%s ON DUPLICATE KEY UPDATE id=id", CLASSIFIERS_COLUMNS.join(','));

var ANALYTICS_VALUES_PLACEHOLDER = '(?,?,?)';

// classifier, value, timestamp, alpha, beta, gamma, delta... rest of classifiers
var CLASSIFIER_VALUES_PLACEHOLDER = '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';

var AnalyticsRow = function(joined_classifier, value, timestamp) {
    this.joined_classifier = joined_classifier;
    this.classifiers = this._get_classifiers(joined_classifier);
    this.classifier_id = this._compute_id(this.classifiers);
    this.value = value;
    this.timestamp = timestamp;
};

AnalyticsRow.prototype.classifier_values = function() {
    return [this.classifier_id, this.joined_classifier].concat(this.classifiers);
};

AnalyticsRow.prototype.analytics_values = function() {
    return [this.classifier_id, this.value, this.timestamp];
};

AnalyticsRow.prototype._get_classifiers = function(joined_classifier) {
    var classifiers = joined_classifier.split('.');

    // if there are more classifiers than classifier columns,
    // lets just throw the remainder in the last classifier column
    if (classifiers.length > CLASSIFIERS.length) {
        var extra = classifiers.splice(CLASSIFIERS.length - 1);
        classifiers[classifiers.length] = extra.join('.');
    } else if (classifiers.length < CLASSIFIERS.length) {
        var diff = CLASSIFIERS.length - classifiers.length;
        classifiers = classifiers.concat(new Array(diff + 1).join(' ').split('').map(function() { return ''; }));
    }

    return classifiers;
};

AnalyticsRow.prototype._compute_id = function(classifiers) {
    var shasum = crypto.createHash('sha1');
    shasum.update(classifiers.join('.'));
    return parseInt(shasum.digest('hex').slice(0, 16), 16);
};

var AnalyticsCache = function(logger, database_name, connection_pool, prefix) {
    this._logger = logger;
    this._database_name = database_name;
    this._pool = connection_pool;
    this._prefix = _.isUndefined(prefix) ? prefix : prefix + '.';
    this._pending = [];
    this._seen_classifiers = [];
};

AnalyticsCache.prototype.record = function(key, value, timestamp) {
    if (!_.isUndefined(this._prefix)) { key = this._prefix + key; }
    this._pending.push(new AnalyticsRow(key, value, new Date(timestamp * 1000).toISOString()));
};

AnalyticsCache.prototype.flush = _.throttle(function(callback) {
    var statement_template = util.format(ANALYTICS_INSERT, this._database_name);

    var queries = [];
    var flushed_num = this._pending.length;

    this._throttled_classifiers_reset();
    this._record_classifiers(this._pending);

    // build queries
    while (this._pending.length > 0) {
        var batch = this._pending.splice(0, 128);

        // build values array
        var statement_values = [];
        var values = [];
        for (var i = 0, l = batch.length; i < l; ++i) {
            var row = batch[i];
            values.push.apply(values, row.analytics_values());
            statement_values.push(ANALYTICS_VALUES_PLACEHOLDER);
        }

        var statement = statement_template + statement_values.join(',');
        queries = queries.concat([statement, values]);
    }

    // execute queries in parallel
    this._pool.acquire(function(err, connection) {
        if (err) {
            return callback(err);
        }

        queries.push(function(err, result) {
            if (err) {
                this._pool.destroy(connection);
                callback(err);
            } else {
                this._pool.release(connection);
                callback(null, flushed_num);
            }
        }.bind(this));

        try {
            connection.execSeries.apply(connection, queries);
        } catch (e) {
            // connection error occured
            this._logger.debug('Error while inserting analytics data.');
            this._pool.destroy(connection);
        }
    }.bind(this));
}, 1000);

AnalyticsCache.prototype._throttled_classifiers_reset = _.throttle(function() {
    this._seen_classifiers = [];
}, 1000 * 5 * 60);

AnalyticsCache.prototype._record_classifiers = function(rows) {
    var pending = [];

    for (var i = 0, l = rows.length; i < l; ++i) {
        var row = rows[i];
        var index = _.sortedIndex(this._seen_classifiers, row.classifier_id);
        if (this._seen_classifiers[index] !== row.classifier_id) {
            // new classifier
            this._seen_classifiers.splice(index, 0, row.classifier_id);
            pending.push(row);
        }
    }

    if (pending.length > 0) {
        var statement_template = util.format(CLASSIFIERS_INSERT, this._database_name);
        var queries = [];

        while (pending.length > 0) {
            var batch = pending.splice(0, 64);

            // build values array
            var statement_values = [];
            var values = [];
            for (var i = 0, l = batch.length; i < l; ++i) {
                var row = batch[i];
                values.push.apply(values, row.classifier_values());
                statement_values.push(CLASSIFIER_VALUES_PLACEHOLDER);
            }

            var statement = util.format(statement_template, statement_values.join(','));
            queries = queries.concat([statement, values]);
        }

        var conn = this._pool.connect_master();
        queries.push(function(err, result) {
            this._pool.connection_destroy(conn);
        }.bind(this));

        try {
            conn.execSeries.apply(conn, queries);
        } catch (e) {
            // connection error occured
            this._logger.debug('Error while inserting classifiers.');
            this._pool.connection_destroy(conn);
        }
    }
};

module.exports = AnalyticsCache;
