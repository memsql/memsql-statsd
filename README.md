# MemSQL StatsD Backend

This is a pluggable backend for [StatsD][statsd], which sends stats to [MemSQL Ops][ops].

## Requirements

 * [StatsD][statsd] versions >= 0.3.0.
 * A [MemSQL Ops][ops] server.

## Installation

    $ cd /path/to/statsd
    $ npm install statsd-instrumental-backend

## Configuration

You have to add the following basic configuration information to your
StatsD config file.

```js
{
    memsql: {
        host: "<MASTER AGGREGATOR IP/HOSTNAME>",
        port: <MASTER AGGREGATOR PORT>,
        user: "<MASTER AGGREGATOR USERNAME>",
        password: "<MASTER AGGREGATOR PASSWORD>",
        database: "<MEMSQL OPS DATABASE NAME>"
    }
}
```

## Enabling

Add `memsql-statsd` backend to the list of StatsD
backends in the StatsD configuration file:

```js
{
    backends: ["memsql-statsd"]
}
```

Start/restart the statsd daemon and your StatsD metrics should now be
pushed to your MemSQL Ops dashboard.

## NPM Dependencies

None

## Development

Contributing:

 * Fork the project
 * Make your feature addition or bug fix
 * Commit. Do not mess with package.json, version, or history.
 * Send a pull request. Bonus points for topic branches.

[statsd]: https://github.com/etsy/statsd
[ops]: http://www.memsql.com/features#dashboard
