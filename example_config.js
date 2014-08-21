{
    port: 8125,
    flushInterval: 1000,
    backends: [ "memsql-statsd" ],
    debug: true,
    memsql: {
        prefix: "stats",
        host: "MASTER_AGGREGATOR_HOSTNAME",
        port: 3306,
        user: "root",
        password: "",
        database: "dashboard",
        whitelist: [],
        blacklist: []
    },
}
