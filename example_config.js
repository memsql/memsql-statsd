{
    port: 8125,
    flushInterval: 1000,
    backends: [ "memsql-statsd" ],
    debug: true,
    memsql: {
        host: "MASTER_AGGREGATOR_HOSTNAME",
        port: 3306,
        user: "root",
        password: "",
        database: "dashboard"
    }
}
