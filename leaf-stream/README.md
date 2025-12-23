# Leaf Stream

A Leaf stream is a managed event-sourcing database built on SQLite, intended to serve a group or community.

Events sent to the stream are stored persistently in the `stream_db`, which is like an append-only log (not strictly guaranteed at this point).

Access to the stream is managed by a stateful outer layer, the `module`. The module consists of SQL code, which is used to:
1. `materializer`: materialise views over the stream data (expected to be deterministic over the sequence of stream events)
2. `authorizer`: to authorise or unauthorise writes to the stream
3. `queries`: to serve the result of named SQL queries, which can select from either the `module_db` or the `stream_db`. These can use params passed in at query time.

There is also some SQL code `initSql` for setting up tables internal to the module, so it can use these when processing incoming events.

Together, these four sets of SQL statements make up the entire configuration for a **module**, defining the behaviour of a stream. 

Because the module's state is expected to be deterministic over the stream sequence, we can tear down, change and recreate modules when needed. 

## Configuring Modules

The stream always has the same schema. It's fairly simple and can be found in `./src`.

Module code can be uploaded to the server using the Typescript client's `uploadModule()` method. The server will return a content ID (CID) which corresponds to the hash of the code. You can also check if the module corresponding to the CID already exists on the server with `hasModule()`. When creating a new stream, you pass in this content ID.

### Context

The module's SQL execution context has access to *both* tables in the `stream_db` and module-defined tables in the `module_db`, without needing a database name prefix where there is no ambiguity.

For `materializer` and `authorizer` SQL, both have access to a temporary table `event` containing the incoming event, which they can query from.

### Utility functions

Module SQL has access to a few utility user defined functions defined in `./src/module/basic.rs`:
- `throw` - aborts sql execution in the case of an error
- `unauthorized` - same as 'throw', but labelled as 'unauthorized' for transparency to the client that they didn't meet intended, module-defined access requirements
- `drisl_extract` - select properties from DRISL-encoded data, similar to SQLite's `json_extract`

### Queries

Queries are how clients read data. Queries can be simple, as in the case of single-row tables:

```ts
queries: [
    {
        name: "stream_info",
        sql: `select * from stream_info;`,
        params: [],
    },
]
````

In many cases, it will make sense for queries to accept params at query time. These can be defined explicitly in the module:

```ts
queries: [
    {
        name: 'example',
        sql: `select * from users where name = $name`,
        params: [
            {
                kind: 'text',
                name: 'name',
                optional: false
            }
        ]
    }
]
```

All queries are also passed some implicit params: `$start`, `$limit`, and `$requesting_user`. 

`$start` is the stream index from which to begin querying