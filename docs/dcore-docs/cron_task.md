## Cron Tasks

This component provides a Quartz-backed scheduler for config-defined cron tasks.
It is designed to mirror the “handler + components” pattern used across D-Core.

### Component config (Integrant/Duct)

```clj
{:d-core.libs.cron-task/scheduler
 {:handlers {:cleanup #ig/ref :my-app.handlers/cleanup
             :sync #ig/ref :my-app.handlers/sync}
  :deps {:db #ig/ref :d-core.core.clients/postgres/client}
  :tasks {:cleanup {:cron "0 0 * * * ?"
                    :handler :cleanup
                    :payload {:limit 100}
                    :timezone "UTC"
                    :enabled true}
          :sync {:cron "0 */5 * * * ?"
                 :handler :sync
                 :payload {:scope :daily}}}
  :sync-mode :replace
  :start? true
  :shutdown-wait? true
  :quartz {:properties {"org.quartz.threadPool.threadCount" "4"}}}}
```

### Task shape

Required:
- `:id` (keyword or string) when tasks are provided as a vector.
- `:cron` Quartz cron expression (seconds field required).
- `:handler` registered handler id.

Optional:
- `:payload` data passed to the handler.
- `:timezone` string/keyword/TimeZone (defaults to JVM default).
- `:enabled` boolean (default true).
- `:misfire` one of `:fire-now`, `:do-nothing`, `:ignore-misfires`.
- `:description` string (stored on job/trigger).

### Handler contract

Handlers receive a single map:

```clj
{:task-id :cleanup
 :handler :cleanup
 :payload {:limit 100}
 :deps {:db ...}
 :context <JobExecutionContext>
 :fire-time <java.util.Date>
 :scheduled-fire-time <java.util.Date>
 :next-fire-time <java.util.Date>
 :previous-fire-time <java.util.Date>}
```

### Sync modes

- `:replace` (default): removes scheduled jobs not present in config.
- `:merge` (or any non-`:replace` value): only upserts configured tasks.

### Clustering / persistence

To enable clustered, durable scheduling, pass Quartz properties for JDBCJobStore.
Example for Postgres + HikariCP:

```clj
{:d-core.libs.cron-task/scheduler
 {:quartz {:properties {"org.quartz.scheduler.instanceName" "d-core-cron"
                        "org.quartz.scheduler.instanceId" "AUTO"
                        "org.quartz.threadPool.threadCount" "5"
                        "org.quartz.jobStore.class" "org.quartz.impl.jdbcjobstore.JobStoreTX"
                        "org.quartz.jobStore.isClustered" "true"
                        "org.quartz.jobStore.driverDelegateClass" "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate"
                        "org.quartz.jobStore.dataSource" "main"
                        "org.quartz.dataSource.main.connectionProvider.class" "org.quartz.utils.HikariCpPoolingConnectionProvider"
                        "org.quartz.dataSource.main.driver" "org.postgresql.Driver"
                        "org.quartz.dataSource.main.URL" "jdbc:postgresql://localhost:5432/d-core"
                        "org.quartz.dataSource.main.user" "postgres"
                        "org.quartz.dataSource.main.password" "postgres"
                        "org.quartz.dataSource.main.maxConnections" "5"}}}}
```

When using JDBCJobStore, values stored in `:payload` must be serializable.
You also need to install the Quartz tables for your database (see the Quartz
`tables_*.sql` scripts for your DB vendor).

### Runtime operations

These helpers are available for API/CLI integration:
- `upsert-task!`
- `delete-task!`
- `pause-task!`
- `resume-task!`
- `sync-tasks!`
- `list-task-ids`
