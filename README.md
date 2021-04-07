<p align="center">
  <img src="https://raw.githubusercontent.com/silentsokolov/dbt-clickhouse/master/etc/dbt-logo-full.svg" alt="dbt logo" width="300"/>
</p>

[![Tests](https://github.com/emacha/dbt-oracle-adapter/actions/workflows/tests.yaml/badge.svg)](https://github.com/emacha/dbt-oracle-adapter/actions/workflows/tests.yaml)

# dbt-oracle-adapter

This plugin ports [dbt](https://getdbt.com) functionality to [Oracle](https://www.oracle.com/database/).

This is only tested against Oracle XE-18c and dbt 0.19

### Installation

`pip install dbt-oracledb` :cake:

### Supported features

- [x] Table materialization
- [x] View materialization
- [x] Ephemeral materialization
- [x] Tests
- [x] Documentation
- [x] Sources
- [x] Seeds
- [x] Snapshots
- [ ] Incremental materialization

# Example Profile

```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: oracle
      username: [username/schema]
      password: [1234]
      host: [localhost]
      database: [xepdb1]
      schema: [username/schema]

      # optional
      port: [port]  # default 1521
```
# Development

### Setting up

The package was developed with [poetry](https://python-poetry.org/),
so start by installing that in your system.

`cd` into the root of the cloned repo and run `poetry install`
to install the package in editable mode with all dev dependencies.

We need an Oracle database running to do anything with the adapter.
Install [docker](https://docs.docker.com/engine/install/)
and start a test database with:

`docker run -d -p 1521:1521 silverax/dbt-oracle-adapter:oracle-18.4.0-xe`

This will take some time to finish setting up (~10 minutes),
you can check the progress with `docker ps`
and wait for the status to go from `starting`
to `healthy`.

> **NOTE**:
>
> That's an Oracle 18.4.0 XE database with user `SYSTEM` and
> password `oracle`. The image is a *very slightly* modified
> version taken from the original Oracle [repo](https://github.com/oracle/docker-images/tree/main/OracleDatabase/SingleInstance)
> I've built and uploaded to docker-hub

Run the tests with `pytest specs/oracle.dbtspec`

This is using the [dbt-adapter-tests](https://github.com/fishtown-analytics/dbt-adapter-tests) plugin. Most sequences
are passing, and failing ones are commented out.

Of the failing ones. The incremental materialization is
not implemented. While the rest I suspect are failing because
of the plugin is hardcoding some queries. Running that
capability on `test_project` works fine. 

### TODO / Adapter issues
- The `alter_column_type` macro is not implemented
- `varchar2` columns are created with hardcoded precisions
- Oracle does not distinguish between *schemas* and *users*,
so any new schemas are created with the `1234` password.
I probably need to add more options to schema creation
and set the default password as the same as the logged in user.
- Connection to db is passing username/password/database 
parameters directly to cx_oracle connect. I think there
should be another optional parameter `easy_connect_string`
that supersedes any others.
- There's some issues leftover with quoting. So table searching
is calling upper on any inputs. So to be safe try disabling
quoting and don't mix uppercase with lowercase in the same
table/column name.
