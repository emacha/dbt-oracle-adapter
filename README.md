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

# TODO:
- Implement alter column type macro
- A decent*er* readme
- Do something about the hardcoded Varchar2 precisions?
- Improve the schema creation. Maybe set the password
  and permissions the same as creator?

