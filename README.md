<p align="center">
  <img src="https://raw.githubusercontent.com/silentsokolov/dbt-clickhouse/master/etc/dbt-logo-full.svg" alt="dbt logo" width="300"/>
</p>

# dbt-oracle-adapter

This plugin ports [dbt](https://getdbt.com) functionality to [Oracle](https://www.oracle.com/database/).

This is only tested against Oracle XE-18c and dbt 0.19

### Installation

Not deployed yet!

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
- stick it in github
    - Use their docker thing for the test db?
    - CI/CD with actions
- Do something about the hardcoded Varchar2 precisions?
