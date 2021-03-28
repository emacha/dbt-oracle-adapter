# Oracle DBT Adapter

This is an attempt to implement a dbt adapter for Oracle from
scratch. Half as a learning exercise, half so I can have 
a nicer workflow at my job.

I don't think I'll ever implement all features,
but I want to at least reach a point where it's usable.
Without being more trouble than it's worth.

TODO:
- Macros in `not_implemented.sql`
- Keep making the dbtspec tests to pass
- Now we need to sort swappable from `base`. Failing on rename_relation from view to table or vs
- data_test_ephemeral_models is getting recursive CTEs? Test a couple of layers in test_project

- A decent readme
- stick it in github
    - Use their docker thing for the test db?
    - CI/CD with actions
- Rename all the terrible placeholder names (noracle)
- incremental model probably hard to implement, leave for later.
