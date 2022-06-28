# Snowflake Writer Migration

[![Build Status](https://travis-ci.com/keboola/app-snowflake-writer-migrate.svg?branch=master)](https://travis-ci.com/keboola/app-snowflake-writer-migrate)

Application for migrating Snowflake writers between project's and regions.
It migrates all Snowflake writers in source project into project where the application is executed.

- It migrates whole Snowflake writer configuration. Ids of configurations are preserved.
- If writer is provisioned by Keboola new workspace is created in destination project for writer
- For customer writers configuration is only copied
- Data are not migrated, writer have to be manually executed


# Usage

> fill in usage instructions

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/my-component
cd my-component
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 

## License

MIT licensed, see [LICENSE](./LICENSE) file.
