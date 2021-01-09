## Usage

After installing the package, a `sweeper` command should be available.

This will run the `mypipeline` pipeline:

```bash
sweeper run mypipeline
```

```bash
$ sweeper run -h
usage: sweeper run [-h] [--config CONFIG] [--quiet] job

positional arguments:
  job                   name of the job section in jobs.toml

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG, -c CONFIG
  --quiet, -q
```

## Concepts

### Pipelines

A pipeline is a set of orchestrated operations around data files pertaining to a specific "business" use case. What you probably want to do with this library is to write your own pipelines. For example, you might want to take some files from a HTTP Server and push them to [data.gouv.fr] and/or scp them somewhere else.

A pipeline is just standard python code organized in a class. Default pipelines are defined in `sweeper.pipelines`. All pipelines should inherit from `sweeper.pipelines.base.BasePipeline`.

A pipeline is run through `sweeper.run` (cf [usage]).

### Gateways

A gateway is a way to move a file to or from a specific protocol or platform. Default gateways are defined in `sweeper.gateways`. They include:

- `sweeper.gateways.http.HTTPDownloadGateway`: download a file from an HTTP server, supporting authentication
- `sweeper.gateways.ssh.SSHGateway`: upload a file to a SSH server (uses SFTP)
- `sweeper.gateways.s3.S3Gateway`: upload a file to a S3 compatbile server
- `sweeper.gateways.datagouvfr.DataGouvFrGateway`: upload a file or update a resource to [data.gouv.fr].

### Configuration

Configuration is done through `jobs.toml` file.

```toml
[main]
# Where temporary files will be stored (eg when downloading from an HTTP server).
# This directory should be cleaned up by the script after each run.
tmp_dir = "/tmp/data-gw"

# Pipeline definition
[mypipeline]
# `module:class` path to the pipeline, this can be outside of sweeper
backend = "sweeper.pipelines.mypipeline:MyPipeline"

# Pipeline arbitrary configuration (eg anything you want/need)
# This will be available on your pipeline class as `self.config`
[mypipeline.config]
foo = bar

# Secrets are automatically fetched from env vars
# They will be available in `self.secrets` in your pipeline class
[mypipeline.secrets]
secret_value = "SECRET_ENV_VAR"
```

## Write your own pipeline

1. Write your class inheriting from `sweeper.pipelines.base.BasePipeline`, the only required method is `run`. Obviously you should use some gateways or this library is not very useful :-).
2. Declare your pipeline as seen in [configuration](#configuration).
3. Run with `sweeper run mypipeline`.

## Job run history

The script writes its operations in a database. By default it's a `jobs.db` SQLite file database, this can be configured through `DATABASE_URL` to anything that works with SQLAlchemy.

### Metadata

The `metadata` table stores every `sweeper run` iteration, logging the error if any.

### Pipeline tables

Each pipeline also has its dedicated table, based on its `name` attribute. `sweeper.pipelines.base.BasePipeline.register_file` and `sweeper.pipelines.base.BasePipeline.register_error` will write rows into this table when called from a pipeline. `sweeper.pipelines.base.BasePipeline.file_has_changed` uses the info stored from `sweeper.pipelines.base.BasePipeline.register_file` to check if a file has changed since last run.


[data.gouv.fr]: https://www.data.gouv.fr
[usage]: #usage
