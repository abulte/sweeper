import importlib

import toml
from minicli import cli, run as clirun

from datagateway.backends.metadata import MetadataBackend


@cli
def run(job):
    """Run a job sync

    :job: name of the job section in jobs.toml
    """
    with open("jobs.toml") as cfile:
        config = toml.loads(cfile.read())
    main_config = config.get('main', {})
    if job not in config:
        print(f"Unknown job {job}")
        return
    jobconf = config[job]

    _mod, _class = jobconf["backend"].split(':')
    _mod = importlib.import_module(_mod)
    _class = getattr(_mod, _class)(
        main_config,
        jobconf.get('config', {}),
        jobconf.get('secrets', {})
    )

    metadata = MetadataBackend(main_config, {}, {})
    metadata.start(job)
    error = None
    try:
        _class.pre_run()
        _class.run()
    except KeyboardInterrupt:
        error = 'Cancelled by user'
    except Exception as e:
        error = e
        raise e
    finally:
        _class.post_run()
        metadata.end(error)


if __name__ == "__main__":
    clirun()
