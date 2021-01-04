import importlib

import toml
import locale
from minicli import cli, run as clirun

from datagateway.backends.metadata import MetadataBackend

locale.setlocale(locale.LC_TIME, "fr_FR")


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

    # FIXME: this is a strange abstraction (cf teardown)
    # maybe the db should be handled as a singleton from here anyway
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
        try:
            _class.post_run()
        finally:
            metadata.end(error)
            _class._teardown()


if __name__ == "__main__":
    clirun()
