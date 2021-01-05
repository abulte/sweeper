import importlib

import toml
import locale
from minicli import cli, run as clirun, wrap

from sweeper import close_db
from sweeper.backends.metadata import MetadataBackend

locale.setlocale(locale.LC_TIME, "fr_FR")


@cli
def run(job, config="jobs.toml"):
    """Run a job sync

    :job: name of the job section in jobs.toml
    """
    with open(config) as cfile:
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
    meta_error = None
    run_errors = None
    try:
        _class.pre_run()
        run_errors = _class.run()
    except KeyboardInterrupt:
        meta_error = 'Cancelled by user'
    except Exception as e:
        meta_error = e
        raise e
    finally:
        try:
            _class.post_run()
        finally:
            metadata.end(meta_error, run_errors)
            _class._teardown()


@wrap
def close_db_after():
    yield
    close_db()


if __name__ == "__main__":
    clirun()
