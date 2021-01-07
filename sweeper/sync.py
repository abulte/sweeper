import importlib

import toml
import locale
from minicli import cli, run as clirun, wrap

from sweeper import close_db
from sweeper.utils.metadata import Metadata

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

    metadata = Metadata()
    metadata.start(job)
    main_error = None
    # TODO: store run_errors in job table instead?
    # makes more sense since it's file by file
    run_errors = None
    try:
        _class.pre_run()
        run_errors = _class.run()
    except KeyboardInterrupt:
        main_error = 'Cancelled by user'
    except Exception as e:
        main_error = e
        raise e
    finally:
        try:
            _class.post_run()
        finally:
            metadata.end(main_error, run_errors)
            _class._teardown()


@wrap
def close_db_after():
    yield
    close_db()


if __name__ == "__main__":
    clirun()
