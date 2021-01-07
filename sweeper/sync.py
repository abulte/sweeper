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

    metadata = Metadata()
    metadata.start(job)

    job = getattr(_mod, _class)(
        metadata.id,
        main_config,
        jobconf.get('config', {}),
        jobconf.get('secrets', {})
    )

    main_error = None
    try:
        job.pre_run()
        job.run()
    except KeyboardInterrupt:
        main_error = 'Cancelled by user'
    except Exception as e:
        main_error = e
        raise e
    finally:
        try:
            job.post_run()
        finally:
            metadata.end(main_error, job.errors)
            job._teardown()


@wrap
def close_db_after():
    yield
    close_db()


if __name__ == "__main__":
    clirun()
