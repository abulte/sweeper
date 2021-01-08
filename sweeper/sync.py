import importlib
import locale

import coloredlogs
from minicli import cli, run as clirun, wrap

from sweeper import close_db
from sweeper.utils.config import load as load_config
from sweeper.utils.metadata import Metadata

locale.setlocale(locale.LC_TIME, "fr_FR")


@cli
def run(job, config="jobs.toml", quiet=False):
    """Run a job sync

    :job: name of the job section in jobs.toml
    """
    level = "DEBUG" if not quiet else "INFO"
    coloredlogs.install(level=level, fmt="%(asctime)s %(name)s %(levelname)s %(message)s")

    config = load_config(config, job)
    _mod, _class = config[job]["backend"].split(':')
    _mod = importlib.import_module(_mod)

    metadata = Metadata()
    metadata.start(job)

    job = getattr(_mod, _class)(metadata.id, config)

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
def setup():
    yield
    close_db()


if __name__ == "__main__":
    clirun()
