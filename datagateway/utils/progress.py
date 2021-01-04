import sys
from datetime import timedelta

from progressist import ProgressBar as ProgressistProgressBar


class ProgressBar(ProgressistProgressBar):
    """Wrapper around progressist.ProgressBar handling isatty()

    It will automatically throttle the output and get rid of
    carriage returns, replacing them with line breaks for a
    better experience in log files.
    """

    def __init__(self, **kwargs):
        self.is_terminal = sys.stdout.isatty()
        if not self.is_terminal:
            throttle = timedelta(seconds=60)
            kwargs["throttle"] = throttle
        super().__init__(**kwargs)
        self.template = self.template.replace('\r', '') + '\n'
