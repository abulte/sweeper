import logging

from paramiko import SSHClient, SFTPClient

from sweeper.utils.progress import ProgressBar

log = logging.getLogger(__name__)


class SSHGateway():

    def __init__(self, host, username="root"):
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(host, username=username)
        self.host = host
        transport = self.ssh.get_transport()
        if transport is not None:
            self.sftp = SFTPClient.from_transport(transport)
        else:
            raise Exception("Failed to get ssh transport")

    def upload(self, local, remote):
        bar = ProgressBar(
            template="|{animation}| {done:B}/{total:B} ({speed:B}/s)",
        )

        def cb(done, total):
            bar.update(done=done, total=total)

        log.info(f"Uploading {local} to {self.host}:{remote}...")
        self.sftp.put(local, remote, callback=cb)

    def teardown(self):
        self.sftp.close()
        self.ssh.close()
