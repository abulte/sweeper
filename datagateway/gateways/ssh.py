from paramiko import SSHClient, SFTPClient
from datagateway.utils.progress import ProgressBar


class SSHGateway():

    def __init__(self, host, username="root"):
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(host, username=username)
        self.host = host
        self.sftp = SFTPClient.from_transport(self.ssh.get_transport())

    def upload(self, local, remote):
        bar = ProgressBar(
            template="|{animation}| {done:B}/{total:B} ({speed:B}/s)",
        )

        def cb(done, total):
            bar.update(done=done, total=total)

        print(f"Uploading {local} to {self.host}:{remote}...")
        self.sftp.put(local, remote, callback=cb)

    def teardown(self):
        self.sftp.close()
        self.ssh.close()
