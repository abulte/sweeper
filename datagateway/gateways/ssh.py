from paramiko import SSHClient, SFTPClient


class SSHGateway():

    def __init__(self, host, username="root"):
        self.ssh = SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(host, username=username)
        self.sftp = SFTPClient.from_transport(self.ssh.get_transport())

    def upload(self, local, remote):
        # TODO: plug callback func(int, int)) that accepts the bytes transferred
        # so far and the total bytes to be transferred
        # and progress bar \o/
        self.sftp.put(local, remote)

    def teardown(self):
        self.sftp.close()
        self.ssh.close()
