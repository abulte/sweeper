import toml


def load(config_filepath: str, job: str):
    with open(config_filepath) as cfile:
        config = toml.loads(cfile.read())
    assert "main" in config
    assert job in config
    return config
