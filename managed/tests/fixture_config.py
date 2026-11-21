from unittest import addModuleCleanup
from unittest.mock import patch

from managed.location import example_config


def test_config():
    config = example_config()
    config.update(paused=False, kinds={kind: kind != 'daily' for kind in config['kinds']})
    config['automation'].update(enabled=True, automatic_mail=True)
    config['pipeline']['enabled'] = True
    config['mailbox_adapter'] = 'microsoft-graph'
    return config


def use_example_config():
    mocked = patch('managed.location.load_config', side_effect=lambda *args, **kwargs: example_config())
    mocked.start()
    addModuleCleanup(mocked.stop)


def use_cli_config():
    mocked = patch('managed.control.load_config', side_effect=lambda *args, **kwargs: test_config())
    mocked.start()
    addModuleCleanup(mocked.stop)
