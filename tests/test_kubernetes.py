import wo, os, shutil, pytest, json
import random, logging, urllib.parse

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Tests 
# -----

@pytest.fixture
def w():
    yield wo.Orchestrator(
        default_params={"uri.google": "https://www.google.com"}, dev=True)


def test_read_config_map_default(w):
    logger.info(w.get_config())
    assert w.get_config()["uri.google"] == "https://www.google.com"


def test_read_config_map(w): 
    os.makedirs("mount")
    with open("mount/uri.google", "w+") as file:
        file.write("https://google.com")
    assert w.get_config(mount_path="mount")["uri.google"] == "https://google.com"
    shutil.rmtree("mount")