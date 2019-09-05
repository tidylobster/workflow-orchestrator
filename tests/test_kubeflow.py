import wo, os, shutil, pytest, json
import random, logging, urllib.parse

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Tests 
# -----

@pytest.fixture
def w():
    yield wo.Orchestrator(dev=True)


def test_export_outputs_json(w):
    outputs = {
        "mlpipeline-ui-metadata.json": { 
            'outputs': [
                {
                    'type': 'tensorboard',
                    'source': "s3://bucket/somewhere-in-my-heart/",
                },
            ]
        },
    }
    w.log_execution(outputs=outputs)
    with open("mlpipeline-ui-metadata.json", "r") as file:
        dumped = json.load(file)
    assert outputs["mlpipeline-ui-metadata.json"] == dumped
    os.remove("mlpipeline-ui-metadata.json")


def test_export_outputs_other(w):
    outputs = {
        "random": random.randint(0, 1000000)
    }
    w.log_execution(outputs=outputs)
    with open("random", "r") as file:
        value = int(file.read())
    assert outputs["random"] == value
    os.remove("random")