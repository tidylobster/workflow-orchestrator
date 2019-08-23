import wo, os, shutil, pytest
import random, logging, boto3, urllib.parse

s3 = boto3.resource('s3')
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Helper functions
# ----------------

def with_bucket(path):
    return "/".join([bucket_uri.strip("/"), path.lstrip("/")])


def random_file():
    filename = "random-{}.file".format(random.randint(0, 10000000000000))
    with open(filename, "w+") as file:
        file.write(str(random.randint(0, 1000000000000)))
    return filename


def parse_uri(path):
    result = urllib.parse.urlparse(path)
    return result.netloc, result.path.lstrip("/")


def head_file_s3(path):
    bucket, key = parse_uri(path)
    s3.meta.client.head_object(Bucket=bucket, Key=key)
    return True


def delete_file_s3(path):
    bucket, key = parse_uri(path)
    s3.meta.client.delete_object(Bucket=bucket, Key=key)


# Prepare testing environment 
# ---------------------------

existing_sample_version = "sample-version=13e4e7f62eb6ac60e44c2094a6cd86b7"
test_sample_version = "sample-version=test"
bucket_name = "workflow-orchestrator-test"
bucket_uri = "s3://workflow-orchestrator-test"

test_existing_file_prefix = with_bucket(
    "data/{}/t10k/labels.npz".format(existing_sample_version.strip("/")))
test_existing_dir_prefix = with_bucket(
    "data/{}/t10k/".format(existing_sample_version.strip("/")))
test_existing_file = "data/{}/t10k/labels.npz".format(
    existing_sample_version.strip("/"))
test_existing_dir = "data/{}/t10k/".format(
    existing_sample_version.strip("/"))

test_nonexisting_file_prefix = with_bucket("data/{}")


# Tests 
# -----

@pytest.fixture
def w():
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix="data/sample-version=test/").delete()
    yield wo.Orchestrator(is_dev=True)
    bucket.objects.filter(Prefix="data/sample-version=test/").delete()


def test_list_dir(w):
    expected = {"imgs.npz", "labels.npz"}
    for full_path, relative_path in w.list_prefix(test_existing_dir_prefix):
        expected.remove(relative_path)
    assert len(expected) == 0
    

def test_download_file_with_equal_source_and_destination(w):
    w.download_file(test_existing_file_prefix, test_existing_file_prefix)
    assert os.path.exists(test_existing_file)
    shutil.rmtree("data")


def test_download_file_with_random_destination_dir(w):
    w.download_file(test_existing_file_prefix, "data/test/labels.npz")
    assert os.path.exists("data/test/labels.npz")
    shutil.rmtree("data")


def test_download_file_with_filename(w):
    w.download_file(test_existing_file_prefix, "labels.npz")
    assert os.path.exists("labels.npz")
    os.remove("labels.npz")


def test_download_dir_with_equal_source_and_destination(w):
    w.download_prefix(test_existing_dir_prefix, test_existing_dir_prefix)
    assert os.path.exists(test_existing_dir)
    assert os.path.exists(os.path.join(test_existing_dir, "imgs.npz"))
    assert os.path.exists(os.path.join(test_existing_dir, "labels.npz"))
    shutil.rmtree("data")


def test_download_dir_with_random_destination_dir(w):
    w.download_prefix(test_existing_dir_prefix, "data/mnist/test")
    assert os.path.exists("data/mnist/test")
    assert os.path.exists("data/mnist/test/imgs.npz")
    assert os.path.exists("data/mnist/test/labels.npz")
    shutil.rmtree("data")


def test_upload_file(w):
    filename = random_file()
    destination = os.path.join(
        test_nonexisting_file_prefix, "test_upload_file", filename)
    w.upload_file(filename, destination)
    assert head_file_s3(destination)
    os.remove(filename)


def test_upload_dir(w):
    os.makedirs("data")
    filename1 = random_file()
    filename2 = random_file()
    shutil.move(filename1, "data")
    shutil.move(filename2, "data")

    test_upload_dir_path = with_bucket("data/sample-version=test/test_upload_dir")
    w.upload_prefix("data", test_upload_dir_path)
    for fullpath, relpath in w.list_prefix(test_upload_dir_path):
        assert relpath in (filename1, filename2)
        assert head_file_s3(fullpath)

    shutil.rmtree("data")

