from obiba_opal import TablePermService
from tests.utils import make_client
import random


class TestClass:
    @classmethod
    def setup_class(cls):
        client = make_client()
        cls.client = client

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_table(self):
        client = self.client
        service = TablePermService(client)
        id = random.choice(list(range(1, 999, 1)))
        name = f"pwel{id}"
        perms = service.get_perms("CNSIM", "CNSIM1", "user")
        assert len(perms) > 0
        service.add_perm("CNSIM", "CNSIM1", name, "user", "view")
        perms = service.get_perms("CNSIM", "CNSIM1", "user")
        assert name in [x["subject"]["principal"] for x in perms]
        service.delete_perm("CNSIM", "CNSIM1", name, "user")
        perms = service.get_perms("CNSIM", "CNSIM1", "user")
        assert name not in [x["subject"]["principal"] for x in perms]
