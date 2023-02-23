from obiba_opal import TablePermService
from tests.utils import make_client

class TestClass:

    @classmethod
    def setup_class(cls):
        client = make_client()
        setattr(cls, 'client', client)

    @classmethod
    def teardown_class(cls):
        cls.client.close()
    
    def test_table(self):
        client = self.client
        service = TablePermService(client)
        perms = service.get_perms('CNSIM', 'CNSIM1', 'user')
        assert len(perms) > 0
        service.add_perm('CNSIM', 'CNSIM1', 'pwel', 'user', 'view')
        perms = service.get_perms('CNSIM', 'CNSIM1', 'user')
        assert 'pwel' in [x['subject']['principal'] for x in perms]
        service.delete_perm('CNSIM', 'CNSIM1', 'pwel', 'user')
        perms = service.get_perms('CNSIM', 'CNSIM1', 'user')
        assert 'pwel' not in [x['subject']['principal'] for x in perms]
        