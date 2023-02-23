from obiba_opal import UserService, GroupService
from tests.utils import make_client
import random

class TestClass:

    @classmethod
    def setup_class(cls):
        client = make_client()
        setattr(cls, 'client', client)

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_user_group(self):
        client = self.client
        service = UserService(client)
        id = random.choice(list(range(1, 999, 1)))
        name = 'shadow%s' % id
        grp = 'chats%s' % id
        upassword = 'aA1aaa@a'
        service.add_user(name, upassword, groups = [grp], disabled=True)
        user = service.get_user(name)
        assert user['name'] == name
        assert user['enabled'] == False
        assert len(user['groups']) == 1
        assert user['groups'][0] == grp
        assert user['authenticationType'] == 'PASSWORD'
        
        gservice = GroupService(client)
        groups = gservice.get_groups()
        assert grp in [x['name'] for x in groups]
        gservice.delete_group(grp)
        groups = gservice.get_groups()
        assert grp not in groups
        
        service.delete_user(name)
        user = service.get_user(name)
        assert user is None
    