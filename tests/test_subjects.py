from obiba_opal.user import UserService
from obiba_opal.group import GroupService
from tests.utils import make_client
import random

def test_user_group():
    client = make_client()
    service = UserService(client)
    id = random.choice(list(range(1, 999, 1)))
    name = 'shadow%s' % id
    upassword = 'aA1aaa@a'
    service.add_user(name, upassword, groups = ['chats'], disabled=True)
    user = service.get_user(name)
    assert user['name'] == name
    assert user['enabled'] == False
    assert len(user['groups']) == 1
    assert user['groups'][0] == 'chats'
    assert user['authenticationType'] == 'PASSWORD'
    
    gservice = GroupService(client)
    groups = gservice.get_groups()
    assert 'chats' in [x['name'] for x in groups]
    gservice.delete_group('chats')
    groups = gservice.get_groups()
    assert 'chats' not in groups
    
    service.delete_user(name)
    user = service.get_user(name)
    assert user is None
    