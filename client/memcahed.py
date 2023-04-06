from pymemcache.client import base


client = base.Client(('memcached', 11211))
client.set('some_key', 'some value')
print(client.get('some_key'))