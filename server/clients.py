import aerospike
from aerospike_vector_search import types, Client, AdminClient
from config import Config

vector_seed = types.HostPort(host=Config.VECTOR_HOST, port=Config.VECTOR_PORT)
vector_admin = AdminClient(seeds=vector_seed)
vector_client = Client(seeds=vector_seed)

aerospike_client_config = {
    'hosts': [(Config.AEROSPIKE_HOST, Config.AEROSPIKE_PORT)]
}
aerospike_client = aerospike.client(aerospike_client_config).connect()
