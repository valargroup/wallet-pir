"""One inventory-style settings object per external worker role, shared by tests."""
SETTINGS = {
    ('transparent-history', 'provider'): {
        'listen': '127.0.0.1:8092', 'state_dir': '/var/lib/transparent-provider',
        'publisher': 'https://publisher.example.invalid', 'archive_upstream': 'https://archive.example.invalid',
        'publisher_network': 'main', 'publisher_genesis': '00' * 32},
    ('transparent-history', 'https-recent'): {
        'listen': '127.0.0.1:8092', 'state_dir': '/var/lib/transparent-provider',
        'publisher': 'https://publisher.example.invalid', 'archive_upstream': 'https://archive.example.invalid',
        'publisher_network': 'main', 'publisher_genesis': '00' * 32,
        'member_id': 'https-1', 'fleet_host': 'coordinator', 'fleet_state': '/opt/transparent-publisher/state'},
    ('enhance', 'external-worker'): {
        'listen': '127.0.0.1:8091', 'data_dir': '/srv/enhance-worker',
        'synchronization': {'publisher_origin': 'https://publisher.example', 'download_budget_bytes': 8 << 30,
                            'cache_limit_bytes': 8 << 30, 'shards': [0]},
        'credentials': ['/etc/enhance-worker/internal-token.env'], 'limits': {'MemoryMax': '7G'}},
    ('enhance', 'combined'): {
        'listen': '127.0.0.1:8090', 'control_listen': '127.0.0.1:8096', 'data_dir': '/srv/pir-query-server/enhance',
        'synchronization': {'publisher_origin': 'https://enhance-pir.valargroup.dev', 'trusted_ca': None,
                            'download_budget_bytes': 8 << 30, 'cache_limit_bytes': 8 << 30, 'shards': [0]},
        'credentials': ['/etc/enhance-pir/internal-token.env']},
    ('status', 'combined'): {
        'network_hex': '01' * 32, 'state_dir': '/srv/pir-query-server/status', 'listen': '127.0.0.1:8481',
        'query_listen': '127.0.0.1:8090', 'publisher': 'https://enhance-pir.valargroup.dev'},
    ('status', 'external-worker'): {
        'network_hex': 'ab' * 32, 'listen': '127.0.0.1:8481', 'publisher': 'https://publisher.example',
        'state_dir': '/var/lib/status-worker'},
    ('transparent-history', 'external-worker'): {
        'listen': '127.0.0.1:8093', 'state_dir': '/var/lib/history-worker',
        'publisher': 'https://publisher.example.invalid', 'publisher_network': 'main',
        'publisher_genesis': '00' * 32, 'cache_bytes': 8 << 30},
    ('transparent-display', 'external-worker'): {
        'listen': '127.0.0.1:8095', 'state_dir': '/var/lib/display-worker',
        'publisher': 'https://publisher.example.invalid', 'publisher_network': 'main',
        'publisher_genesis': '00' * 32},
    ('receiver', 'query'): {
        'config_path': '/etc/receiver-query/config.json', 'state_dir': '/srv/receiver-query',
        'listen': '127.0.0.1:18390', 'management_listen': '127.0.0.1:18391',
        'publisher': 'https://receiver-publisher.example.invalid', 'genesis': [0] * 32,
        'activation_height': 3428143},
    ('receiver', 'server'): {
        'config_path': '/etc/receiver-publisher/config.json', 'management_listen': '127.0.0.1:18381',
        'chain': {'rpc_url': 'http://127.0.0.1:8232', 'no_auth': True},
        'receiver': {'data_dir': '/srv/receiver-pir/index', 'bind': '127.0.0.1:18380'}},
}
