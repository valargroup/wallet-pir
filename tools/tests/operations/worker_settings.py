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
    ('receiver', 'serve-publication'): {
        'config_path': '/etc/receiver-serve/config.json',
        'publication_record': '/etc/receiver-serve/record/publication.json',
        'publication_root': '/srv/receiver-serve/publications', 'state_dir': '/var/lib/receiver-serve',
        'bind': '127.0.0.1:18390', 'rpc_urls': ['http://127.0.0.1:8232'], 'no_auth': True},
}
