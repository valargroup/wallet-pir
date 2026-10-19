"""Backend selections shared by full-package checks and qualification shards."""

NATIVE = ['enhance-query-server/native-reinspiring', 'status-query-server/native-reinspiring',
          'enhance-publisher/native-reinspiring', 'status-publisher/native-reinspiring', 'status-publisher/cli',
          'enhance-pir-client/native-reinspiring', 'enhance-pir-load-test/native-reinspiring',
          'native-certificate/native-reinspiring']
CUDA = ['enhance-query-server/cuda', 'status-query-server/cuda', 'enhance-publisher/cuda', 'status-publisher/cuda',
        'enhance-publisher/native-reinspiring', 'enhance-pir-load-test/native-reinspiring']
TRANSPARENT = ['transparent-publisher/rocksdb-reader', 'transparent-tools/test-support',
               'transparent-tools/compact-export', 'pir-publisher/transparent-tests']
