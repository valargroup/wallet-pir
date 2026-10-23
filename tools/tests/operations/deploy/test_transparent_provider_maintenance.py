"""Include provider-maintenance integration tests in the deployment suite."""
from tools.transparent.tests import test_provider_maintenance


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromModule(test_provider_maintenance)
