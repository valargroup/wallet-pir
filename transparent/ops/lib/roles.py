"""Transparent publisher configuration and its matching systemd sandbox."""
from pathlib import Path
import shlex

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.role_config import directory, render
from wallet_pir_ops.deploy import units


def publisher(config, *, config_path, state_dir, executable='/usr/local/bin/transparent-publish-controller',
              template=None):
    """Render a journal writer or follower with permissions derived from its config."""
    if not isinstance(config, dict):
        raise ValueError('publisher configuration must be an object')
    follow = config.get('follow_journal', False)
    if type(follow) is not bool:
        raise ValueError('follow_journal must be a boolean')
    data, publications, initial = (directory(config[name]) for name in
                                  ('data_dir', 'publication_root', 'initial_publication'))
    state, config_path = directory(str(state_dir)), directory(str(config_path))
    writes = [publications, initial, state]
    if config.get('snapshot_dir') is not None:
        writes.append(directory(config['snapshot_dir']))
    if follow:
        # A writable ancestor would undo the intended read-only journal boundary.
        if any(Path(data).is_relative_to(p) or Path(p).is_relative_to(data) for p in writes):
            raise ValueError('follower journal must be separate from writable role paths')
        reads = [data, config_path]
    else:
        writes.append(data)
        reads = [config_path]
    if any(Path(config_path).is_relative_to(p) for p in writes):
        raise ValueError('publisher configuration must be outside writable role directories')
    source = ROOT / 'transparent/ops/deploy/transparent-publish-controller.service'
    template = source.read_text() if template is None else template
    template_reads = []
    for section, key, value in units.entries(template):
        if section == 'Service' and key == 'ReadOnlyPaths':
            if value:
                template_reads.extend(shlex.split(value))
            else:
                template_reads.clear()
    reads.extend(p for p in template_reads if p != data and p not in writes and p != config_path)
    return render(template, executable,
                  ['--config', config_path], {config_path: config}, writes, reads)
