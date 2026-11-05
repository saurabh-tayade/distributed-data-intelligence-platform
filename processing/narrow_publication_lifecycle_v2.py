"""Shared inactive v2 publication checks, never a launcher or execution authority.

The caller owns Spark and single-writer exclusion. No result sidecar, cleanup
claim, artifact registration, stage allocation or recovery adoption occurs here.
"""
from pathlib import Path

import content_metadata as m
import narrow_projection_transport as transport
from publication_metadata import canonical_bytes


def binding_root(binding):
    root = Path(binding['root'])
    m.need(root == m.root_path(root, 'PUBLICATION_UNAVAILABLE'), 'PUBLICATION_CHANGED')
    info = m._info(root, True, 'PUBLICATION_UNAVAILABLE', 'PUBLICATION_CHANGED')
    m.need([str(info.st_dev), str(info.st_ino)] == [binding['rootDevice'], binding['rootInode']],
           'PUBLICATION_CHANGED')
    return root, (info.st_dev, info.st_ino, info.st_mode)


def roots(command):
    """Check live native roots, not runtime registration or writer authorization."""
    output = command['projectionContext']['kind'].lower()
    root, identity = binding_root(command[output])
    runtime = command['runtime']
    binding_root(dict(root=runtime['scratchRoot'], rootDevice=runtime['scratchDevice'],
                      rootInode=runtime['scratchInode']))
    return root, identity


def check_guards(guards):
    for path, identity in guards:
        m._info(path, True, 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', identity)


def reserve(root, directory, root_identity):
    """Reserve only after actual absence, retaining every incomplete namespace."""
    for parent in (root / 'v1', directory.parent):
        try:
            parent.lstat()
        except FileNotFoundError:
            continue
        m._info(parent, True, 'PUBLICATION_UNAVAILABLE', 'PUBLICATION_CHANGED')
    try:
        directory.lstat()
    except FileNotFoundError:
        existing = False
    else:
        m._info(directory, True, 'PUBLICATION_INVALID', 'PUBLICATION_CHANGED')
        existing = True
        try:
            (directory / 'complete.json').lstat()
        except FileNotFoundError:
            raise m.ContentFailure('PUBLICATION_INCOMPLETE') from None
    if not existing:
        directory.parent.mkdir(parents=True, exist_ok=True)
        directory.mkdir(exist_ok=False)
    guards = [(root, root_identity)]
    for path in (*m.namespace_guards(root, directory)[1:], directory):
        info = m._info(path, True, 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED')
        guards.append((path, (info.st_dev, info.st_ino, info.st_mode)))
    check_guards(guards)
    return existing, tuple(guards)


def write(path, raw):
    with path.open('xb') as stream:
        m.need(stream.write(raw) == len(raw), 'PUBLICATION_FAILED')


def unchanged_before_marker(before, after):
    files = {item.path: item for item in after.files}
    m.need(all(files.get(item.path) == item for item in before.files), 'PUBLICATION_CHANGED')
    m.need(before.guards == after.guards, 'PUBLICATION_CHANGED')
    current = dict(after.directories)
    for path, (identity, names) in before.directories:
        new_identity, new_names = current[path]
        expected = set(names) | ({'complete.tmp'} if path == before.directory else set())
        m.need(identity == new_identity and expected == set(new_names), 'PUBLICATION_CHANGED')


def rename_last(directory):
    try:
        (directory / 'complete.json').lstat()
    except FileNotFoundError:
        pass
    else:
        raise m.ContentFailure('PUBLICATION_CHANGED')
    # If rename executes but its acknowledgement is lost, leave it alone. Only
    # fresh read-only verification may reconcile the completed native output.
    (directory / 'complete.tmp').rename(directory / 'complete.json')


def equal(left, right):
    m.need(canonical_bytes(left) == canonical_bytes(right), 'PUBLICATION_CHANGED')


def qualify(bound, verification):
    """All complete publication envelopes and all applicable RESUME pairs."""
    result = transport.qualify_publication_v2(bound, verification)
    if bound.kind == 'CONTENT':
        command = bound.value()
        for boundary in ('BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'):
            arguments = {} if boundary == 'BEFORE_FETCH' else dict(
                gold_verification=command['goldProof']['artifact']['verification'],
                prepared=command['prepared'])
            result += transport.resume_v2(bound, boundary, **arguments)
    return result
