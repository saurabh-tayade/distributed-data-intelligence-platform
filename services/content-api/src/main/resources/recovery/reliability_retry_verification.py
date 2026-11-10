"""Read-only retry qualification. No writer import, claim, publication or database effects."""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

POLICY = 'E5_RELIABILITY_RETRY_EVIDENCE_V1'
COMPATIBLE = 'c528e4ec78ced7b574ada3efc4e62fbffde4e86149abe1ffd82dddad07192927'


def absent(p, root, fingerprint):
    """Absence only, with every existing parent resolved without following symlinks."""
    current = root
    stamps = []
    for segment in (None, 'v2', 'processor_revision=2', 'publication=' + fingerprint):
        if segment is not None:
            current = current / segment
        try:
            info = current.lstat()
        except FileNotFoundError:
            stamps.append(None)
        else:
            p.need(stat.S_ISDIR(info.st_mode) and current.resolve(strict=True) == current, 'PATH_UNSAFE')
            p.need(segment != 'publication=' + fingerprint, 'PUBLICATION_EXISTS')
            stamps.append(p.capture.stamp(info))
    return stamps


def main():
    path = Path(sys.argv[1])
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as f:
        s = os.fstat(f.fileno())
        if not (stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and s.st_uid == os.getuid() and s.st_mode & 0o077 == 0 and 0 < s.st_size <= 65536):
            return 1
        raw = f.read(65537)
    hint = json.loads(raw)
    root = Path(hint['runtime']['configuration']['processingRoot'])
    if hashlib.sha256((root/'reliability_execution_preflight.py').read_bytes()).hexdigest() != COMPATIBLE:
        return 1
    sys.path.insert(0, str(root))
    import reliability_execution_preflight as p
    from publication_metadata import canonical_bytes
    c = p.parse(p.read(path, p.COMMAND_LIMIT, private=True), p.COMMAND_LIMIT)
    bundle_raw = p.read(path.parent/'selection.json', p.EVIDENCE_LIMIT, private=True)
    bundle = p.parse(bundle_raw, p.EVIDENCE_LIMIT)
    p.need(hashlib.sha256(bundle_raw).hexdigest() == c['bundleSha256'])
    evidence = p.qualify(c, bundle)
    fingerprint = evidence['nativeDependencyFingerprint']
    output = p.root(bundle['bindings']['output'])
    before = absent(p, output, fingerprint)
    p.need(p.recheck(c, bundle, evidence) == evidence, 'EVIDENCE_CHANGED')
    p.need(before == absent(p, output, fingerprint), 'EVIDENCE_CHANGED')
    p.need(raw == p.read(path, p.COMMAND_LIMIT, private=True) and bundle_raw == p.read(path.parent/'selection.json', p.EVIDENCE_LIMIT, private=True), 'EVIDENCE_CHANGED')
    result = dict(policy=POLICY, commandSha256=hashlib.sha256(raw).hexdigest(),
                  selectionSha256=c['capturedConfigSha256'], bridgeSha256=sys.argv[2],
                  classification='OUTPUT_ABSENT', evidence=evidence)
    p.write_new(path.parent/'result.json', canonical_bytes(result), p.EVIDENCE_LIMIT)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        sys.exit(1)
