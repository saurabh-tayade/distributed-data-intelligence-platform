"""Verified, detached Gold/E1a selections; no Spark imports or Silver reads."""
from dataclasses import dataclass
from hashlib import sha256

import gold_analytics
import gold_reader
import publisher_reference
from publication_metadata import canonical_bytes
import content_metadata as m

GOLD_TABLES = ('gold_articles', 'gold_source_daily')
GOLD_TOP = frozenset(('inputs.json', 'complete.json', *GOLD_TABLES))
REFERENCE_TOP = frozenset(('reference.csv', 'vocabulary.json', 'manifest.json', 'complete.json'))


def _reference(root, reference_id, revision, fingerprint, evidence):
    try:
        result = publisher_reference.open_snapshot(root, reference_id, revision, fingerprint)
        evidence.check()
        return result
    except publisher_reference.ReferenceFailure as error:
        evidence.check()
        code = {'PATH_UNSAFE': 'PATH_UNSAFE', 'RESOURCE_LIMIT': 'RESOURCE_LIMIT',
                'SNAPSHOT_CHANGED': 'DEPENDENCY_CHANGED',
                'SNAPSHOT_UNAVAILABLE': 'DEPENDENCY_UNAVAILABLE'}.get(error.code, 'DEPENDENCY_INVALID')
        raise m.ContentFailure(code) from None


@dataclass(frozen=True)
class Dependencies:
    gold_root: object
    gold_fingerprint: str
    reference_root: object
    reference: publisher_reference.Reference
    gold: m.Evidence
    snapshot: m.Evidence
    gold_marker: object

    def paths(self, table):
        m.need(table in GOLD_TABLES, 'ARGUMENT_INVALID')
        return tuple(str(self.gold.directory / item['path'])
                     for item in self.gold_marker['files'][table])

    def check(self):
        self.gold.check()
        self.snapshot.check()
        current = _reference(self.reference_root, self.reference.reference_id,
                             self.reference.revision, self.reference.fingerprint, self.snapshot)
        m.need(current == self.reference, 'DEPENDENCY_CHANGED')

    def manifest(self):
        return dict(manifest_version=1, product_id=m.PRODUCT, product_contract_version=1,
                    processor_revision=1, dataset_contract_versions=dict(m.VERSIONS),
                    gold=dict(publication_fingerprint=self.gold_fingerprint,
                              input_manifest=m.descriptor('gold-inputs.json', self.gold.raw('inputs.json')),
                              completion=m.descriptor('gold-complete.json', self.gold.raw('complete.json'))),
                    reference=dict(reference_id=self.reference.reference_id,
                                   revision=self.reference.revision,
                                   snapshot_fingerprint=self.reference.fingerprint),
                    semantic_config=dict(m.SEMANTICS))

    @property
    def manifest_bytes(self):
        return canonical_bytes(self.manifest())

    @property
    def fingerprint(self):
        return sha256(self.manifest_bytes).hexdigest()


def _verify_dependencies(*, gold_root, gold_fingerprint, reference_root, reference_id,
                        revision, reference_fingerprint, output_root=None):
    for value in (gold_fingerprint, reference_fingerprint):
        m.match(value)
    m.match(reference_id, m.IDENTIFIER)
    m.match(revision, m.REVISION)
    gold_root = m.root_path(gold_root)
    reference_root = m.root_path(reference_root)
    roots = [gold_root, reference_root]
    if output_root is not None:
        roots.append(m.root_path(output_root, 'PUBLICATION_UNAVAILABLE'))
    m.separate(*roots)
    directory = gold_root / 'v1' / 'processor_revision=1' / f'input_set={gold_fingerprint}'
    # Selected-directory absence is availability; artifact failures are integrity.
    m.root_path(directory)
    gold = m.capture(directory, GOLD_TABLES, GOLD_TOP, 'DEPENDENCY_INVALID',
                     'DEPENDENCY_CHANGED', guards=m.namespace_guards(gold_root, directory))
    marker = m.parse(gold.raw('complete.json'), code='DEPENDENCY_INVALID', canonical=False)
    m.keys(marker, ('publication_version', 'gold_contract_versions', 'processor_revision',
                    'input_set_fingerprint', 'input_manifest_sha256', 'input_counts',
                    'duplicates', 'output_counts', 'files'), 'DEPENDENCY_INVALID')
    m.validate_inventory(marker['files'], GOLD_TABLES, 'DEPENDENCY_INVALID')
    m.need(marker['files'] == gold.inventory(), 'DEPENDENCY_INVALID')
    for value in marker['output_counts'].values():
        m.count(value, code='DEPENDENCY_INVALID')
    m.need(marker['output_counts']['gold_articles'] <= m.MAX_ROWS, 'RESOURCE_LIMIT')
    try:
        metadata = gold_reader.verify_metadata(gold_root, gold_fingerprint)
        m.need(metadata.marker_bytes == gold.raw('complete.json') and
               metadata.manifest_bytes == gold.raw('inputs.json'), 'DEPENDENCY_CHANGED')
        with gold_analytics.open_publication(gold_root, gold_fingerprint, memory_limit='512MiB'):
            pass
        metadata.verify_unchanged()
        gold.check()
    except gold_reader.AnalyticsFailure as error:
        gold.check()
        code = {'PUBLICATION_READ_FAILED': 'DEPENDENCY_UNAVAILABLE',
                'RESOURCE_LIMIT': 'RESOURCE_LIMIT'}.get(error.code, 'DEPENDENCY_INVALID')
        raise m.ContentFailure(code) from None
    reference_directory = reference_root / f'reference_id={reference_id}' / f'revision={revision}'
    m.root_path(reference_directory)
    snapshot = m.capture(reference_directory, (), REFERENCE_TOP, 'DEPENDENCY_INVALID',
                         'DEPENDENCY_CHANGED', guards=(reference_root, reference_directory.parent))
    reference = _reference(reference_root, reference_id, revision, reference_fingerprint, snapshot)
    result = Dependencies(gold_root, gold_fingerprint, reference_root, reference, gold, snapshot,
                          m.freeze(marker))
    m.validate_manifest(result.manifest_bytes)
    result.check()
    return result


def verify_dependencies(**selections):
    try:
        return _verify_dependencies(**selections)
    except m.ContentFailure:
        raise
    except OSError:
        raise m.ContentFailure('DEPENDENCY_UNAVAILABLE') from None
    except Exception:
        raise m.ContentFailure('DEPENDENCY_INVALID') from None
