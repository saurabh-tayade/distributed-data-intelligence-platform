#!/usr/bin/env bash
# Run after `mvn -f ingestion/pom.xml package`. This is offline rejection/packaging evidence, not public acceptance.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import pathlib, subprocess, tempfile, xml.etree.ElementTree as ET
root=pathlib.Path.cwd()
report=ET.parse(root/'target/surefire-reports/TEST-platform.ingestion.managed.StrictPublicManagedRssFetchTest.xml')
properties={p.attrib['name']:p.attrib['value'] for p in report.getroot().find('properties')}
classpath=properties.get('surefire.test.class.path',properties['java.class.path'])
# Workflow exports do not persist across steps. Reuse the actual tested JDK,
# never a runner's unrelated JAVA_HOME default (which may still be JDK 17).
if properties['java.specification.version'] != '21':
    raise SystemExit('Packaged public FETCH smoke requires a successful JDK 21 test report')
java=pathlib.Path(properties['java.home'])/'bin/java'
with tempfile.TemporaryDirectory(prefix='public-fetch-package-') as tmp:
    subprocess.run([str(java),'-cp',classpath,'platform.ingestion.managed.PublicFetchPackagedSmoke',str(root/'target'),tmp],check=True)
PY
