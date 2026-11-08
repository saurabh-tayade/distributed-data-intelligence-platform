"""Strict child's actual installed-runtime checks and owned Spark lifecycle."""
from contextlib import contextmanager
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile

import artifact_import_verifier as p


def installed_versions():
    import boto3
    import botocore
    import duckdb
    import py4j
    import pyspark
    actual = dict(python='.'.join(map(str, sys.version_info[:3])), pyspark=pyspark.__version__,
                  boto3=boto3.__version__, botocore=botocore.__version__, duckdb=duckdb.__version__, py4j=py4j.__version__)
    p.need(actual == dict(python='3.12.15', pyspark='4.0.2', boto3='1.43.114',
                         botocore='1.43.114', duckdb='1.4.5', py4j='0.10.9.9'), 'RUNTIME_INCOMPATIBLE')
    return actual


def check_runtime(runtime):
    p.fields(runtime, ('javaHome', 'scratchRoot', 'scratchDevice', 'scratchInode'))
    scratch, java = Path(runtime['scratchRoot']), Path(runtime['javaHome'])
    p.need(scratch.is_absolute() and java.is_absolute() and scratch == scratch.resolve(strict=True)
           and java == java.resolve(strict=True) and (java / 'bin/java').is_file(), 'PATH_UNSAFE')
    info = p.info(scratch, True)
    p.need([str(info.st_dev), str(info.st_ino)] == [runtime['scratchDevice'], runtime['scratchInode']],
           'EVIDENCE_CHANGED')
    p.need(stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.getuid()
           and re.fullmatch(r'[A-Za-z0-9/_.,=-]+', str(scratch)) is not None, 'PATH_UNSAFE')
    p.need(shutil.disk_usage(scratch).free >= 1024**3, 'RESOURCE_LIMIT')
    return scratch, java


class Cleanup:
    def __init__(self):
        self.status, self.pid = 'NOT_STARTED', None

    def value(self):
        return dict(status=self.status, gatewayPid=self.pid)

    def observe(self, gateway):
        """Require actual exit zero, not a projected shutdown-status field."""
        if gateway is None:
            return
        self.status = 'UNCONFIRMED'
        process = gateway.proc
        p.need(process is not None, 'SPARK_CLEANUP_UNCERTAIN')
        self.pid = str(process.pid)
        p.need(process.poll() == 0, 'SPARK_CLEANUP_UNCERTAIN')
        self.status = 'CONFIRMED'


@contextmanager
def spark_session(runtime, cleanup):
    from pyspark import SparkContext
    from silver import session
    scratch, java = check_runtime(runtime)
    p.need(SparkContext._active_spark_context is None and SparkContext._gateway is None,
           'RUNTIME_INCOMPATIBLE')
    private = Path(tempfile.mkdtemp(prefix='strict-content-', dir=scratch))
    for key in ('PYSPARK_SUBMIT_ARGS', 'SPARK_REMOTE', 'SPARK_CONNECT_MODE_ENABLED',
                'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS'):
        os.environ.pop(key, None)
    os.environ.update(JAVA_HOME=str(java), PYSPARK_PYTHON=sys.executable, SPARK_LOCAL_IP='127.0.0.1',
        SPARK_LOCAL_DIRS=str(private), TMPDIR=str(private), SPARK_SUBMIT_OPTS='-Djava.io.tmpdir=' + str(private))
    tempfile.tempdir = str(private)
    original_directory = Path.cwd()
    os.chdir(private)
    spark = gateway = None
    cleanup.status = 'UNCONFIRMED'
    try:
        spark = session()
        gateway = SparkContext._gateway
        p.need(spark.version == '4.0.2' and
               spark._jvm.java.lang.System.getProperty('java.specification.version') == '21',
               'RUNTIME_INCOMPATIBLE')
        spark.sparkContext.setLogLevel('ERROR')
        yield spark
    finally:
        try:
            if spark is not None:
                spark.stop()
            gateway = gateway or SparkContext._gateway
            p.need(gateway is not None and gateway.proc is not None, 'SPARK_CLEANUP_UNCERTAIN')
            cleanup.pid = str(gateway.proc.pid)
            gateway.shutdown()
            gateway.proc.stdin.close()
            p.need(gateway.proc.wait(timeout=5) == 0, 'SPARK_CLEANUP_UNCERTAIN')
            cleanup.observe(gateway)
        except Exception:
            cleanup.status = 'UNCONFIRMED'
            raise p.Failure('SPARK_CLEANUP_UNCERTAIN') from None
        finally:
            os.chdir(original_directory)
