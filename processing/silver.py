"""Bounded, fixture-only Bronze-to-Silver experiment; no network/storage client."""
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from hashlib import sha256
from pathlib import Path
from uuid import UUID
from xml.parsers import expat

from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
MAX_BYTES = 5 * 1024 * 1024


def strings(*names):
    return [T.StructField(name, T.StringType()) for name in names]


RSS_ITEM = T.StructType(strings("guid", "title", "link", "description", "pubDate"))
ATOM_ENTRY = T.StructType(strings("id", "title", "summary", "published", "updated") + [
    T.StructField("content", T.StructType(strings("_type", "_src", "_VALUE"))),
    T.StructField("link", T.ArrayType(T.StructType(strings("_href", "_rel"))))])
RSS = T.StructType([
    T.StructField("channel", T.StructType([
        T.StructField("item", T.ArrayType(RSS_ITEM))])),
    T.StructField("_corrupt_record", T.StringType())])
ATOM = T.StructType([
    T.StructField("entry", T.ArrayType(ATOM_ENTRY)),
    T.StructField("_corrupt_record", T.StringType())])
SILVER_SCHEMA = T.StructType([
    T.StructField("contract_version", T.IntegerType()),
    *strings("article_id", "source_id", "source_uri", "ingestion_id"),
    T.StructField("ingested_at", T.TimestampType()),
    *strings("input_uri", "payload_sha256"),
    T.StructField("payload_byte_length", T.LongType()),
    *strings("bronze_bucket", "bronze_payload_key", "bronze_metadata_key", "feed_format"),
    T.StructField("entry_index", T.IntegerType()),
    *strings("identifier_kind", "source_article_id", "title", "url", "summary"),
    T.StructField("published_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
    T.StructField("quality_warnings", T.ArrayType(T.StringType())),
])
REJECTION_SCHEMA = T.StructType([
    *SILVER_SCHEMA.fields,
    T.StructField("rejection_reasons", T.ArrayType(T.StringType())),
])


@dataclass(frozen=True)
class Observation:
    source_id: str
    source_uri: str
    ingestion_id: str
    ingested_at: datetime
    feed_format: str

    def validate(self):
        if not self.source_id.strip() or not self.source_uri.strip():
            raise ValueError("source_id and source_uri are required")
        if str(UUID(self.ingestion_id)) != self.ingestion_id:
            raise ValueError("ingestion_id must be a canonical UUID")
        if self.ingested_at.utcoffset() is None:
            raise ValueError("ingested_at must include a timezone")
        if self.feed_format not in ("rss", "atom"):
            raise ValueError("feed_format must be rss or atom")


def session():
    return (SparkSession.builder.master("local[2]").appName("silver-fixture-prototype")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate())


def preflight(payload, feed_format):
    """Validate a complete bounded document, without extracting article fields."""
    if len(payload) > MAX_BYTES:
        return "PAYLOAD_TOO_LARGE"
    try:
        xml = payload.decode("utf-8")
    except UnicodeDecodeError:
        return "UNSUPPORTED_ENCODING"
    parser = expat.ParserCreate(namespace_separator="|")
    root = []

    def start(name, attrs):
        if not root:
            root.append(name)

    def forbidden(*args):
        raise ValueError("DTD_FORBIDDEN")

    def declaration(version, encoding, standalone):
        if encoding and encoding.lower() not in ("utf-8", "utf8"):
            raise ValueError("UNSUPPORTED_ENCODING")

    parser.StartElementHandler = start
    parser.StartDoctypeDeclHandler = forbidden
    parser.EntityDeclHandler = forbidden
    parser.ExternalEntityRefHandler = forbidden
    parser.XmlDeclHandler = declaration
    try:
        parser.Parse(xml, True)
    except ValueError as error:
        return str(error)
    except expat.ExpatError:
        return "MALFORMED_XML"
    expected = "rss" if feed_format == "rss" else "http://www.w3.org/2005/Atom|feed"
    if root != [expected]:
        return "UNSUPPORTED_ROOT"
    return None


def clean(column):
    return F.when(F.length(F.trim(column)) > 0, F.trim(column))


def timestamp(column, feed_format):
    if feed_format == "atom":
        return F.try_to_timestamp(column, F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"))
    # Explicit English RFC-822 subset: weekday is informational; numeric offset or GMT.
    value = F.regexp_replace(column, r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun), ", "")
    value = F.regexp_replace(value, r" GMT$", " +0000")
    return F.try_to_timestamp(value, F.lit("d MMM yyyy HH:mm:ss Z"))


def project(frame, schema):
    return frame.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema])


def normalize(spark, path, observation):
    """Return accepted and rejected frames. Caller provides synthetic provenance.

    Failures reading files or invalid caller provenance abort rather than invent
    ingestion identity. Document failures produce one rejection with null ordinal.
    """
    observation.validate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    path = Path(path).resolve()
    with path.open("rb") as stream:
        payload = stream.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise ValueError("PAYLOAD_TOO_LARGE: fixture exceeds 5 MiB; no output written")
    metadata = {
        "contract_version": 1, "source_id": observation.source_id,
        "source_uri": observation.source_uri, "ingestion_id": observation.ingestion_id,
        "ingested_at": observation.ingested_at.astimezone(timezone.utc),
        "input_uri": path.as_uri(), "payload_sha256": sha256(payload).hexdigest(),
        "payload_byte_length": len(payload), "feed_format": observation.feed_format,
    }
    failure = preflight(payload, observation.feed_format)
    if failure:
        rejection = dict(metadata, quality_warnings=[], rejection_reasons=[failure])
        return (spark.createDataFrame([], SILVER_SCHEMA),
                spark.createDataFrame([rejection], REJECTION_SCHEMA))
    is_rss = observation.feed_format == "rss"
    parsed = spark.range(1).select(F.from_xml(
        F.lit(payload.decode("utf-8")), RSS if is_rss else ATOM,
        {"mode": "PERMISSIVE", "ignoreNamespace": "true",
         "columnNameOfCorruptRecord": "_corrupt_record"}).alias("doc"))
    entries = "doc.channel.item" if is_rss else "doc.entry"
    rows = parsed.select("doc._corrupt_record", F.posexplode_outer(entries).alias("entry_index", "entry"))
    if is_rss:
        guid, link = clean(F.col("entry.guid")), clean(F.col("entry.link"))
        identifier = F.coalesce(guid, link)
        kind = F.when(guid.isNotNull(), "rss_guid").when(link.isNotNull(), "rss_link")
        summary = F.col("entry.description")
        published, updated = clean(F.col("entry.pubDate")), F.lit(None).cast("string")
    else:
        native_id = clean(F.col("entry.id"))
        links = F.filter(F.col("entry.link"), lambda x:
                         (x._rel.isNull() | (x._rel == "alternate")) & clean(x._href).isNotNull())
        link = clean(F.try_element_at(links, F.lit(1))["_href"])
        identifier = F.coalesce(native_id, link)
        kind = F.when(native_id.isNotNull(), "atom_id").when(link.isNotNull(), "atom_link")
        content = F.col("entry.content")
        supported_content = F.when(
            content._src.isNull() & (content._type.isNull() | content._type.isin("text", "html")),
            content._VALUE)
        summary = F.when(clean(F.col("entry.summary")).isNotNull(), F.col("entry.summary")).otherwise(
            F.when(clean(supported_content).isNotNull(), supported_content))
        published, updated = clean(F.col("entry.published")), clean(F.col("entry.updated"))
    rows = rows.select("*", identifier.alias("source_article_id"), kind.alias("identifier_kind"),
                       clean(F.col("entry.title")).alias("title"), link.alias("url"),
                       summary.alias("summary"), published.alias("published_raw"),
                       updated.alias("updated_raw"))
    for name, value in metadata.items():
        rows = rows.withColumn(name, F.lit(value))
    for name in ("bronze_bucket", "bronze_payload_key", "bronze_metadata_key"):
        rows = rows.withColumn(name, F.lit(None).cast("string"))
    rows = rows.withColumn("published_at", timestamp(F.col("published_raw"), observation.feed_format))
    rows = rows.withColumn("updated_at", timestamp(F.col("updated_raw"), observation.feed_format))
    # JSON array framing avoids delimiter collisions; no ingestion UUID in article identity.
    rows = rows.withColumn("article_id", F.when(F.col("source_article_id").isNotNull(), F.sha2(
        F.to_json(F.array(F.lit("v1"), "source_id", "identifier_kind", "source_article_id")), 256)))
    reasons = F.filter(F.array(
        F.when(F.col("_corrupt_record").isNotNull(), "XML_MAPPING_ERROR"),
        F.when(F.col("entry_index").isNull(), "NO_SUPPORTED_ENTRIES"),
        F.when(F.col("source_article_id").isNull(), "MISSING_IDENTIFIER"),
        F.when(F.col("title").isNull(), "MISSING_TITLE"),
    ), lambda x: x.isNotNull())
    warnings = F.filter(F.array(
        F.when(F.col("published_raw").isNotNull() & F.col("published_at").isNull(), "INVALID_PUBLISHED_AT"),
        F.when(F.col("updated_raw").isNotNull() & F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
    ), lambda x: x.isNotNull())
    rows = rows.withColumn("quality_warnings", warnings).withColumn("rejection_reasons", reasons)
    return (project(rows.filter(F.size("rejection_reasons") == 0), SILVER_SCHEMA),
            project(rows.filter(F.size("rejection_reasons") > 0), REJECTION_SCHEMA))


def demo(spark, output):
    frames = []
    for number, name in enumerate(("rss-technology", "rss-business", "atom-science"), 1):
        observation = Observation(name, "https://example.com/" + name,
                                  str(UUID(int=number)), datetime(2026, 10, 12, 13, tzinfo=timezone.utc),
                                  name.split("-")[0])
        frames.append(normalize(spark, ROOT / "ingestion/src/test/resources/feeds" / (name + ".xml"), observation))
    accepted = reduce(lambda a, b: a.unionByName(b), [x[0] for x in frames])
    rejected = reduce(lambda a, b: a.unionByName(b), [x[1] for x in frames])
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    accepted.write.mode("errorifexists").parquet(str(output / "articles"))
    rejected.write.mode("errorifexists").parquet(str(output / "rejections"))
    loaded = spark.read.schema(SILVER_SCHEMA).parquet(str(output / "articles"))
    loaded.printSchema()
    print(f"Normalized records: {loaded.count()}; rejections: {rejected.count()}")
    loaded.createOrReplaceTempView("article_observations")
    spark.sql("SELECT source_id, count(*) AS observations FROM article_observations GROUP BY source_id ORDER BY source_id").show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "outputs/silver-prototype")
    args = parser.parse_args()
    spark = session()
    try:
        demo(spark, args.output)
    finally:
        spark.stop()
