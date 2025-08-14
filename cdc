private static Map<String, ColType> children(Field f) {
  Map<String, ColType> m = new LinkedHashMap<>();
  FieldList subs = f.getSubFields();          // may be null unless RECORD
  if (subs != null) {
    // FieldList implements Iterable<Field> in all supported versions
    for (Field sf : subs) {
      m.put(sf.getName().toLowerCase(), fromField(sf));
    }
  }
  return m;
}
============

totally hear you — you want a **single, generalized Flex template** that works for *any* Spanner table(s) and *any* column types, including **DATE** and **DATETIME/TIMESTAMP**, with your delete-sentinel rules and robust null/format handling.

Below is a **complete, production-grade** implementation that:

* reads **Spanner Change Streams**
* dynamically converts keys/new values based on the **actual BigQuery schema** (so it’s table/column-agnostic)
* handles **STRING/BOOL/INT64/FLOAT64/NUMERIC/BIGNUMERIC/DATE/DATETIME/TIMESTAMP/JSON/BYTES/ARRAY/RECORD/GEOGRAPHY**
* applies your **DELETE policy** (strings = `"Deleted"`, DATE=`9999-12-31`, DATETIME/TIMESTAMP=`9999-12-31T00:00:00` (TIMESTAMP will be “Z”))
* writes with **CDC semantics** using **Storage Write API** + `RowMutationInformation`
* supports **dynamic destinations**: `bqProjectId:bqDataset.<spanner_table_name>`
* validates required args up front (no late NPEs)

You can drop these classes into your Dataflow image (package names are examples).

---

# 0) Flex parameters (containerSpec.json)

Make sure the template declares these (names must match):

```json
"parameters": [
  { "name": "spannerProjectId", "is_optional": true },
  { "name": "spannerInstanceId" },
  { "name": "spannerDatabaseId" },
  { "name": "changeStreamId" },

  { "name": "bqProjectId" },
  { "name": "bqDataset" },

  { "name": "sourceTableFilter", "is_optional": true },   // default ".*"
  { "name": "opField", "is_optional": true },             // default "_op"
  { "name": "commitTsField", "is_optional": true }        // default "_commit_ts"
]
```

---

# 1) Options

`src/main/java/com/example/cdc/CdcOptions.java`

```java
package com.example.cdc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CdcOptions extends PipelineOptions {
  // Spanner
  String getSpannerProjectId();                  void setSpannerProjectId(String v); // optional (falls back to --project)
  @Validation.Required String getSpannerInstanceId();  void setSpannerInstanceId(String v);
  @Validation.Required String getSpannerDatabaseId();  void setSpannerDatabaseId(String v);
  @Validation.Required String getChangeStreamId();     void setChangeStreamId(String v);

  // BigQuery
  @Validation.Required String getBqProjectId();        void setBqProjectId(String v);
  @Validation.Required String getBqDataset();          void setBqDataset(String v);

  // Generic controls
  @Default.String(".*") String getSourceTableFilter(); void setSourceTableFilter(String v); // regex for table names
  @Default.String("_op") String getOpField();          void setOpField(String v);
  @Default.String("_commit_ts") String getCommitTsField(); void setCommitTsField(String v);
}
```

---

# 2) Mutation element + coder

`src/main/java/com/example/cdc/DynamicMutation.java`

```java
package com.example.cdc;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;

import java.io.*;

public class DynamicMutation implements java.io.Serializable {
  private final String table;                    // destination table name
  private final TableRow row;                    // values (normalized)
  private final RowMutationInformation rmi;      // UPSERT/DELETE + sequence

  public DynamicMutation(String table, TableRow row, RowMutationInformation rmi) {
    this.table = table; this.row = row; this.rmi = rmi;
  }
  public String table(){ return table; }
  public TableRow row(){ return row; }
  public RowMutationInformation rmi(){ return rmi; }

  /** Compact coder: Table/Row/MutationInfo */
  public static class Coder extends CustomCoder<DynamicMutation> {
    private static final StringUtf8Coder STR = StringUtf8Coder.of();
    private static final VarLongCoder LONG = VarLongCoder.of();
    private static final TableRowJsonCoder ROW = TableRowJsonCoder.of();

    @Override public void encode(DynamicMutation v, OutputStream out) throws IOException {
      STR.encode(v.table, out);
      ROW.encode(v.row, out);
      LONG.encode(v.rmi.getSequenceNumber(), out);
      STR.encode(v.rmi.getMutationType().name(), out);
    }

    @Override public DynamicMutation decode(InputStream in) throws IOException {
      String table = STR.decode(in);
      TableRow row = ROW.decode(in);
      long seq = LONG.decode(in);
      var mt = RowMutationInformation.MutationType.valueOf(STR.decode(in));
      return new DynamicMutation(table, row, RowMutationInformation.of(mt, seq));
    }

    @Override public void verifyDeterministic() {}
  }
}
```

---

# 3) Schema-aware normalizer (handles all BQ types + delete sentinels)

`src/main/java/com/example/cdc/DynamicValueNormalizer.java`

```java
package com.example.cdc;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/** Converts JSON values (from Spanner change stream) to BQ-compatible values per column type. */
public class DynamicValueNormalizer implements java.io.Serializable {

  /** BQ logical types we care about (mode handled via ColType.repeated/fields). */
  public enum SqlT { STRING, BYTES, BOOL, INT64, FLOAT64, NUMERIC, BIGNUMERIC, DATE, DATETIME, TIMESTAMP, JSON, GEOGRAPHY, RECORD, UNKNOWN }

  /** Column type tree (supports RECORD and REPEATED-of-Element). */
  public static final class ColType implements java.io.Serializable {
    public final SqlT kind;
    public final boolean repeated;
    public final Map<String, ColType> fields;  // for RECORD
    public final ColType element;              // for REPEATED

    private ColType(SqlT kind, boolean repeated, Map<String, ColType> fields, ColType element) {
      this.kind = kind; this.repeated = repeated; this.fields = fields; this.element = element;
    }
    public static ColType scalar(SqlT k) { return new ColType(k, false, Map.of(), null); }
    public static ColType array(ColType el) { return new ColType(SqlT.UNKNOWN, true, Map.of(), el); }
    public static ColType record(Map<String, ColType> fields) { return new ColType(SqlT.RECORD, false, fields, null); }
  }

  /** Registry: table -> (column -> ColType) */
  private final Map<String, Map<String, ColType>> registry;
  private final String opField, commitField;

  // DELETE sentinels/policy
  private static final String DELETE_STRING = "Deleted";
  private static final String DATE_DELETE = "9999-12-31";
  private static final String DT_DELETE   = "9999-12-31T00:00:00";
  private static final String TS_DELETE_Z = "9999-12-31T00:00:00Z";

  private static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final DateTimeFormatter LDT  = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  public DynamicValueNormalizer(Map<String, Map<String, ColType>> registry,
                                String opField, String commitField) {
    this.registry = registry; this.opField = opField; this.commitField = commitField;
  }

  /** Build registry from a BQ dataset (call in main; ensure SA has BigQuery Viewer). */
  public static DynamicValueNormalizer fromDataset(String project, String dataset,
                                                   String opField, String commitField) {
    BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
    Dataset ds = bq.getDataset(DatasetId.of(project, dataset));
    Map<String, Map<String, ColType>> out = new HashMap<>();
    for (Table t : ds.list().iterateAll()) {
      String table = t.getTableId().getTable();
      var def = t.getDefinition();
      if (!(def instanceof StandardTableDefinition std) || std.getSchema() == null) continue;
      Map<String, ColType> cols = new LinkedHashMap<>();
      for (Field f : std.getSchema().getFields()) cols.put(f.getName().toLowerCase(), fromField(f));
      out.put(table, cols);
    }
    return new DynamicValueNormalizer(out, opField, commitField);
  }

  private static ColType fromField(Field f) {
    var std = f.getType().getStandardType(); // element type for REPEATED
    ColType base = switch (std) {
      case STRING     -> ColType.scalar(SqlT.STRING);
      case BYTES      -> ColType.scalar(SqlT.BYTES);
      case BOOL       -> ColType.scalar(SqlT.BOOL);
      case INT64      -> ColType.scalar(SqlT.INT64);
      case FLOAT64    -> ColType.scalar(SqlT.FLOAT64);
      case NUMERIC    -> ColType.scalar(SqlT.NUMERIC);
      case BIGNUMERIC -> ColType.scalar(SqlT.BIGNUMERIC);
      case DATE       -> ColType.scalar(SqlT.DATE);
      case DATETIME   -> ColType.scalar(SqlT.DATETIME);
      case TIMESTAMP  -> ColType.scalar(SqlT.TIMESTAMP);
      case JSON       -> ColType.scalar(SqlT.JSON);
      case GEOGRAPHY  -> ColType.scalar(SqlT.GEOGRAPHY);
      case STRUCT     -> ColType.record(children(f));
      default         -> ColType.scalar(SqlT.UNKNOWN);
    };
    if (f.getMode() == Field.Mode.REPEATED) return ColType.array(base);
    return base;
  }
  private static Map<String,ColType> children(Field f){
    Map<String,ColType> m = new LinkedHashMap<>();
    if (f.getSubFields() != null) for (Field sf : f.getSubFields().getFields()) m.put(sf.getName().toLowerCase(), fromField(sf));
    return m;
  }

  /** Convert Spanner JSON (keys/newValues) into TableRow typed per BQ schema. */
  public TableRow normalize(String table, Map<String,Object> keyVals, Map<String,Object> newVals,
                            boolean isDelete, Instant commitTs) {
    var colTypes = registry.getOrDefault(table, Map.of()); // may be empty (we still pass through)
    TableRow out = new TableRow();

    // keys (always present)
    keyVals.forEach((k,v) -> out.set(k, convert(colTypes.get(k.toLowerCase()), v, isDelete)));

    // non-delete: payload values
    if (!isDelete) newVals.forEach((k,v) -> out.set(k, convert(colTypes.get(k.toLowerCase()), v, false)));

    // CDC metadata
    out.set(commitField, commitTs.toString()); // RFC3339 with 'Z'
    out.set(opField, isDelete ? "DELETE" : "UPSERT");
    return out;
  }

  /** Recursive convert honoring column type (including RECORD/REPEATED). */
  private Object convert(ColType t, Object v, boolean isDelete) {
    if (t == null) t = ColType.scalar(SqlT.UNKNOWN); // pass-through if unknown
    if (v == null) return deleteSentinelOrNull(t, isDelete);

    // org.json → Java collections
    if (v instanceof org.json.JSONObject jo) v = jo.toMap();
    if (v instanceof org.json.JSONArray  ja) v = ja.toList();

    if (t.repeated) {
      if (v instanceof List<?> list) {
        List<Object> out = new ArrayList<>(list.size());
        for (Object e : list) out.add(convert(t.element, e, isDelete));
        return out;
      }
      // single value into array column: wrap
      return List.of(convert(t.element, v, isDelete));
    }

    switch (t.kind) {
      case STRING     -> { return isDelete ? DELETE_STRING : v.toString(); }
      case BOOL       -> { return (v instanceof Boolean b) ? b : Boolean.valueOf(v.toString()); }
      case INT64      -> { return (v instanceof Number n) ? n.longValue() : (v.toString().isBlank()? null : Long.valueOf(v.toString())); }
      case FLOAT64    -> { return (v instanceof Number n) ? n.doubleValue() : Double.valueOf(v.toString()); }
      case NUMERIC, BIGNUMERIC -> { return v.toString(); } // keep as String to preserve precision
      case DATE       -> { return isDelete ? DATE_DELETE : toDateString(v); }
      case DATETIME   -> { return isDelete ? DT_DELETE   : toDatetimeString(v); }  // no timezone in BQ DATETIME
      case TIMESTAMP  -> { return isDelete ? TS_DELETE_Z : toTimestampString(v); } // RFC3339 'Z'
      case JSON       -> { return (v instanceof String s) ? s : toJsonString(v); }
      case BYTES      -> { return (v instanceof String s) ? s : v.toString(); }    // expect base64
      case GEOGRAPHY  -> { return v.toString(); }  // WKT/GeoJSON string
      case RECORD     -> {
        // v can be Map<String,Object> or JSON string; normalize into Map recursively
        Map<String,Object> src = (v instanceof Map<?,?> m) ? cast(m) : parseJsonToMap(v.toString());
        Map<String,Object> out = new LinkedHashMap<>();
        for (var e : src.entrySet()) {
          ColType child = t.fields.getOrDefault(e.getKey().toLowerCase(), ColType.scalar(SqlT.UNKNOWN));
          out.put(e.getKey(), convert(child, e.getValue(), isDelete));
        }
        return out;
      }
      case UNKNOWN    -> { return v; }
    }
    return v;
  }

  private static Object deleteSentinelOrNull(ColType t, boolean isDelete) {
    if (!isDelete) return null;
    if (t.repeated) return List.of(); // delete -> empty array (or null; choose policy)
    return switch (t.kind) {
      case DATE      -> DATE_DELETE;
      case DATETIME  -> DT_DELETE;
      case TIMESTAMP -> TS_DELETE_Z;
      case STRING    -> DELETE_STRING;
      default        -> null; // numbers/booleans/bytes/json/record -> null on delete
    };
  }

  private static String toDateString(Object v) {
    String s = v.toString();
    if (s.length() >= 10) s = s.substring(0,10);
    return LocalDate.parse(s, DATE).format(DATE);
  }

  private static String toDatetimeString(Object v) {
    String s = v.toString();
    try { return OffsetDateTime.parse(s).toLocalDateTime().format(LDT); } catch (Exception ignore) {}
    if (s.endsWith("Z")) s = s.substring(0, s.length()-1);
    try { return LocalDateTime.parse(s, LDT).format(LDT); } catch (Exception ignore) {}
    // Truncate extra fractional seconds if any
    int dot = s.indexOf('.');
    if (dot > 0) {
      String head = s.substring(0, dot);
      String frac = s.substring(dot+1).replaceAll("Z|[+-].*$", "");
      if (frac.length() > 9) frac = frac.substring(0, 9);
      return LocalDateTime.parse(head + "." + frac, LDT).format(LDT);
    }
    return LocalDateTime.parse(s, LDT).format(LDT);
  }

  private static String toTimestampString(Object v) {
    String s = v.toString();
    try { return Instant.parse(s).toString(); } catch (Exception ignore) {}
    try { return OffsetDateTime.parse(s).toInstant().toString(); } catch (Exception ignore) {}
    // local date-time: treat as UTC
    try { return LocalDateTime.parse(s, LDT).atOffset(ZoneOffset.UTC).toInstant().toString(); } catch (Exception ignore) {}
    // strip trailing 'Z' then parse as local
    if (s.endsWith("Z")) s = s.substring(0, s.length()-1);
    return LocalDateTime.parse(s, LDT).atOffset(ZoneOffset.UTC).toInstant().toString();
  }

  @SuppressWarnings("unchecked")
  private static Map<String,Object> cast(Map<?,?> m){
    Map<String,Object> out = new LinkedHashMap<>();
    for (var e : m.entrySet()) out.put(String.valueOf(e.getKey()), e.getValue());
    return out;
  }

  private static String toJsonString(Object v) {
    // Simple JSON encoding (no external deps at runtime); you can swap in Gson/Jackson
    if (v instanceof String s) return s;
    return new com.google.gson.Gson().toJson(v);
  }

  private static Map<String,Object> parseJsonToMap(String s) {
    try { return cast(new com.google.gson.Gson().fromJson(s, Map.class)); }
    catch(Exception e) { return Map.of(); }
  }
}
```

---

# 4) Spanner record → DynamicMutation (uses the normalizer)

`src/main/java/com/example/cdc/RecordToDynamicMutationFn.java`

```java
package com.example.cdc;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation;
import org.apache.beam.sdk.io.gcp.bigquery.RowMutationInformation.MutationType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Map;

public class RecordToDynamicMutationFn extends DoFn<DataChangeRecord, DynamicMutation> {
  private final DynamicValueNormalizer norm;
  private final String tableRegex;

  public RecordToDynamicMutationFn(DynamicValueNormalizer norm, String tableRegex) {
    this.norm = norm; this.tableRegex = tableRegex;
  }

  @ProcessElement
  public void process(@Element DataChangeRecord rec, OutputReceiver<DynamicMutation> out) {
    String table = rec.getTableName();
    if (!table.matches(tableRegex)) return;

    Timestamp ts = rec.getCommitTimestamp();
    long seq = ts.getSeconds()*1_000_000_000L + ts.getNanos();
    boolean isDelete = rec.getModType() == ModType.DELETE;
    MutationType mt = isDelete ? MutationType.DELETE : MutationType.UPSERT;
    var rmi = RowMutationInformation.of(mt, seq);
    Instant commit = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());

    for (Mod mod : rec.getMods()) {
      Map<String,Object> keys = new JSONObject(nz(mod.getKeysJson(),"{}")).toMap();
      Map<String,Object> vals = new JSONObject(nz(mod.getNewValuesJson(),"{}")).toMap();
      TableRow row = norm.normalize(table, keys, vals, isDelete, commit);
      out.output(new DynamicMutation(table, row, rmi));
    }
  }

  private static String nz(String s, String d){ return s==null? d : s; }
}
```

---

# 5) Main pipeline (dynamic destinations + CDC write)

`src/main/java/com/example/cdc/SpannerCdcGeneric.java`

```java
package com.example.cdc;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class SpannerCdcGeneric {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(CdcOptions.class);
    CdcOptions o = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdcOptions.class);

    SpannerConfig spCfg = SpannerConfig.create()
        .withProjectId(o.getSpannerProjectId() != null ? o.getSpannerProjectId() : o.getProject())
        .withInstanceId(o.getSpannerInstanceId())
        .withDatabaseId(o.getSpannerDatabaseId());

    // Build schema registry/normalizer ONCE at startup (workers will serialize it)
    DynamicValueNormalizer norm = DynamicValueNormalizer.fromDataset(
        o.getBqProjectId(), o.getBqDataset(), o.getOpField(), o.getCommitTsField());

    Pipeline p = Pipeline.create(o);

    PCollection<DynamicMutation> muts =
        p.apply("ReadChangeStream",
                SpannerIO.readChangeStream().withSpannerConfig(spCfg).withChangeStreamName(o.getChangeStreamId()))
         .apply("ToDynamic",
                org.apache.beam.sdk.transforms.ParDo.of(new RecordToDynamicMutationFn(norm, o.getSourceTableFilter())))
         .setCoder(new DynamicMutation.Coder());

    muts.apply("WriteCDC",
        BigQueryIO.<DynamicMutation>write()
            .to(m -> tableRef(o.getBqProjectId(), o.getBqDataset(), m.table()))
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)     // provision tables in advance
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(Method.STORAGE_API_AT_LEAST_ONCE)              // CDC mode
            .withFormatFunction(DynamicMutation::row)
            .withRowMutationInformationFn(DynamicMutation::rmi));

    p.run();
  }

  private static TableReference tableRef(String proj, String ds, String table) {
    TableReference tr = new TableReference();
    tr.setProjectId(proj); tr.setDatasetId(ds); tr.setTableId(table);
    return tr;
  }
}
```

---

## Why this is “generalized” and robust

* **No per-table models**: it inspects the **actual BQ schema** and converts JSON accordingly.
* **All major BQ types** supported, including **RECORD** (nested) and **REPEATED** (arrays).
* **DATE/DATETIME/TIMESTAMP**:

  * accepts ISO with/without `Z`/offsets,
  * truncates overlong fractions,
  * outputs **DATE** `"yyyy-MM-dd"`, **DATETIME** `"yyyy-MM-dd'T'HH:mm:ss[.fffff...]"`, **TIMESTAMP** RFC3339 `"…Z"`.
* **Delete policy** applied per type:

  * STRING → `"Deleted"`,
  * DATE → `9999-12-31`,
  * DATETIME → `9999-12-31T00:00:00`,
  * TIMESTAMP → `9999-12-31T00:00:00Z`,
  * numerics/booleans/others → `null` (you can change this).
* **Dynamic destinations**: one sink per `tableName` from the change stream.
* **CDC correctness**: uses `RowMutationInformation` to perform **UPSERT/DELETE** in place (BigQuery Storage Write API).

---


> **Important**: provisioner must **pre-create** destination tables with the intended schema (and primary keys for CDC), and **add new columns (nullable) before** Spanner starts emitting them (Storage Write API won’t auto-add fields).

---

If you want me to wire **Spanner→BQ table renames**, **column transforms**, or **policy tags** via a small `configLocation` JSON, I can extend `DynamicValueNormalizer` to read that in one pass.
