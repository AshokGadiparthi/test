perfect — you already have a working Flex template and a `gcloud dataflow flex-template run …` command. I’ll adjust the backend so it can launch **your** template (with your parameter names like `spannerInstanceId`, `bigQueryDataset`, etc.), and pass all the runtime environment flags you use (`maxWorkers`, `machineType`, `subnetwork`, `use_runner_v2`, `disable_public_ips`, KMS key, etc.).

Below are **drop-in changes** (3 small files) + an example request that mirrors your CLI.

---

# 1) New model to hold your Flex launch spec

**`src/main/java/com/example/meshcdc/model/FlexLaunchSpec.java`**

```java
package com.example.meshcdc.model;

import java.util.List;
import java.util.Map;

public class FlexLaunchSpec {
  /** gs://.../Template/<your-template>.json */
  public String templateGcs;

  /** parameters expected by your template (e.g. spannerInstanceId, bigQueryDataset, ...) */
  public Map<String, String> parameters;

  /** environment flags equivalent to gcloud run options */
  public Env env = new Env();

  public static class Env {
    public Integer numWorkers;
    public Integer maxWorkers;
    public String machineType;

    /** VPC */
    public String network;         // e.g. "shared-np-east"
    public String subnetwork;      // e.g. full URL: https://www.googleapis.com/compute/v1/projects/.../subnetworks/...

    /** Private IPs? (equivalent to --disable-public-ips) */
    public String ipConfiguration; // set to "WORKER_IP_PRIVATE" for private workers

    public Boolean enableStreamingEngine;
    public List<String> additionalExperiments; // e.g. ["use_runner_v2","disable_public_ips"]

    /** IAM + security */
    public String serviceAccountEmail; // override default SA if you want
    public String kmsKeyName;          // KMS key resource name

    /** Temp location override (optional; otherwise app config is used) */
    public String tempLocation;
  }
}
```

---

# 2) Update the Dataflow launcher to accept your template + env + params

**Replace** your `DataflowLauncherService.java` with this version:

```java
package com.example.meshcdc.service;

import com.example.meshcdc.model.FlexLaunchSpec;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.FlexTemplateRuntimeEnvironment;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class DataflowLauncherService {

  private final Dataflow df;
  private final String projectId;
  private final String region;
  private final String defaultSpecGcs;
  private final String defaultSaEmail;
  private final String defaultTempLocation;

  public DataflowLauncherService(
      Dataflow df,
      @Value("${mesh.project-id}") String projectId,
      @Value("${mesh.region}") String region,
      @Value("${mesh.dataflow.containerSpecGcs}") String defaultSpecGcs,
      @Value("${mesh.dataflow.service-account}") String defaultSaEmail,
      @Value("${mesh.dataflow.temp-location}") String defaultTempLocation
  ) {
    this.df = df;
    this.projectId = projectId;
    this.region = region;
    this.defaultSpecGcs = defaultSpecGcs;
    this.defaultSaEmail = defaultSaEmail;
    this.defaultTempLocation = defaultTempLocation;
  }

  /** Launch your Flex template with arbitrary parameter names and environment flags. */
  public String launchFlex(String jobName, FlexLaunchSpec spec) throws IOException {
    // 1) Build environment (fill from spec.env, fall back to defaults)
    FlexTemplateRuntimeEnvironment env = new FlexTemplateRuntimeEnvironment()
        .setServiceAccountEmail(or(spec.env.serviceAccountEmail, defaultSaEmail))
        .setTempLocation(or(spec.env.tempLocation, defaultTempLocation))
        .setEnableStreamingEngine(spec.env.enableStreamingEngine != null ? spec.env.enableStreamingEngine : Boolean.TRUE)
        .setAdditionalUserLabels(Map.of("product", jobName));

    if (spec.env.machineType != null) env.setMachineType(spec.env.machineType);
    if (spec.env.numWorkers != null)  env.setNumWorkers(spec.env.numWorkers);
    if (spec.env.maxWorkers != null)  env.setMaxWorkers(spec.env.maxWorkers);
    if (spec.env.network != null)     env.setNetwork(spec.env.network);
    if (spec.env.subnetwork != null)  env.setSubnetwork(spec.env.subnetwork);
    if (spec.env.kmsKeyName != null)  env.setKmsKeyName(spec.env.kmsKeyName);
    if (spec.env.ipConfiguration != null) env.setIpConfiguration(spec.env.ipConfiguration);
    if (spec.env.additionalExperiments != null && !spec.env.additionalExperiments.isEmpty())
      env.setAdditionalExperiments(spec.env.additionalExperiments);

    // 2) Build launch parameter with your template path and params
    LaunchFlexTemplateParameter param = new LaunchFlexTemplateParameter()
        .setJobName(jobName)
        // IMPORTANT: use your template file path (or fallback to default)
        .setTemplateFileGcsPath(or(spec.templateGcs, defaultSpecGcs))
        .setEnvironment(env)
        // Dataflow API requires Map<String,String>
        .setParameters(spec.parameters != null ? spec.parameters : Map.of());

    // 3) Launch
    LaunchFlexTemplateRequest req = new LaunchFlexTemplateRequest().setLaunchParameter(param);
    LaunchFlexTemplateResponse resp = df.projects().locations().flexTemplates()
        .launch(projectId, region, req)
        .execute();

    return resp.getJob() != null ? resp.getJob().getId() : null;
  }

  private static String or(String a, String b) { return (a != null && !a.isBlank()) ? a : b; }
}
```

This maps your CLI flags to Dataflow API fields:

* `--template-file-gcs-location` → `setTemplateFileGcsPath(...)`
* `--parameters k=v ...` → `setParameters(Map<String,String>)`
* `--region` → provided by `mesh.region`
* `--max-workers` → `env.setMaxWorkers(...)`
* `--worker-machine-type` → `env.setMachineType(...)`
* `--network` / `--subnetwork` → `env.setNetwork(...)` / `env.setSubnetwork(...)`
* `--enable-streaming-engine` → `env.setEnableStreamingEngine(true)`
* `--additional-experiments=use_runner_v2,disable_public_ips` → `env.setAdditionalExperiments([...])`
* `--disable-public-ips` → `env.setIpConfiguration("WORKER_IP_PRIVATE")`
* `--service-account-email` → `env.setServiceAccountEmail(...)`
* `--kms-key` → `env.setKmsKeyName(...)`
* `--staging-location` → `env.setTempLocation(...)`

---

# 3) Let pipeline creation carry this spec (store in DB as JSON)

**Update** your pipelines controller so you can POST the template path + params + env in one go and store it in `pipeline.spec_json`.

**`src/main/java/com/example/meshcdc/web/V2Controllers.java`** (replace the pipelines part only):

```java
// ... keep the ProductsV2Controller as-is above ...

@RestController
@RequestMapping("/v2/pipelines")
class PipelinesV2Controller {
  private final com.example.meshcdc.service.PipelineService svc;
  private final com.example.meshcdc.service.ProductService products;
  private final com.example.meshcdc.service.DataflowLauncherService launcher;

  public PipelinesV2Controller(com.example.meshcdc.service.PipelineService s,
                               com.example.meshcdc.service.ProductService p,
                               com.example.meshcdc.service.DataflowLauncherService l) {
    this.svc = s; this.products = p; this.launcher = l;
  }

  // Create request now carries your FlexLaunchSpec
  public record CreatePipelineReq(String productId, String name, String template,
                                  com.example.meshcdc.model.FlexLaunchSpec spec) {}

  @PostMapping
  public com.example.meshcdc.jpa.PipelineEntity create(@RequestBody CreatePipelineReq r) throws Exception {
    var pl = svc.create(r.productId(), r.name(), r.template());
    // Persist the spec JSON as-is so deploy can use it later
    pl.specJson = com.example.meshcdc.util.JsonUtil.MAPPER().writeValueAsString(r.spec());
    return svc.repo.save(pl);
  }

  @PostMapping("/{id}:deploy")
  public java.util.Map<String,String> deploy(@PathVariable String id) throws Exception {
    var pl = svc.findById(id);

    // Read stored spec JSON back into FlexLaunchSpec
    com.example.meshcdc.model.FlexLaunchSpec spec =
        com.example.meshcdc.util.JsonUtil.MAPPER().readValue(pl.specJson, com.example.meshcdc.model.FlexLaunchSpec.class);

    String jobId = launcher.launchFlex(
        ("repl-" + pl.product.id.substring(0,8)).toLowerCase(), // job name
        spec
    );
    svc.recordRun(pl, jobId);
    return java.util.Map.of("jobId", jobId);
  }
}
```

> Note: I’m reusing your existing `spec_json` column on `pipeline`. No DB changes needed.

---

# 4) Example: create + deploy (mirrors your screenshot)

Set the region you actually use:

```bash
export REGION=us-east4
```

**Create the pipeline** with your template and flags (adjust values to yours):

```bash
PL_CREATE=$(cat <<'JSON'
{
  "productId": "PUT-YOUR-PRODUCT-ID",
  "name": "spanner-to-bq-cust-acct-prfl",
  "template": "DATAFLOW_FLEX",
  "spec": {
    "templateGcs": "gs://<your-bucket>/Template/spanner-to-bq-cdc-cust-acct-prfl1.json",
    "parameters": {
      "spannerInstanceId": "<your-spanner-instance>",
      "spannerDatabaseId": "vzw_cdp_prd_tbls",
      "spannerCustAcctPrflStreamId": "cust_acct_prfl_cdc_stream",
      "bigQueryProjectId": "<your-project-id>",
      "bigQueryDataset": "vzw_cdp_cust",
      "bigQueryCustAcctPrflsTableName": "ntt_vzw_cust_acct_prfl"
    },
    "env": {
      "maxWorkers": 2,
      "machineType": "n2d-standard-2",
      "network": "shared-np-east",
      "subnetwork": "https://www.googleapis.com/compute/v1/projects/<vpc-project>/regions/us-east4/subnetworks/shared-np-east-green-subnet-1",
      "enableStreamingEngine": true,
      "additionalExperiments": ["use_runner_v2","disable_public_ips"],
      "serviceAccountEmail": "<your-sa>@<your-project>.iam.gserviceaccount.com",
      "kmsKeyName": "projects/<project>/locations/us-east4/keyRings/<ring>/cryptoKeys/<key>",
      "ipConfiguration": "WORKER_IP_PRIVATE",
      "tempLocation": "gs://<your-bucket>/staging"
    }
  }
}
JSON
)

curl -sX POST localhost:8080/v2/pipelines \
  -H 'content-type: application/json' \
  -d "$PL_CREATE" | jq
```

Grab the pipeline id from the response, then **deploy**:

```bash
curl -sX POST localhost:8080/v2/pipelines/<PIPELINE_ID>:deploy | jq
```

You should get `{ "jobId": "..." }`. Validate in Dataflow:

```bash
gcloud dataflow jobs list --region=$REGION --filter="id:<jobId>"
```

---

## Why this fixes “unrecognized parameter”

* We now **send exactly your parameter names** (whatever your `containerSpec.json` expects).
* We pass your **gcloud flags** through the proper Dataflow API fields (no silent drops).
* If you ever change your template, just update the JSON in the create call—no code changes.

If you paste your real `containerSpec.json` parameters block, I can verify the names one-by-one and give you an exact `curl` body with all the right keys.
