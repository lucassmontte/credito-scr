# Decision log — credito-scr

## Day 0 — architecture questions asked before implementation

**Question:** Batch or streaming ingestion for SCR.data?
**Answer:** Batch. The source itself is a monthly aggregate CSV published once a month by Banco Central, so streaming would simulate a real-time need that does not exist in the data.
**Rationale:** Building a streaming pipeline against a monthly batch source is a common portfolio mistake — it signals "I used the trendy pattern" instead of "I matched the pattern to the data." Batch is the honest choice here.

**Question:** Which years of SCR.data to ingest?
**My answer:** 2023, 2024, 2025 — the most recent three years, enough to show a trend.
**Alternatives considered:** Full history since June 2012 (13+ years) vs. last 3 years only.
**Rationale for default:** Full history means downloading and processing 13+ annual ZIP files, each containing national-level aggregated credit data — likely several GB uncompressed per year. For a 5-day sprint, that trades days of ingestion time for marginal storytelling value. Three years is enough to show a trend (rising/falling default rates), enough to make the star schema meaningful, and small enough to keep BigQuery free-tier query costs near zero.
**Consequence:** If the interview conversation needs a longer time series (e.g., "how did the pandemic affect default rates"), 2020-2021 data is not included. Re-running ingestion for additional years is a one-line change to the YEARS list in the script, not a redesign.
**How I'd know this was wrong:** if the dashboard's year-over-year trend looks flat or uninteresting with only 3 years, or if a specific interview story needs 2020 data, add years and re-run.

**Question:** How to move the data from Banco Central to GCP — direct API/Cloud Function, or local Python download-then-upload?
**My answer:** Local Python script (download → unzip → upload to GCS), run once from Cloud Shell.
**Alternatives considered:**
1. Cloud Function triggered on a schedule, pulling directly from BCB and writing to GCS.
2. BigQuery Data Transfer Service (not applicable — BCB is not a supported source).
3. Manual download and manual `gsutil cp` (no code, no repeatability).
**Rationale for default:** Option 1 (Cloud Function) is the "productionized" answer and is exactly right for a service that updates monthly — but building and deploying a Cloud Function costs a half-day you don't have on day 1, and the ingestion job for a portfolio only needs to run a handful of times, not on a schedule an interviewer will actually see fire. The Python script is honest about what it is: a batch loader you would wrap in a Cloud Function or Cloud Composer task if this went to production. That wrapping step is captured as a backlog item, not skipped silently.
**Consequence:** Ingestion is not scheduled or automated yet. If asked in an interview "how would this run in production," the answer is "this script becomes the body of a Cloud Function triggered monthly by Cloud Scheduler" — which is true and which onda 2 will implement for the Dataflow/BigQuery-load stage, not for this raw pull.
**How I'd know this was wrong:** if I decide during onda 2 that full automation of the raw pull matters more than the transform/orchestration layer, revisit and move this into a Cloud Function.

**Question:** Bucket naming and region?
**My answer:** `credito-scr-raw`, region `southamerica-east1` (São Paulo).
**Rationale:** Data is Brazilian, I'm Brazil-based, and same-region storage-to-BigQuery load avoids cross-region egress cost. `southamerica-east1` is the standard GCP region for Brazil.
**Consequence:** If the free-tier trial account is not enabled for `southamerica-east1` for any reason, fall back to `us-central1` and note the added latency/cost tradeoff in this log.

## Day 1 — raw ingestion

**Context:** Attempted `gcloud services enable compute.googleapis.com dataflow.googleapis.com` and `gcloud storage buckets create` on project `credito-scr`. Both failed with `FAILED_PRECONDITION: Billing account for project ... is not found`. The project is running in BigQuery Sandbox mode — no billing account linked.

**Question:** Link a billing account to unlock Cloud Storage and managed Dataflow, or redesign around BigQuery Sandbox with zero cost?
**My answer:** I don't want to risk any cost. Redesign around the sandbox.
**Alternatives considered:**
1. Link billing, accept small real cost (estimated a few BRL for a one-off small Dataflow batch job and a few GB in a regional bucket), rely on budget alerts to cap risk.
2. Redesign the pipeline to run entirely inside what BigQuery Sandbox allows for free: no Cloud Storage bucket, no managed Dataflow, no Compute Engine.
**Choice:** Option 2.
**Rationale:** BigQuery itself does not require a billing account — the sandbox gives 10 GB active storage and 1 TB of query processing per month at zero cost. Cloud Storage, Compute Engine, and managed Dataflow all sit outside that sandbox and need billing enabled, even for near-zero real usage. Given the explicit preference to avoid any billing risk during a 5-day unpaid sprint, the redesign removes every service that requires billing and keeps the ones that don't.
**New architecture:**
```
BCB annual ZIP (downloaded to local/Cloud Shell disk)
  → extracted CSV, read locally
  → Apache Beam pipeline, DirectRunner (local execution, no GCP compute billed)
  → BigQuery load job into `raw` dataset (free, inside sandbox limits)
  → SQL transform inside BigQuery into the star schema in a `marts` dataset (free, inside sandbox limits)
```
**Consequence:** No Cloud Storage raw layer, no managed Dataflow execution, no Cloud Composer/Scheduler (those also need billing). The project narrative changes from "runs on managed GCP services end-to-end" to "engineered for GCP, developed and run inside the free-tier sandbox, with a documented path to managed Dataflow/Composer once billing is enabled." This is still fully defensible technically — the Beam code is unchanged, only the runner changes (DirectRunner vs DataflowRunner is a one-line/one-flag difference), which is itself worth stating explicitly in an interview.
**How I'd know this was wrong:** if 10 GB of BigQuery storage or 1 TB/month of query processing is exceeded — unlikely for 3 years of monthly *aggregated* national credit data, which is small (aggregated by dimension combinations, not one row per credit operation). If it is exceeded, the fallback is trimming to fewer years or enabling billing at that point, consciously.
**Not yet confirmed:** exact uncompressed size of the SCR.data yearly ZIPs — I didn't check this against a live download before writing the script. First real signal comes from running the ingestion script in Day 1.

**Update — billing enabled.** User activated a billing account on the original project (`credito-scr`) with a budget alert set at R$30. This reopens Cloud Storage and managed Dataflow.
**Decision:** Revert to the original architecture — GCS raw layer, Apache Beam pipeline with the option to run on DataflowRunner (managed), Cloud Scheduler/Composer for orchestration in a later wave.
**Rationale:** I made an informed, capped decision (a real cost ceiling, actively monitored) rather than an uncapped one. A R$30 alert on a batch job processing a few years of aggregated monthly data is very unlikely to be reached, and if it is, the alert fires before real damage. This also restores the stronger portfolio story: pipeline actually deployed to managed Dataflow, not only DirectRunner.
**Consequence:** The BigQuery-Sandbox-only redesign (previous entry) is shelved, not deleted — it stays in this log as a legitimate documented alternative and as evidence, for an interview answer, that I understand both the free-tier-constrained path and the production path, and made a conscious cost/tradeoff call.
**How I'd know this was wrong:** the R$30 alert fires. If it does, stop, check what triggered it (very likely a misconfigured Dataflow job left running, or too many years re-ingested), and only continue after understanding the cause — do not just raise the cap.

**Incident:** first real run of `ingest_raw.py` downloaded `scrdata_2023.zip` (173.6 MB, 12 monthly CSVs) fine, but the upload of one CSV to GCS failed with a timeout after 120s (`RetryError: Timeout of 120.0s exceeded`).
**Cause:** `blob.upload_from_string()` holds the whole decompressed CSV in memory and uses the client library's default per-chunk timeout (120s), which is too short for Cloud Shell's network when a single monthly file is large — national-level aggregated SCR data, broken down by UF × segmento × cliente × CNAE/ocupação × porte × modalidade × submodalidade × origem × indexador, is a lot of row combinations even though it is aggregated, not micro-data.
**Fix:** switched to writing each CSV to a local temp file in 8 MB streamed chunks, then `blob.upload_from_filename()` with `timeout=600` and an explicit 8 MB `chunk_size` (smaller chunks retry more gracefully than the library default on a flaky connection). Also made the upload step idempotent — skip any blob that already exists in the bucket — so a re-run after a partial failure does not re-upload files that already succeeded.
**Alternative considered:** splitting each CSV into smaller pieces before upload. Rejected for now — adds complexity for a one-off ingestion script, and the timeout/chunking fix is the more direct cause-to-fix match.
**How I'd know this was wrong:** if the retry still times out on the same file, the file itself may need to be split, or the Cloud Shell session may need to run the ingestion via `nohup` in the background so a dropped shell session doesn't kill a long upload.

**Day 1 complete.** All 36 monthly CSVs (12 months × 3 years, 2023-2025) uploaded successfully to `gs://credito-scr-raw/raw/scr_data/`, roughly 100 MB each, ~3.6 GB total. The background `nohup` run outlived the terminal tab it was checked from — a job-control quirk (job numbers are per-tab in Cloud Shell, not global), not a pipeline problem. No cost concern: Cloud Storage regional storage at this volume is a few cents a month, nowhere near the R$30 alert.

## Day 2 — Beam pipeline and bronze layer

**Question:** what decimal separator do the SCR.data CSV value columns use?
**My answer:** not confirmed yet — the methodology PDF documents column meaning but not literal number formatting, and I hadn't inspected a real row at this point. Assumed Brazilian convention: comma as decimal separator, period as thousands separator (e.g. `1.234.567,89`), flagged to confirm once real data loads.
**Rationale:** this is the standard for Brazilian government open data exports, and the fix is cheap either way — the pipeline includes a small `parse_brl_number` step that strips thousands separators and swaps the decimal comma for a dot before casting to float. The first real run (Step 1 below, one file, five printed rows) either confirms or disproves this before the pipeline touches all 36 files.
**How I'd know this was wrong:** parsed values coming out absurdly large (off by 1000x) or `ValueError` on cast — either means the format assumption is wrong and `parse_brl_number` needs adjusting to plain-dot-decimal instead.

**Question:** what format is `data_base` in the raw CSV?
**My answer:** assumed `AAAAMM` (year+month, no day, matching the filename pattern `scrdata_202501.csv` and the fact the dataset is monthly), flagged to confirm. Parsed as the first day of that month.
**How I'd know this was wrong:** date parsing throws, or the row count per file doesn't match a single competencia — checked in the same Step 1 validation.

**Confirmed wrong, fixed from real data.** The Step 1 test run (5-row sample, file `scrdata_202501.csv`) showed every row hitting the `Unrecognized data_base format` branch. Two real findings from the actual CSV, not documentation:
1. `data_base` is already ISO format (`2025-01-31` — the last calendar day of the competencia month), not `AAAAMM` as assumed. No reformatting needed, just validate and pass through.
2. Every field is wrapped in double quotes (`"Indisponível"`, `"Prefixado"`), which a naive `line.split(";")` does not strip — the quote character was stuck to the front of every value, including `data_base`, which is what actually broke the date parser (not the date format itself, though that assumption was also wrong).

**Fix:** replaced the manual `line.split(";")` with Python's `csv.reader` (delimiter `;`, quotechar `"`), which handles quoting correctly and would also handle a semicolon inside a quoted field if one ever appears (none seen so far, but the aggregation categories are free-text institution/CNAE names, so it is a real possibility for some UF or segment name — this is a case the naive split could never have handled safely, another reason to prefer the standard library parser over ad hoc splitting). Updated `parse_data_base` to accept `YYYY-MM-DD` directly.
**How I'd know this is still wrong:** if any `Skipping row with parse error` warnings remain after this fix, on the full run. Worth grepping the log for that string after Step 2 runs on all 36 files, not just eyeballing the first page of output.

**Confirmed from the passing test run:** the CSVs are UTF-8 (not Latin-1 as speculatively assumed in the earlier, now-shelved direct-BigQuery-load version of the ingestion script) — Portuguese accented characters (`Empréstimos`, `Cooperativa`) decoded correctly via Beam's `read_utf8()`. Files carry a UTF-8 BOM at the start, which made the header row fail the `is_header()` check and fall through to the row parser, where it was correctly (if noisily) dropped as malformed — not a data problem, just a header-detection gap. Fixed by stripping `\ufeff` before the header check.

**DirectRunner failed on the full 36-file run.** The 5-row sample test passed clean, but running the full dataset (~3.6 GB) on DirectRunner crashed with `grpc._channel._MultiThreadedRendezvous: DEADLINE_EXCEEDED`. DirectRunner's local execution goes through Prism, a portable-runner gRPC simulator running as a subprocess — fine for small samples, but Cloud Shell's small, ephemeral VM does not have the sustained CPU/memory to push 3.6 GB through that simulator within its internal gRPC deadlines.
**Decision:** move the full run to DataflowRunner (managed), exactly as the original sprint architecture planned for Day 4 — just earlier than scheduled, because the volume forced the issue now instead of later.
**Rationale:** this is the correct, expected boundary of DirectRunner — it is a correctness-testing tool for small samples, not a substitute for a distributed runner at real data volume. Reaching that boundary here, with billing already enabled and a budget alert in place, is a low-risk, well-timed moment to make the jump, not a failure of the plan.
**Plan:** validate on a single file via DataflowRunner first (proves the managed environment, GCS temp staging, and the BigQuery FILE_LOADS write path all work together) before committing to the full 36-file run, to keep both cost and time risk small.
**Expected cost:** a small batch job (1-2 `n1-standard-2` workers, roughly 3.6 GB processed, likely under 30 minutes of worker time) should land in the range of a few BRL at most — comfortably under the R$30 alert, but the alert stays as the real backstop, not this estimate.
**How I'd know this was wrong:** the Dataflow job also fails or costs run noticeably higher than expected — check the Dataflow job's execution graph and worker logs in the Console before increasing worker count or retrying blindly.

**Clarification on the record — two separate decisions, not one.** Worth stating explicitly because it's easy to conflate them in an interview narrative:
1. *Leaving BigQuery Sandbox (enabling billing)* was a proactive scope decision, not a response to hitting a limit — I chose to build the full GCS + managed Dataflow architecture the target job actually asks for, with a R$30 budget alert as the safety net. Sandbox alone was never going to run Dataflow at all (see the earlier billing-precondition-failure entry).
2. *Leaving DirectRunner* was a real technical limit, and came later, independently — DirectRunner correctly validated parsing logic on a 5-row sample, then failed outright (not just slow) on the full 3.6 GB run, hitting a `DEADLINE_EXCEEDED` in Prism's internal gRPC handling on Cloud Shell's small VM.
These should not be told as one story ("moved to the cloud because it was too slow") — that's not what happened and is a weaker, less specific answer than the truth.

**Open item — no before/after timing exists yet.** Because DirectRunner crashed rather than completing slowly, there is no real baseline duration to compare against a Dataflow run. Once the DataflowRunner test (single file) and full run complete, capture: total job duration, number of workers autoscaling actually used, and bytes processed. Do not write a "reduced processing time from X to Y" claim anywhere (README, LinkedIn post, interview notes) until these real numbers exist — a fabricated before/after is exactly the kind of unverifiable claim that falls apart under a follow-up question.

**Cloud Storage's role — two distinct functions in the same bucket, not one.**
1. *Landing zone for raw source data*: `gs://credito-scr-raw/raw/scr_data/` holds the CSVs extracted from the Bacen ZIPs — the actual business data, pre-transformation.
2. *Beam/Dataflow execution infrastructure*: `temp_location` (staging for `FILE_LOADS` before the BigQuery load job fires) and `staging_location` (where Dataflow packages and distributes the pipeline code to workers) are mechanism, not business data — they exist because of how the runner works, not because of anything about SCR.data.
Worth being able to explain this distinction cleanly if asked "why does the same bucket show up twice in your architecture."

**First DataflowRunner attempt failed: dataset didn't exist.** Single-file test on managed Dataflow failed with `HttpNotFoundError: Not found: Dataset credito-scr:staging`. `WriteToBigQuery` with `create_disposition=CREATE_IF_NEEDED` only creates the *table*, not the *dataset* — the dataset itself must already exist before the load job can run. Not caught earlier because Day 2 planning focused on the pipeline logic (parsing), not on pre-provisioning the destination dataset.
**Fix:** `bq mk --dataset --location=southamerica-east1 credito-scr:staging` before re-running.
**How I'd know this was wrong:** same error recurs after the dataset is created — would point to a permissions issue (the Dataflow worker service account lacking BigQuery dataset access) rather than a missing-resource issue.

**Second DataflowRunner attempt succeeded — `JOB_STATE_DONE`.** After creating the `staging` dataset, the single-file test ran to completion. Worth noting: the job hit `ZONE_RESOURCE_POOL_EXHAUSTED` three times in `southamerica-east1-a` (transient Compute Engine capacity shortage in that zone) before workers finally started, about 6 minutes into the job. Dataflow retried automatically — no action was needed, and this is a known characteristic of smaller regions like São Paulo, not a configuration mistake. Worth mentioning if asked about production reliability: a real production job might want `--zone` left unset (regional, not zonal) or a multi-zone fallback, which Dataflow already does by default — this was Dataflow's own retry behavior working as intended, not something the pipeline code needed to handle.

**Full-run attempt hit the same zone exhaustion, this time terminal.** The single-file test recovered from `ZONE_RESOURCE_POOL_EXHAUSTED` after 4 retries; the full 36-file run hit the same error 6 times over ~5 minutes in `southamerica-east1-a` and gave up (`JOB_STATE_FAILED`, no workers ever started). This is a genuine Compute Engine capacity shortage in that specific zone at that specific time, not a pipeline defect — no code ran, since no worker ever came up.
**Fix:** retry (zone selection can land differently on a fresh submission), and if that fails again, pin `--worker_zone` to a different zone in the same region (`southamerica-east1-b` or `-c`) to route around the exhausted zone explicitly.
**How I'd know this was wrong:** if a specific zone fails repeatedly across multiple job submissions on different days, that would suggest a persistent capacity constraint worth reporting to GCP support, not just bad luck — but one bad afternoon in a smaller region is not enough evidence for that conclusion.

**Day 2 complete — bronze layer loaded and verified.** `scr-data-bronze-full-2` finished `JOB_STATE_DONE` in 25 min 52 s (1-2 `n1-standard-1` workers, autoscaled). `staging.scr_data` holds 11,191,990 rows across all 36 source files, spanning `2023-01-31` to `2025-12-31` — exactly the 3-year window planned in Day 1, with no gaps or duplicate competencias. This closes the earlier open item about not having real before/after numbers: this is the actual, grounded figure to cite (~11.2M rows of national aggregated credit data processed via managed Dataflow in ~26 minutes), not an estimate.

**Verified against BigQuery:** `staging.scr_data` after the single-file load holds 312,839 rows, all with `data_base = 2025-01-31` (min = max, confirming no cross-competencia contamination). First real, grounded number for this project — safe to cite as "one month of national aggregated SCR.data is ~313K rows across UF × segment × client type × modality × ... combinations" in the README or an interview answer.

**Question:** DirectRunner (local) or DataflowRunner (managed) for this stage?
**Answer:** DirectRunner for now, same reasoning as the original sprint plan — develop and validate the transform logic locally where iteration is fast and free, and only pay for managed Dataflow once the pipeline is proven correct on real data. Switching runners is a one-flag change (`--runner=DataflowRunner`), not a rewrite.
**Consequence:** Day 2's BigQuery load still goes through a real `WriteToBigQuery` transform using `FILE_LOADS` (which stages temp files in GCS, same bucket, `tmp/` prefix) — so the only thing that changes when this moves to managed Dataflow later is the `--runner` flag and worker settings, not the pipeline logic itself. That parity is worth stating explicitly in an interview.

## Correction — no dbt used yet at this point

I'd described this project as following "dbt best practices." That wasn't accurate, and it's worth correcting explicitly: every SQL statement so far ran directly via `bq query` / `bq mk` against BigQuery. There was no dbt project, no dbt models directory, no `dbt run`, no dbt tests or docs generation. dbt wasn't among the target job's listed requirements (BigQuery, Dataflow, Python, LookML, data governance — dbt isn't mentioned), and introducing a new tool this late in a two-day-remaining sprint would have been scope creep the timeline couldn't absorb safely.

The Day 3 star schema is plain SQL DDL/DML executed in BigQuery, not a dbt model. This distinction matters for interview honesty — claiming dbt experience I hadn't actually exercised in this project is exactly the kind of gap a follow-up question ("walk me through your dbt project structure") would expose immediately.

## Day 3 — scope change before first run: sector analysis + interest rate

**Context:** I want two additions before running `star_schema.sql` for the first time: (1) ability to analyze indebtedness by client economic sector (agro, retail, industry — matches my real Bradesco segmentation experience), and (2) relate credit metrics to the Selic interest rate over time, to analyze how sectors were affected by rate hikes. Also flagged: the test files uploaded to the bucket during Day 1/2 debugging need cleanup — explicitly deferred, not urgent.

**Question:** keep `cnae_ocupacao` bundled inside `dim_cliente`, or split it into its own dimension?
**Answer:** split it out into `dim_atividade_economica`.
**Rationale:** `cnae_ocupacao` in SCR.data is a sector-level category (e.g. "Indústrias de transformação"), not a full CNAE code — good enough for the requested agro/varejo/indústria cut. Keeping it bundled with `porte` and `cliente` (PF/PJ) inside one dimension forces every by-sector query to either ignore those columns (wasteful join) or group by a combination that mixes concerns. Splitting it out is the standard star-schema move when a dimension attribute becomes a first-class analysis axis on its own — which "by sector" analysis just made it.
**Consequence:** `dim_cliente` now holds only `cliente` (PF/PJ) and `porte`; `dim_atividade_economica` is new. `fato_operacao_credito` gains an `atividade_economica_id` foreign key. This is a pre-run schema change — no data has been loaded into `marts` yet, so no migration cost, just a design correction before the first execution.
**How I'd know this was wrong:** if `cnae_ocupacao` and `porte`/`cliente` are almost always queried together in practice, re-merging would reduce join complexity — revisit after I actually use the marts layer for a few queries.

**Question:** which Selic series, and at what granularity, to join against monthly credit data?
**My answer:** Bacen SGS series 432 (Meta Selic definida pelo Copom), taking the rate value as of the last day of each competencia month, joined to `dim_tempo`, flagged to verify.
**Rationale:** series 432 is the Copom-set target rate — the one referenced in "aumento da taxa de juros" news and analysis, as opposed to the daily effective overnight rate (a different, noisier series). Using end-of-month value aligns with `data_base` already being the last day of each competencia.
**Not verified:** I hadn't called `api.bcb.gov.br` yet at this point, so the series code and the shape of the API response weren't confirmed against a real call. The fetch script prints a data preview before loading anything into BigQuery, mirroring the Day 2 pattern (test small, verify, then scale) — I should eyeball the first output before trusting it.
**How I'd know this was wrong:** printed sample values don't look like plausible Selic rates (expected range roughly 2%-15% a.a. across 2023-2025), or the API response shape doesn't match what the script expects — in either case, stop and adjust before loading.

**Deferred, not urgent:** clean up test/debug files left in `gs://credito-scr-raw/` from the Day 1-2 troubleshooting (partial uploads, single-file Dataflow test runs' staging artifacts under `staging/scr-data-test-single*` and `tmp/`). Explicitly postponed — flagged here so it doesn't get forgotten before the project is presented as a portfolio piece, since leftover debug artifacts in the bucket would look sloppy in a walkthrough.

## Day 3 (continued) — dbt introduced mid-sprint

**Context:** I received a second job offer whose requirements include everything already built plus dbt. Decision needed: work dbt into `credito-scr`, or treat it as out of scope for this project.
**Decision:** work it in, replacing the raw-SQL star schema (`star_schema.sql`, written earlier the same day) with an equivalent dbt project (`dbt_credito_scr/`).
**Rationale:** this is not scope creep for its own sake — it maps cleanly onto the architecture already in place. Dataflow does EL (extract-load: Bacen → GCS → `staging.scr_data`); dbt now does the T (transform: staging → star schema), which is exactly the ELT pattern dbt is built for. I already have real dbt experience (dbt Fundamentals study, dbt Cloud + BigQuery setup from an earlier project) — this is not a cold start with a new tool, it's applying a tool I already know to a project that already has the right shape for it.
**What changed:**
- `models/staging/stg_scr_data.sql` — thin passthrough over `staging.scr_data` (a `source()`, since Beam/Dataflow already did type parsing in Day 2 — dbt does not re-parse raw data here, it starts from an already-clean typed table).
- Five dimension models (`dim_tempo`, `dim_uf`, `dim_modalidade`, `dim_cliente`, `dim_atividade_economica`) and `fato_operacao_credito`, each a direct dbt-model translation of yesterday's raw SQL, using `ref()` to build the dependency graph instead of manual `CREATE TABLE` statements run in a fixed order.
- `fato_operacao_credito` keeps the same BigQuery partitioning (`data_base`) and clustering (`uf`, `modalidade_id`) via dbt's `config()` block — the physical table design didn't change, only how it's declared and orchestrated.
- The four manual quality-check queries at the end of `star_schema.sql` are replaced by native dbt tests (`not_null`, `unique`, `relationships`, `dbt_utils.accepted_range`) in `_schema.yml` — `dbt test` now does what those manual `SELECT COUNT(*) WHERE ...` queries did, which is a stronger, more standard signal to show in an interview than ad hoc validation queries.
- `dim_taxa_selic` (loaded by `fetch_selic.py`, outside dbt) is declared as a second `source()`, not a dbt model — dbt did not fetch it and should not claim to have.
**Consequence:** `star_schema.sql` is now superseded by the dbt project for anything going forward, but is kept in the repo as-is — it documents the same modeling decisions and is evidence I can model in plain SQL first, then formalize it in dbt, which is itself a legitimate interview point (dbt is a layer on top of SQL competency, not a replacement for understanding it).
**How I'd know this was wrong:** `dbt run` fails, or `dbt test` fails on `relationships`/`not_null` in a way the raw SQL run yesterday did not — since the SQL logic is a direct translation, any new failure points to a dbt-specific issue (source/ref wiring, BigQuery OAuth profile) rather than a modeling error already resolved in Day 3's first pass.

## Day 3 complete — dbt run and test, verified

`dbt run`: 7 models built (6 tables, 1 view), 0 errors. `dbt test`: 12/12 PASS, 0 failures. Verified against BigQuery directly (not just trusting dbt's own success message):

| table | rows |
|---|---|
| fato_operacao_credito | 11,191,990 |
| dim_modalidade | 66 |
| dim_cliente | 14 |
| dim_atividade_economica | 30 |
| dim_taxa_selic | 36 |

`fato_operacao_credito` row count matches `staging.scr_data` exactly (11,191,990 = 11,191,990 from Day 2) — confirms no rows were silently dropped by the dimension joins, which is exactly what the `relationships` tests were designed to catch and did not flag.

Non-blocking: dbt logged 3 deprecation warnings (`loaded_at_field` should move under `config()` in `_sources.yml`; `relationships` test arguments should nest under `arguments`). Cosmetic, does not affect correctness on the current dbt version — worth fixing before Day 5 polish, not urgent now.

## Day 4 — English localization decision

**Question:** the target roles are international ("vaga na gringa") — should all project artifacts be in English, including column and table names?
**Answer:** documentation, comments, and the dbt-layer schema (staging model onward) move to English. The bronze layer (`staging.scr_data`, loaded by Beam/Dataflow) keeps its original Portuguese column names, matching the SCR.data source schema exactly.
**Rationale:** renaming bronze columns would mean re-running the Day 2 ingestion (Beam schema, BigQuery load) purely for cosmetic reasons — no functional gain, real time cost, on a tight remaining timeline. More importantly, keeping the raw/bronze layer faithful to the source schema is standard medallion-architecture practice, not a shortcut: bronze exists to mirror the source of record, and translation belongs in the modeling layer where the data is being shaped for consumption. dbt's staging model is exactly that seam — `stg_scr_data.sql` now aliases every Portuguese source column to an English name, and everything downstream (`dim_*`, `fact_*`) inherits English naming through dbt's `ref()` graph, touching only the modeling layer, not the ingestion pipeline.
**What changed:** dbt project renamed throughout — `stg_scr_data` aliases columns to English (e.g. `data_base` → `reference_date`, `carteira_inadimplencia` → `default_portfolio`, `cnae_ocupacao` → `economic_activity`). Model files renamed to English (`dim_tempo` → `dim_date`, `dim_cliente` → `dim_client`, `fato_operacao_credito` → `fact_credit_operation`, etc.). All SQL comments translated. `_schema.yml` and `_sources.yml` updated to match. README and `decisions.md` were already English throughout (per the original decision-log instruction).
**Consequence:** `star_schema.sql` (the pre-dbt raw-SQL version, already superseded per the earlier Day 3 entry) is left with its original Portuguese column names — it documents what was actually run that day, not rewritten retroactively. Its comments are translated to English for readability, but the SQL itself stays historically accurate.
**How I'd know this was wrong:** an interviewer or reviewer opening `staging.scr_data` directly (bypassing dbt) finds the Portuguese names confusing without context — mitigated by documenting this explicitly in the README's architecture section, so it reads as a deliberate medallion-layer decision, not an oversight.

## Day 4 complete — English rename verified, bucket cleaned

`dbt run`/`dbt test` re-run after the English rename: 8 models built, 12/12 tests PASS. `fact_credit_operation` still 11.2M rows / 1.9 GiB processed, matching Day 3's bronze count — confirms the rename didn't silently change row counts. Orphaned Portuguese-named tables (`dim_tempo`, `dim_cliente`, `dim_modalidade`, `dim_atividade_economica`, `fato_operacao_credito`) dropped from `marts` via `bq rm`.

One deprecation warning remained after the first rename pass (`dbt_utils.accepted_range` also needed its arguments nested under `arguments:`, same fix already applied to `relationships` — missed on the first edit). Fixed. Bucket cleanup (`gs://credito-scr-raw/staging/`, `gs://credito-scr-raw/tmp/`) verified empty via `gcloud storage ls` returning "matched no objects" — expected result, not an error. `raw/scr_data/year=2023,2024,2025` confirmed untouched.

## Day 4 — orchestration scope

**Question:** how should the monthly refresh be automated — Cloud Composer, or Cloud Scheduler + Cloud Function?
**Answer:** Cloud Scheduler + Cloud Function, as decided back in the original sprint plan (see the very first architecture entries) — Composer's always-on environment cost is the first thing to cut when time/budget is tight, and a monthly trigger doesn't need a full workflow orchestrator.
**Design problem solved:** SCR.data ships as one ZIP per YEAR, not per month. A naive "re-run the whole pipeline monthly" would re-download and re-append all 12 months every time, duplicating already-loaded data (the Beam write uses `WRITE_APPEND`). The Cloud Function instead computes the target competencia (previous month), extracts only that month's CSV from the annual ZIP, uploads it only if not already present (idempotent), and would trigger Dataflow scoped to only that new file — so `WRITE_APPEND` stays correct.
**Assumption flagged, not verified:** Bacen's publication lag for a given competencia. The function assumes roughly 30-45 days and is meant to be scheduled around day 20 of each month, targeting the previous month — not confirmed against a real observed historical publication pattern at this point. If a scheduled run finds the target month not yet in the ZIP, it exits cleanly (200, not an error) and logs that it will retry next month — this was a deliberate design choice, not an oversight: a monthly job hitting "not published yet" is an expected, non-exceptional outcome, not a failure to alert on.
**Known gap, explicit not silent:** actually launching the Dataflow job from inside the Cloud Function requires a Dataflow Flex Template (packaging the Beam pipeline as a container the Dataflow API can invoke) — this was not built in this sprint. The function's `launch_dataflow_job()` is a documented placeholder that logs what it would do, rather than a fake API call that looks complete but would fail. This is the honest state of the automation: file detection and idempotent upload work end-to-end; the Dataflow trigger step is designed but not wired.
**Also not automated:** `dbt run` (staging → marts refresh) is not triggered by this function. A production version would likely add this as a Cloud Run Job (better suited to dbt's runtime needs than a Cloud Function) invoked after the Dataflow job completes, or moved into Composer once the pipeline has more than two steps to sequence — noted as a next step, not implemented.
**How I'd know this was wrong:** if asked in an interview "does this run in production," the honest answer is "the trigger and idempotent ingestion logic are built and would work; the Dataflow launch and the dbt refresh step are designed and documented but not wired end-to-end" — that is the true state, and is a stronger answer than overclaiming a fully automated pipeline that was not actually built end-to-end in five days.

## Day 4 — orchestration deploy deferred

**Decision:** I chose to skip actually deploying the Cloud Function + Scheduler for now (code and design are done, see previous entry) and move to the Looker Studio dashboard instead.
**Rationale:** with limited days left, dashboard + narrative likely matters more for the Friday deadline than a live-deployed scheduler that would sit idle anyway (no new competencia to actually process before the interview). The code remains in the repo as evidence of the design.
**Consequence:** if asked "is this actually running on a schedule," the honest answer is "the trigger logic is written and tested for its idempotency behavior conceptually, but not deployed" — same honesty pattern as the Dataflow-launch gap noted above. Do not claim it's live.

## Day 4 — Cloud Function deploy skipped for now

I chose to move to Looker Studio before deploying/testing the Cloud Scheduler + Cloud Function orchestration. The code and deploy steps are written and ready (`cloud_function/main.py`, deploy commands in the prior chat turn) but not yet run. Flagged explicitly so this doesn't get presented as "done" in the README without actually having run — matches the project's standing rule of not claiming untested work.

## Day 4 — Looker Studio prep

**Question:** connect Looker Studio directly to `fact_credit_operation` and its dimensions (multiple joins in-tool), or pre-aggregate into a single reporting view?
**Answer:** pre-aggregate. New dbt model `rpt_sector_default_by_month` joins fact + `dim_date` + `dim_economic_activity` + `dim_selic_rate` and aggregates to sector × month, exposing exactly the cut requested (indebtedness by sector vs. Selic).
**Rationale:** Looker Studio's free tier handles single-table connections far more reliably than multi-table blends/joins configured in the tool itself — blends are a known source of silent miscalculation (e.g. double-counting on fan-out joins) if the join keys and cardinality aren't exactly right. Doing the join once in dbt, where it's tested and version-controlled, is both safer and matches how a real analytics-engineering team would hand off a BI-ready table rather than asking a dashboard tool to reimplement warehouse logic.
**Consequence:** this is a `reporting` layer sitting on top of `marts` — not a new architectural layer, just a purpose-built aggregate for one specific dashboard need. If more dashboards are added later, each with its own reporting shape, worth reconsidering whether a `models/reporting/` subfolder makes sense, rather than mixing reporting views into `marts/`.
**Note on Looker vs. Looker Studio, restated:** the target job asks for Looker/LookML specifically. Looker Studio is not the same product — it has no LookML semantic layer, no Explores, no access control model. This substitution was already flagged as a deliberate, honest trade-off back in the original sprint plan (no Looker license available in the sandbox), not something to represent as equivalent in an interview.

## Day 4 — Looker Studio: join refinement + dashboard config catch

**Change:** I changed the `dim_selic_rate` join in `rpt_sector_default_by_month` from exact `reference_date` equality to `date_trunc(..., month)` equality.
**Assessment:** a good change to keep, independent of whether it fixed a specific observed bug — both sources already normalize to month-end dates today, so the exact-equality join was very likely already correct in practice. `date_trunc(month)` makes the real join intent explicit ("match by month") rather than relying on an implicit, undocumented assumption that both sides always represent the month the same way. No fan-out risk introduced: `dim_selic_rate` still has exactly one row per month, so the join stays 1:1.
**Separate, more consequential catch:** the Looker Studio time series chart was built with `reference_date` as the only dimension — no `sector` breakdown. That means `default_rate` and `selic_target_rate_annual` were being averaged across all 30 sectors per month, which is exactly the analysis I did NOT want: the whole point of `rpt_sector_default_by_month` was to see sector-level variation, and an unbroken-down time series erases it. Fix: add `sector` as a breakdown dimension on the time series chart. With 30 sectors this will be visually noisy as a single chart — recommended a filter control (by sector) so I can toggle which sectors show, rather than rendering 30 lines at once.
**How I'd know this was wrong:** after adding the `sector` dimension, if the lines all still look identical/flat, the aggregation or the dimension add didn't actually take effect in Looker Studio — re-check the chart config, not the underlying SQL.

## Day 4 — dashboard reframed around default_rate, not portfolio size

**Context:** I caught that the bar chart and table were sorted/focused on `active_portfolio_total` (portfolio size) while the actual analysis goal is default rate vs. Selic. Also: the time series chart with `sector` as a breakdown dimension rendered blank — likely too many series (30 sectors × 36 months) for Looker Studio's chart to plot, and the "Máximo de linhas" legend setting (which I tried) only limits legend display rows, not which series actually get plotted — a common Looker Studio UI confusion, not a data problem.
**Decision:** (1) reconfigure the bar chart and table to sort by `default_rate` descending instead of `active_portfolio_total`, keeping portfolio size as a secondary/context column, not the primary sort. (2) add a new dbt model, `rpt_top_sector_default_by_month`, pre-filtered in SQL to the 8 sectors with the largest total credit exposure — rather than trying to solve "show only the biggest sectors" inside Looker Studio's chart configuration, which has proven fragile (blank chart, unclear legend-vs-series-limit behavior).
**Rationale for ranking by exposure, not by default_rate, when picking the top 8:** small sectors can have extremely volatile/noisy default rates (a handful of loans defaulting in a tiny sector swings the percentage wildly) — ranking the sample by market size keeps the comparison statistically meaningful, while `default_rate` remains the metric actually plotted. This is a real methodological choice worth being able to explain, not an arbitrary cut.
**Consequence:** the full 30-sector `rpt_sector_default_by_month` remains the source for the sector filter/dropdown (so any sector can still be explored on demand), while `rpt_top_sector_default_by_month` becomes the time series chart's default data source — no Looker Studio config workaround needed, the "show a readable sample" logic lives in dbt where it's testable and version-controlled.
**How I'd know this was wrong:** if a sector outside the top 8 by exposure turns out to be the most newsworthy story (e.g., a small sector with a dramatic default spike), it would be invisible in the sampled chart — worth keeping the full-sector table/filter alongside the sampled time series specifically so that case isn't hidden entirely.

## Day 4 — extended to 2026, using the existing idempotent pipeline instead of manual DML

**Question I asked myself:** did I need a DML script to insert the current year's already-published months?
**Answer:** no manual DML written — extended the existing idempotent pipeline instead (added 2026 to `ingest_raw.py`'s `YEARS` list, re-ran ingestion and Dataflow scoped to only `year=2026/*.csv`, re-ran `fetch_selic.py` with the extended date range, re-ran `dbt run`).
**Rationale:** the pipeline's GCS upload is already idempotent (skips existing blobs) and the Beam/Dataflow load is safe from duplication as long as `--input` is scoped to only the new year's files, not the full glob — this is the same pattern already built and documented for the Cloud Function's monthly automation (Day 4 orchestration entry). Writing a one-off manual `INSERT` statement would duplicate logic that already exists correctly, and risks getting out of sync with what the automated path does — two ways to load data is a maintenance liability, not a convenience.
**Consequence:** 2026 data now flows through the exact same path as 2023-2025 — no special-cased backfill logic exists anywhere in the project. This is also a better interview answer than "I wrote a DML script" — it demonstrates the pipeline was actually designed to be re-run incrementally, not a one-shot script that needed a bespoke workaround to extend.
**How I'd know this was wrong:** row counts for 2023-2025 change after this run (they should not) — would indicate the Dataflow `--input` scoping didn't work as intended and reprocessed already-loaded years.

## Backlog — explicitly deferred, revisit later

- **2026 backfill retry.** Third Dataflow attempt for `scr-data-backfill-2026` failed again with `ZONE_RESOURCE_POOL_EXHAUSTED`, this time in `southamerica-east1-a` and `-c` both across retries — worse than the earlier Day 2 occurrence (which recovered after 4 retries). Worth trying `--worker_zone=southamerica-east1-b` explicitly next time, per the fallback already documented earlier, or simply retrying at a different time of day if this is regional capacity pressure.
- ~~Cloud Function + Scheduler deployment~~ — done. Deployed and verified end-to-end in production; see the Day 5 entries below.
- ~~Dataflow launch wiring inside the Cloud Function~~ — done. Turned out not to need a Flex Template; see the Day 5 correction entry below.
- ~~`dbt run` after monthly ingestion~~ — done. Running as a Cloud Run Job on Cloud Scheduler; see the Day 5 entries below.
- **Dashboard polish** — I'm aiming for a more complete "Credit & Default Monitor" layout (KPI scorecards, state/UF map, sector ranking, detail table) — see the new entry below for the state-level gap specifically.

## Day 4 — state-level reporting mart added

**Context:** I'm targeting a more complete dashboard layout (reference: KPI scorecards, state/UF choropleth map, sector ranking, detail table), and identified that no state-level reporting mart existed — only sector-level.
**Decision:** added `rpt_state_default_by_month`, structurally identical to `rpt_sector_default_by_month` but grouped by `state` instead of `sector`. Kept as a separate mart rather than adding `state` as a second dimension to the sector mart, to avoid inflating that table's grain (27 states × 30 sectors × 36 months ≈ 29K rows) when the two cuts are being visualized separately, not cross-filtered together, in the current dashboard design. If a combined state × sector view becomes needed later, that is a deliberate reconsideration, not a default.
**Next step, not yet done:** the reference dashboard's map panel shows a single snapshot (most recent month), not a time series — `rpt_state_default_by_month` is still monthly time series shaped. In Looker Studio, this means adding a date-range filter/control pinned to the latest available month for the map chart specifically, rather than building a separate "latest snapshot" model — simpler and keeps one source of truth. Revisit if that filter approach proves awkward in practice.

## Day 4 — Selic extended to 2026, one data-quality flag

`fetch_selic.py` reloaded successfully with the extended range, now 45 months (2023-01 through 2026-09) in `dim_taxa_selic`. Note: the most recent record (2026-09-30) represents a still-in-progress month (today is 2026-09-03), not a closed month-end rate like all prior records. If the dashboard ever computes "vs. previous month" deltas on the Selic KPI card, this partial month should be excluded or clearly labeled as provisional — comparing a 3-day-old reading against a full closed month is not apples-to-apples.

Also worth noting explicitly: `fact_credit_operation` still only covers 2023-2025 (the 2026 Dataflow backfill failed on zone exhaustion, still pending — see backlog). So any mart joining Selic to the fact table will show 2026 Selic values with no matching credit data yet — expected, not a bug, until the backfill succeeds.

## Day 4 — combined state × sector mart, replacing separate narrow marts as dashboard source

**Question I asked myself:** how do I cross-filter the map (by state) with sector — the two separate marts (`rpt_state_default_by_month`, `rpt_sector_default_by_month`) can't do that; a join would need to happen somewhere.
**Decision:** built one wider mart, `rpt_credit_by_state_sector_month`, at state × sector × month grain, doing the join once in dbt rather than as a Looker Studio blend.
**Rationale:** this is the standard BI pattern — one sufficiently wide, pre-joined fact-shaped table, letting the BI tool aggregate away whichever dimension isn't used in a specific chart (a state-only map built from this table sums across all 30 sectors automatically; a sector-only bar chart sums across all 27 states). Looker Studio blends (joining two separate data sources inside the tool) are exactly the fragile pattern already avoided once this sprint (see the earlier "Looker Studio prep" decision) — doing it again here, just for state+sector, would be the same mistake twice.
**Consequence:** `rpt_state_default_by_month` and `rpt_sector_default_by_month` are now effectively superseded by this combined mart for dashboard purposes — kept in the project rather than deleted (still useful as smaller, simpler models for a walkthrough that doesn't need the full cross-filter, and deleting working models mid-sprint for tidiness isn't worth the risk this close to the deadline). If asked in an interview "why do you have three similar-looking marts," the honest answer is that they represent an iterative widening of scope during the sprint, not three independent design choices — worth being able to say plainly rather than implying it was planned that way from day one.
**Grain check:** ~27 states × ~30 sectors × 36 months ≈ 29,160 rows expected — small enough that no further pre-aggregation is needed.

## Day 4/5 — persistent zone exhaustion, escalating fallbacks

**Context:** the 2026 backfill Dataflow job failed a third time, this time exhausting both `southamerica-east1-a` and `-c` across ~10 retries over 5 minutes. This is no longer a one-off transient blip (the Day 2 occurrence recovered after 4 retries) — it looks like sustained capacity pressure on the default machine type in this region.
**Fallback plan, applied in order:**
1. Force `--worker_zone=southamerica-east1-b` (untried zone) combined with `--machine_type=e2-standard-2` (the `e2` family generally has better availability than `n1` — Google's current recommended default).
2. If that also fails, move the job's `--region` to `us-central1` for this one backfill run only. No data-residency concern here — SCR.data is public aggregate data, not PII — and the cost is negligible for a single ~180MB batch job. BigQuery datasets and the GCS bucket stay in `southamerica-east1`; only Dataflow's compute region changes for this run.
**How I'd know this was wrong:** if `us-central1` also exhausts, that would be surprising enough to warrant checking the GCP status dashboard for a broader outage rather than continuing to guess at zones/regions.

## Day 4/5 — bug found: --region flag was silently ignored

**Bug:** `beam_pipeline.py`'s `PipelineOptions(...)` call hardcoded `region="southamerica-east1"` as a keyword argument, in addition to passing through the parsed CLI args (`pipeline_args`). Beam gives explicit constructor kwargs priority over flags — so the `--region=us-central1` I passed on the command line was silently ignored. The job that "worked" actually ran in `southamerica-east1-c` (confirmed from the job's own logged pipeline options and worker configuration message), not `us-central1` as intended. What actually fixed the zone exhaustion was the `--machine_type=e2-standard-2` change, combined with `-c` happening to have capacity at that moment — not the region change, which never took effect.
**Fix:** removed the hardcoded `region=` kwarg from the `PipelineOptions()` constructor call, so `--region` on the command line is now respected.
**Cost implication, answered honestly:** since the job never actually left `southamerica-east1`, there is no cross-region cost to discuss for this run. The real cost delta is `e2-standard-2` (2 vCPU) vs. the earlier `n1-standard-1` (1 vCPU) — roughly double the compute rate per hour. For a single ~180MB one-year backfill that ran workers for about 8 minutes total, this is a few cents at most, not a meaningful cost concern. If I do need to actually run a job in a different region in the future (e.g. `us-central1` for real), this bug is now fixed and the flag will work as expected.
**How I'd know this was wrong:** if a future run with `--region=us-central1` still shows `southamerica-east1` in its logged pipeline options, the fix didn't take — re-check for any other hardcoded region reference in the script.

## Day 5 — correction: Flex Template was not actually needed

**Correction to the earlier "known gap" entry (Day 4 orchestration):** that entry claimed launching Dataflow from a Cloud Function required a Flex Template. On closer inspection, that's over-engineering for this case — a Cloud Function (2nd gen, built on Cloud Run) can install `apache-beam[gcp]` directly and submit a `DataflowRunner` job the same way the manual Cloud Shell runs did all sprint. Submitting a job (building the pipeline graph and calling the Dataflow API) takes seconds; actually running it happens asynchronously on Dataflow's own managed workers, not inside the function. Flex Templates solve a different problem (packaging a pipeline as a reusable container others can launch without the SDK installed) — not needed here, where the function and the pipeline code are the same codebase.
**What changed:** `cloud_function/main.py` now embeds the same parsing/pipeline logic as `beam_pipeline.py` and actually submits the Dataflow job (scoped to the one new file, `wait_until_finish=False` so the function returns quickly rather than blocking on the full job).
**How I'd know this was wrong:** if the function times out or runs out of memory installing/importing `apache-beam` — Cloud Functions Gen2 has generous limits (up to 60 min, several GB RAM), so this is unlikely for a job-submission-only workload, but worth checking the function's actual memory usage after the first real deploy.

## Day 5 — dbt automation: time-based, not event-driven

**Question:** how should `dbt run` fire after the monthly Dataflow ingestion completes?
**Decision:** a second Cloud Scheduler job, running dbt several hours after the ingestion trigger (same day, later time) — not an event-driven trigger firing exactly when the Dataflow job finishes.
**Rationale:** a fully event-driven design (Dataflow job completion → Pub/Sub notification → triggers dbt) is the more correct production pattern, but adds real orchestration complexity (Pub/Sub topic, a notification-consuming Cloud Function, handling out-of-order or duplicate events) that isn't worth building in the sprint's remaining time. Every observed Dataflow run this sprint finished well under 30 minutes; scheduling dbt 4 hours after ingestion is a generous, simple safety margin.
**Consequence:** this is a deliberate simplification, not a silent gap — if the monthly ingestion job is ever unusually slow (network issues, zone exhaustion retries eating 10+ minutes as seen this sprint), dbt could run before new data lands, silently rebuilding marts on stale data with no error raised. Worth revisiting with a real completion signal if this pipeline ever moves beyond portfolio/demo status.
**How I'd know this was wrong:** the dbt-run scheduled time keeps needing to be pushed later because ingestion occasionally still isn't done — that would be the signal to build the event-driven version instead of widening the time buffer indefinitely.

## Day 5 — Cloud Function deployed successfully

`scr-data-monthly-ingest` deployed, `state: ACTIVE`, URL: `https://southamerica-east1-credito-scr.cloudfunctions.net/scr-data-monthly-ingest`. Uses the default Compute Engine service account (`641184529196-compute@developer.gserviceaccount.com`), not a dedicated one — acceptable for a portfolio project, though a production deployment would typically use a purpose-scoped service account instead of the broad default one. Worth naming as a known simplification if asked in an interview.
Deployment hit two expected friction points, neither a code issue: (1) chained API enablement (Cloud Run, Cloud Build) on first deploy, and (2) a transient IAM propagation delay right after Cloud Build's service account was created — resolved by waiting ~2 minutes and retrying, exactly as GCP's own error message suggested.

## Day 5 — Cloud Function verified end-to-end in production

Manually triggered twice via Cloud Scheduler. Both runs correctly computed the target competencia (August 2026, previous month from execution date), downloaded the 2026 annual ZIP, found Bacen has only published through July 2026 so far, and exited cleanly with the expected "not published yet" message — no error, 200 response. This is the exact idempotent, graceful-no-op behavior designed back in the Day 4 orchestration entry, now confirmed working against the real Bacen API in a deployed Cloud Function, not just in Cloud Shell. IAM bindings (`dataflow.developer`, `iam.serviceAccountUser`) applied to the default Compute service account successfully — no permission errors encountered.

## Day 5 — Cloud Run Job bug: dbt_packages not baked into image

**Bug:** first Cloud Run Job execution failed with "dbt expects 1 package(s) ... but found only 0 package(s) installed". Cause: `.dockerignore` excludes `dbt_packages/` (correct, for the build context sent to Cloud Build — dependency artifacts shouldn't be uploaded as source), but nothing in the Dockerfile ever ran `dbt deps` INSIDE the container to regenerate it. `dbt deps` had only ever been run manually in Cloud Shell, outside the image entirely.
**Fix:** added `RUN dbt deps --profiles-dir /dbt` to the Dockerfile, after `COPY . /dbt` — this runs at image build time, baking `dbt_packages/` into the image so the job doesn't need network access to fetch packages at runtime.
**How I'd know this was wrong:** same "0 package(s) installed" error persists after rebuilding — would mean `dbt deps` itself failed silently during the build (worth checking the full `gcloud builds submit` output for errors around that step, not just the final SUCCESS status, if it recurs).

## Day 5 — Cloud Run Job verified end-to-end

`dbt deps` ran successfully during the image build (`Installed from version 1.4.1`), and `gcloud run jobs execute dbt-run-job --wait` completed successfully (`1 / 1 complete`) — `dbt run` executed inside the container against real BigQuery data, using the job's attached default Compute service account via ADC (no service account key file, no browser oauth flow needed). Both automation pieces (Cloud Function for ingestion, Cloud Run Job for dbt) are now deployed and verified working, not just designed.

Remaining: create the second Cloud Scheduler job (`dbt-run-trigger`) to fire this automatically ~4 hours after the monthly ingestion trigger — the Cloud Run Job itself is proven to work, only the recurring schedule for it is still pending.

## Day 5 — dbt Scheduler created

`dbt-run-trigger` created (fires day 20, 12:00 Brasília time, 4h after the ingestion trigger — see the earlier "time-based, not event-driven" decision). Manually triggered once; `gcloud scheduler jobs describe` showed an empty `status` field after the run (no error code), consistent with success — the initial `code: -1` seen right after creation is the scheduler's default "never yet executed" state, not an error, and shouldn't be misread as one. Confirming via `gcloud run jobs executions list` that a fresh execution actually landed on the Cloud Run Job side, not just trusting the scheduler's own status field.

## Day 5 — automation fully verified end-to-end

`gcloud run jobs executions list` confirms `dbt-run-job-8t9rv` (created 2026-09-08 17:51:06 UTC, immediately after triggering the scheduler) was run by the service account (`641184529196-compute@developer.gserviceaccount.com`), not by the human user — proof the Cloud Scheduler → Cloud Run Job path works autonomously, distinct from the two prior manual executions run by `monte.lucas@gmail.com`. Both automation legs (monthly ingestion via Cloud Function, dbt refresh via Cloud Run Job) are now deployed, scheduled, and confirmed working end-to-end, not just designed. Sprint's core engineering scope is complete; remaining work is presentation (dashboard polish, GitHub publication, README).

## Early warning module — portfolio-level behavior model

**Question:** can this project support credit modeling (application or behavior scoring), not just engineering?
**Answer:** behavior, at portfolio/segment level — yes. Application (concessão) scoring — no.
**Rationale:** SCR.data is aggregated (state × segment × client type × activity × size × modality × origin × indexer). There is no individual borrower, no application-time snapshot, no borrower-level outcome. An application scorecard needs all three. Forcing one onto this data would be a mislabeled model, and that is the first thing a credit team would catch. What the data *does* support honestly is the portfolio-monitoring question risk teams actually ask every month: *which segments are going to deteriorate?* That is a legitimate behavior/early-warning use case (portfolio monitoring, segment risk appetite, provisioning), and it is what this module builds.

**Design:**
- **Grain:** segment = state × credit modality × client type × client size, monthly. Economic activity left out of the grain on purpose — it multiplies segments by ~30 and leaves most of them too small for a stable default rate.
- **Target:** `target_deterioration = 1` if the segment's default rate at t + 6 months rises by ≥ 0.5 p.p. **and** ≥ 15% relative to month t. The two conditions together keep noise in low-rate segments (e.g. mortgage, ~1%) from counting as deterioration. Thresholds are dbt vars, not hardcoded.
- **Features (months ≤ t only):** current default rate, early delinquency rate (15–90 days overdue ÷ active portfolio — the roll-rate signal that precedes 90+ default), 3- and 6-month changes, 6-month portfolio growth, average balance per operation, portfolio size, Selic level and 6-month change.
- **Minimum segment size:** R$ 10 mi active portfolio — smaller segments produce default rates dominated by a handful of operations.
- **Validation:** out-of-time. Test = last 6 labeled months; train = only months whose target window closes before the first test month (no overlap between a training target and the test period). Random splits would leak future macro conditions into training.
- **Models:** logistic regression baseline first, gradient boosting second; the one with the higher KS is kept. Metrics reported: ROC AUC, Gini, KS, PR-AUC, decile table with lift.

**Upstream change, stated plainly:** two columns already present in bronze since Day 2 (`vencido_de_15_ate_90_dias`, `vencido_acima_de_90_dias`) were exposed through `stg_scr_data` and `fact_credit_operation`. Additive columns only — no existing column, join, or test changed, and the row-count check still applies. The two new models live in `models/early_warning/`, plumbed in via `ref()`, same additive pattern as the Selic change.

**Known limitations:**
- Consecutive months of the same segment are autocorrelated, so the effective sample size is smaller than the row count suggests; metrics should be read as indicative, not as a production-grade validation.
- ~43 months of history (2023-01 to 2026-07) covers one Selic cycle, not a full credit cycle — the model has never seen a downturn like 2015–2016 or 2020.
- Scoring (`scripts/train_early_warning.py`) is run manually for now; the feature table refreshes monthly with the existing Cloud Run dbt job once the image is rebuilt, but training/scoring is not scheduled.

**How I'd know this was wrong:** the base rate of `target_deterioration` comes out below ~3% or above ~40% (thresholds need recalibrating), or the gradient boosting beats the baseline by a wide margin on training but not out-of-time (overfitting to segment identity).

## Dashboard: Looker Studio, page 1

**Question:** what goes on the dashboard, and how much of the model does it show?
**My answer:** one page built for a monthly risk review: key metrics, a map by state, breakdowns by client type and product, a delinquency trend by risk band and a ranked watchlist of every scored segment.
**Rationale:** the post that announced the model promised the flagged segments "on a map and a monthly watchlist that a risk analyst can open without writing SQL." Page 1 delivers that and nothing beyond it.

**Question:** how to present the dashboard in English when the source labels are in Portuguese?
**My answer:** a BigQuery view (`ew_segment_scores_en`) that translates product, client type, client size and risk band, keeping the same column names. Looker Studio points at the view, so no chart had to be rebuilt.
**Alternatives considered:** calculated fields in Looker Studio (one CASE per field, repeated in every data source); translating inside the Python scoring script (mixes presentation into the model output).
**Rationale:** one place to maintain, versioned in `sql/looker_views.sql`, and the model tables stay untouched.

**Question:** the dashboard should show the forecast. Should the trend lines be extended into the next 6 months?
**My answer:** no. The model predicts which segments will deteriorate within 6 months; it does not predict monthly delinquency values. Drawing future values would display something the model never produced. The trend chart shows history only, and the "next 6 months" wording sits on the watchlist, which is where the prediction lives.
**Consequence:** a monthly forecast needs a separate time-series model (BigQuery ML ARIMA_PLUS is the obvious candidate). Logged as a later step, not part of this release.

**Question:** why did clicking a month on the trend chart empty every other chart?
**My answer:** cross-filtering. The trend chart covers 37 months; every other chart reads only the latest month. A click on any month other than Jul 2026 filtered the rest of the page down to zero rows, and a click on a Medium or Low point also filtered out the High-only donut. Cross-filtering is off on the trend chart and stays on for the donut and bar chart, where filtering the map and watchlist by product or client type is useful.

**Known gap:** the two views were created in the BigQuery console, not as dbt models. They are versioned in `sql/looker_views.sql`, but nothing tests them or rebuilds them in the monthly job. Moving them into dbt is the next cleanup.


## dbt run → dbt build on the scheduled job

**Context:** the Cloud Run Job's Dockerfile ran `dbt run` as its entrypoint. That builds every model but doesn't test any of them.

**Problem:** the 12 dbt tests only ever ran when I typed `dbt test` by hand in Cloud Shell. On the actual monthly schedule, Cloud Scheduler triggers the Cloud Run Job, which only ran `dbt run` — so if a future month's data broke a `not_null` or `relationships` test, nothing would catch it. Bad rows would flow straight into the marts and onto the Looker Studio dashboard with no alert.

**Decision:** changed the Dockerfile entrypoint from `dbt run` to `dbt build`. `dbt build` runs each model and its tests together, in dependency order, and stops on a failure instead of continuing past it.

**Rationale:** this is exactly the case `dbt build` exists for — an unattended, scheduled job with nobody watching the output. `dbt run` on its own is fine for manual, supervised work where I'd run `dbt test` right after anyway; it's the wrong choice for something Cloud Scheduler fires once a month with no human in the loop.

**How I'd know this was wrong:** the job starts failing on a false positive (a real schema drift that `dbt build` correctly blocks but that doesn't actually matter for the marts) often enough that it becomes noise instead of a signal — would mean some test is too strict for this job and needs a specific `--select` scope, not a full revert to `dbt run`.
