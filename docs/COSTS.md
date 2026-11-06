# What it costs to run a pipeline on awebo + tundra

> **TL;DR** — a daily batch pipeline on this stack costs **about 2 cents a
> day** (≈ US$0.50/month). An hourly pipeline over 100 GB of data costs
> **under a dollar a day**. Compute is almost never the dominant line item;
> S3 request fees and storage are.

awebo (runtime) + tundra (infrastructure) are a framework for anyone who
wants efficient, extremely cheap data pipelines: no clusters, no warehouse
licence, no per-DPU billing, no idle servers. You pay for the seconds a
Fargate task runs, the objects it touches, and the bytes it keeps.

All prices below are **us-east-1 list prices as of November 2026**, taken
from the public pricing pages linked at the bottom. Fargate Spot rates
fluctuate; treat them as ±20%. Re-check the links before budgeting.

## Unit prices used

| Service | Price | Notes |
| --- | --- | --- |
| Fargate (x86, on-demand) | $0.04048 / vCPU-hour, $0.004445 / GB-hour | per-second billing, 1-minute minimum |
| Fargate Spot (x86) | ≈ $0.01254 / vCPU-hour, ≈ $0.001377 / GB-hour | ≈ 69% off on-demand; tundra defaults to `FARGATE_SPOT` |
| Public IPv4 address | $0.005 / hour while attached | tundra's VPC has no NAT, so each task holds one public IP while it runs |
| S3 Standard storage | $0.023 / GB-month | first 50 TB |
| S3 requests | PUT/COPY/POST/LIST $0.005 per 1,000; GET $0.0004 per 1,000 | Iceberg commits are PUT-heavy |
| Glue Data Catalog | $0 | first 1M objects and 1M requests/month are always free |
| CloudWatch Logs | $0.50 / GB ingested, $0.03 / GB-month stored | 5 GB/month ingest is in the free tier |
| EventBridge Scheduler | $0 | 14M invocations/month are free |
| SSM Parameter Store (standard) | $0 | tundra publishes resource IDs here |
| ECR (private image) | $0.10 / GB-month | or **$0** on `ghcr.io` for the public awebo image |
| VPC (no NAT, no endpoints) | $0 | tundra's default |

Cloudflare R2 alternative (awebo `AWEBO_CATALOG_TYPE=cloudflare`, tundra
`modules/cloudflare/datalake-buckets`):

| Service | Price | Free allowance / month |
| --- | --- | --- |
| R2 storage | $0.015 / GB-month | 10 GB |
| R2 Class A ops (writes) | $4.50 / million | 1 million |
| R2 Class B ops (reads) | $0.36 / million | 10 million |
| R2 egress | $0 | — |
| R2 Data Catalog (Iceberg REST) | no charge beyond storage + ops while in beta | Cloudflare has announced catalog-operation and compaction pricing; check the link below |

## Scenario A — one daily batch job (the common case)

Assumptions: 1 run/day, task sized at tundra's default **0.5 vCPU / 1 GB**,
runs for **5 minutes** on Fargate Spot, reads a few source files and
appends to 3 Iceberg tables, 10 GB of data kept in S3, 5 MB of logs per
run, private ECR image of 600 MB.

| Line item | Per run | Per day | Per month (30 d) |
| --- | ---: | ---: | ---: |
| Fargate Spot compute (0.5 vCPU + 1 GB × 5 min) | $0.00064 | $0.00064 | $0.019 |
| Public IPv4 (5 min) | $0.00042 | $0.00042 | $0.013 |
| S3 requests (≈500 PUT + 5,000 GET) | $0.0045 | $0.0045 | $0.135 |
| CloudWatch Logs (5 MB) | $0.0025 | $0.0025 | $0.075 (likely $0 under free tier) |
| S3 storage (10 GB) | — | $0.0077 | $0.23 |
| ECR image (0.6 GB) | — | $0.002 | $0.06 |
| Glue catalog, scheduler, SSM, VPC | $0 | $0 | $0 |
| **Total** | **≈ $0.008** | **≈ $0.018** | **≈ $0.53** |

Just under **two cents a day**. The same task on on-demand Fargate instead
of Spot adds $0.0014/run — still a rounding error. Notice that compute is
8% of the bill; S3 requests and storage are 70%.

## Scenario B — hourly pipeline, 100 GB lake

Assumptions: 24 runs/day, **2 vCPU / 4 GB**, **10 minutes** each on Spot,
≈2,000 PUT + 20,000 GET per run, 20 MB logs per run, 100 GB in S3.

| Line item | Per run | Per day | Per month |
| --- | ---: | ---: | ---: |
| Fargate Spot compute (2 vCPU + 4 GB × 10 min) | $0.0051 | $0.122 | $3.67 |
| Public IPv4 (10 min) | $0.0008 | $0.020 | $0.60 |
| S3 requests | $0.018 | $0.432 | $12.96 |
| CloudWatch Logs (20 MB) | $0.010 | $0.240 | $7.20 (first 5 GB/month free) |
| S3 storage (100 GB) | — | $0.077 | $2.30 |
| **Total** | **≈ $0.034** | **≈ $0.89** | **≈ $27** |

Under a dollar a day for an hourly pipeline over 100 GB. For comparison,
the smallest always-on managed warehouse or Spark cluster starts at
several dollars *per hour*.

## Scenario C — zero cloud compute

Because awebo is a plain Python entrypoint, the task does not have to run
on Fargate at all:

- **GitHub Actions `schedule`** on a public repository: 2,000 free minutes
  per month. A 5-minute daily job uses 150. Compute: **$0**.
- **Cloudflare R2 + R2 Data Catalog** for storage and catalog: the free
  allowances above cover a 10 GB lake with ~30k writes/month. Storage and
  catalog: **$0**.

Total: **$0/month** for a small lake. Tundra is then only needed when you
outgrow the free tiers and want the Fargate/S3/Glue substrate.

## Where the money actually goes (and how to spend less)

1. **S3 request fees dominate.** Every Iceberg commit writes data files,
   manifests, a manifest list and a metadata JSON. Batch your writes
   (one `to_lake` per table per run, not one per chunk) and let awebo's
   streaming writer size the Parquet files. Fewer, larger files also make
   reads cheaper.
2. **Logs are the sneaky second line.** 20 MB/run × 24 runs is 14 GB/month.
   Keep `logging` at `INFO`, keep tundra's `log_retention_days` short
   (default 7).
3. **Spot is the default and it is fine for batch.** A reclaimed task
   retries on the next schedule; Iceberg commits are atomic, so a
   half-finished run leaves no partial data.
4. **Right-size the task.** awebo streams payloads above
   `AWEBO_STREAMING_THRESHOLD_MB` (default 512), so 1 GB of RAM handles
   multi-GB inputs. Start at 0.5 vCPU / 1 GB and only go up when
   `AWEBO_TRACE_MEMORY=1` shows you need it.
5. **The public IP is cheaper than a NAT gateway.** A NAT gateway is
   $0.045/hour *plus* $0.045/GB — $32/month before traffic. Five minutes
   of public IPv4 a day is $0.013/month. That is why tundra's VPC has no
   NAT.
6. **Skip ECR for the public image.** `ghcr.io/samuel2pb/awebo` is free to
   pull. Use ECR only if you build a private derivative.

## Sources

- [AWS Fargate pricing](https://aws.amazon.com/fargate/pricing/) ·
  [Fargate Spot rates (third-party summary, 2026)](https://leanopstech.com/blog/aws-ecs-fargate-pricing-2026/)
- [AWS public IPv4 address charge](https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights)
- [Amazon S3 pricing (2026 summary)](https://upstash.com/blog/aws-s3-pricing-explained-2026)
- [AWS Glue Data Catalog pricing](https://aws.amazon.com/glue/pricing) ·
  [AWS always-free analytics tier](https://aws.amazon.com/free/analytics/)
- [Amazon CloudWatch pricing (summary)](https://cloudchipr.com/blog/cloudwatch-pricing)
- [Cloudflare R2 pricing](https://developers.cloudflare.com/r2/pricing/)
