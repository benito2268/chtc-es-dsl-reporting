import elasticsearch
from elasticsearch_dsl import Search, Q, A
from datetime import datetime, timedelta
from collections import namedtuple
from operator import itemgetter
from pprint import pprint

# each aggregation is initially stored as an elasticseach 'A' object
# in a named tuple along with it's name, 'pretty' name, and type (metric, bucket, or pipeline)
Aggregation = namedtuple("Aggregation", ['object', 'name', 'pretty_name', 'type'])

# global timestamps for the past 24 hours - for now
START_TIME = (datetime.now() - timedelta(hours=24)).timestamp()
CHTC_ES_INDEX = "chtc-schedd-*"
RUNTIME_MAPPINGS = []
ROWS_AGGS = []
TOTALS_AGGS = []

client = elasticsearch.Elasticsearch(hosts=["http://localhost:9200/"], timeout=120)

# =========== helper functions ===========

def print_error(d, depth=0):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif k == "root_cause":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_error(v, depth=depth+1)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}")
        else:
            print(f"{pre}{k}:\t{v}")

def add_runtime_script(search: Search, field_name: str, script: str):
    d = { field_name : {
            "type": "double",
            "script": {
                "language": "painless",
                "source": script,
            }
        }
    }

    RUNTIME_MAPPINGS.append(d)

# returns an 'A' object that uses a bucket_script aggregation
# to compute a percentage across two other metrics
# NOTE: the returned aggregation must be nested under a multi-bucket aggregation
# and cannot be applied at the top level
# 
# example: to calculate percent goodput use
# get_percent_metric("good_cpu_hours", "total_cpu_hours")
# if you have already created 2 metrics good_cpu_hours and total_cpu_hours
def get_percent_bucket_script(want_percent: str, out_of: str) -> A:
     return A("bucket_script",
              buckets_path={"a" : want_percent,
                            "b"  : out_of},
              script="params.a / params.b * 100"                 
              )

# percentages for the totals row is calculated in python
# due to limitations with calculating percents in ES
def calc_totals_percents(resp) -> dict:
    ret = {}
    buckets = resp.aggregations.to_dict()

    # TODO what to do about the previously stored pretty names?
    ret.update({"% Goodput" : buckets["good_core_hours"]["value"] / buckets["cpu_core_hours"]["value"] * 100})
    ret.update({"% Ckptable" : buckets["ckptable_filt"]["doc_count"] / buckets["uniq_job_ids"]["value"] * 100})
    ret.update({"% Removed" : buckets["rmd_filt"]["doc_count"] / buckets["cpu_core_hours"]["value"] * 100})
    ret.update({"Shadow Starts / ID" : buckets["num_shadw_starts"]["value"] / buckets["uniq_job_ids"]["value"]})
    ret.update({"Exec Att / Shadow Start" : buckets["num_exec_attempts"]["value"] / buckets["num_shadw_starts"]["value"]})
    ret.update({"Holds / ID" : buckets["num_holds"]["value"] / buckets["uniq_job_ids"]["value"]})
    ret.update({"% Short" : buckets["short_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"] * 100})
    ret.update({"% Restarted" : buckets["restarted_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"] * 100})
    ret.update({"% Held" : buckets["held_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"] * 100})
    ret.update({"% Over Req. Disk" : buckets["over_disk_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"] * 100})
    ret.update({"% S'ty Jobs" : buckets["sty_jobs"]["doc_count"] / buckets["uniq_job_ids"]["value"] * 100})

    return ret

def table(rows: list):
    head = [a.pretty_name for a in ROWS_AGGS]
    print(f"Report for {datetime.fromtimestamp(START_TIME).strftime('%Y-%m-%d %H:%M:%S')} TO {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        from tabulate import tabulate

    except Exception:
        print("run 'pip install tabulate' to see a nicer table!\n")
        # print for debugging
        print("\t".join(head))
        for row in rows:
            pprint(row.values())
            print()

    # print a nice table if tabulate is installed 
    # NOTE: the table is very wide, should pipe into 'less -S'
    print(tabulate([[item for item in row.values()] for row in rows], 
                   headers=head, 
                   tablefmt="grid"
    ))

# nicely the Q object supports ~ for negation :)
# 'search' is aggregated by project
search = Search(using=client, index=CHTC_ES_INDEX) \
                .filter("range", RecordTime={"gte" : START_TIME}) \
                .filter(~Q("terms", JobUniverse=[7, 12])) \
                .extra(size=0) \
                .extra(track_scores=False)

# totals query is exactly the same
totals = Search(using=client, index=CHTC_ES_INDEX) \
                .filter("range", RecordTime={"gte" : START_TIME}) \
                .filter(~Q("terms", JobUniverse=[7, 12])) \
                .extra(size=0) \
                .extra(track_scores=False)

# top level aggregation is by project
search.aggs.bucket(
    "projects", "terms",
    field="ProjectName.keyword",
    size=1024
)

# =========== aggregations section ===========

# count unique users
# search.aggs["projects"].metric("uniq_users", "cardinality", field="User.keyword")
ROWS_AGGS.append(Aggregation(
                A("cardinality", field="User.keyword"),
                "uniq_users",
                "# Users",
                "metric",
))

# can be resused for totals
TOTALS_AGGS.append(ROWS_AGGS[-1])

# count unique job ids
#search.aggs["projects"].metric("uniq_job_ids", "cardinality", field="GlobalJobId.keyword")
ROWS_AGGS.append(Aggregation(
                A("cardinality", field="GlobalJobId.keyword"),
                "uniq_job_ids",
                "# Jobs",
                "metric",
))

TOTALS_AGGS.append(ROWS_AGGS[-1])

# count total CPU hours, and % goodput hours
CPU_CORE_HOURS_SCRIPT_SRC = """
    double hours = 0;
    int cpus = 1;
    if (doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {
        hours = (double)doc["RemoteWallClockTime"].value / (double)3600;
    }
    if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
        cpus = (int)doc["RequestCpus"].value;
    }
    emit((double)cpus * hours);
"""

GOOD_CPU_HOURS_SCRIPT_SRC = """
    double hours = 0;
    int cpus = 1;
    if (doc.containsKey("lastremotewallclocktime.keyword") && doc["lastremotewallclocktime.keyword"].size() > 0) {
        hours = Double.parseDouble(doc["lastremotewallclocktime.keyword"].value) / (double)3600;
    }
    if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
        cpus = (int)doc["RequestCpus"].value;
    }
    emit((double)cpus * hours);
"""

add_runtime_script(search, "CpuCoreHours", CPU_CORE_HOURS_SCRIPT_SRC)
add_runtime_script(search, "GoodCpuCoreHours", GOOD_CPU_HOURS_SCRIPT_SRC)

add_runtime_script(totals, "CpuCoreHours", CPU_CORE_HOURS_SCRIPT_SRC)
add_runtime_script(totals, "GoodCpuCoreHours", GOOD_CPU_HOURS_SCRIPT_SRC)

ROWS_AGGS.append(Aggregation(
                A("sum", field="CpuCoreHours"),
                "cpu_core_hours",
                "CPU Hours",
                "metric",
))

TOTALS_AGGS.append(ROWS_AGGS[-1])

# these aggs are added dicrectly to the query
# because they will not show up in the final table
search.aggs["projects"].metric("good_core_hours", "sum", field="GoodCpuCoreHours")

totals.aggs.metric("cpu_core_hours", "sum", field="CpuCoreHours")
totals.aggs.metric("good_core_hours", "sum", field="GoodCpuCoreHours")

# calculate percentage within ES
ROWS_AGGS.append(Aggregation(
                get_percent_bucket_script("good_core_hours", "cpu_core_hours"),
                "goodput_percent",
                "% Goodput",
                "metric",
))

# count total job unit hours
# definition of 1 job unit - interpolated into painless script
JOB_UNIT_DEF = {
    "cpus" : 1,
    "mem"  : 4096,      #mb
    "disk" : 4096*1024, #TODO assuming this is kb in ES?
}

JOB_UNIT_HOURS_SCRIPT_SRC = f"""
    double unit_hours = 0;
    if(doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0 
        && doc.containsKey("RequestMemory") && doc["RequestMemory"].size() > 0 
        && doc.containsKey("RequestDisk") && doc["RequestDisk"].size() > 0 
        && doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {{
        
        double units = Collections.max([
            Math.max(1, (int)doc["RequestCpus"].value) / {JOB_UNIT_DEF['cpus']},
            Math.max(0, (int)doc["RequestMemory"].value) / {JOB_UNIT_DEF['mem']},
            Math.max(0, (int)doc["RequestDisk"].value) / {JOB_UNIT_DEF['disk']}
        ]);

        unit_hours = ((double)doc["RemoteWallClockTime"].value / (double)3600) * units;  
    }}
    emit(unit_hours);
"""

add_runtime_script(search, "jobUnitHours", JOB_UNIT_HOURS_SCRIPT_SRC)
add_runtime_script(totals, "jobUnitHours", JOB_UNIT_HOURS_SCRIPT_SRC)

ROWS_AGGS.append(Aggregation(
                A("sum", field="jobUnitHours"),
                "job_unit_hours",
                "Job Unit Hours",
                "metric",
))

TOTALS_AGGS.append(ROWS_AGGS[-1])

# get percent checkpointable jobs

# nested match query read as:
# job must match JobUniverse=5 AND (WhenToTransferOutput=ON_EXIT_OR_EVICT AND Is_resumable=False)
# OR (SuccessCheckpointExitBySignal=False AND SuccessCheckpointExitCode exists)
# TODO can this be reduced using python '&' and '|' ?
cond_1 = Q("bool", must=[
            Q("match", WhenToTransferOutput="ON_EXIT_OR_EVICT"),
            Q("match", Is_resumable="false"),
         ])

cond_2 = Q("bool", must=[
            Q("match", SuccessCheckpointExitBySignal="false"),
            Q("exists", field="SuccessCheckpointExitCode"),
         ])

q = Q("bool", must=[
        Q("match", JobUniverse=5),
        Q("bool", should=[cond_1, cond_2]),
])

# applied directly to the query again
search.aggs["projects"].metric("ckptable_filt", "filter", filter=q)
totals.aggs.metric("ckptable_filt", "filter", filter=q)

# calculate percentage within ES
ROWS_AGGS.append(Aggregation(
            A("bucket_script",
              buckets_path={"num_ckptable" : "ckptable_filt._count",
                            "unique_job_ids"  : "uniq_job_ids"},
              script="params.num_ckptable / params.unique_job_ids * 100"                 
             ),
            "ckptable_percent",
            "% Ckptable",
            "pipeline",
))

# get percent rm'd jobs
rmd_jobs = Q("match", JobStatus=3)
search.aggs["projects"].metric("rmd_filt", "filter", filter=rmd_jobs)
totals.aggs.metric("rmd_filt", "filter", filter=rmd_jobs)

# calculate percentage within ES
# pipeline aggregation doesn't create a new bucket
# which appears to take up a lot of memory :)
ROWS_AGGS.append(Aggregation(
            A("bucket_script",
              buckets_path={"num_rmd" : "rmd_filt._count",
                            "unique_job_ids"  : "uniq_job_ids"},
              script="params.num_rmd / params.unique_job_ids * 100"                 
             ),
            "rmd_percent",
            "% Removed",
            "pipeline",
))

## calculate shadow starts / job id
search.aggs["projects"].metric("num_shadw_starts", "sum", field="NumShadowStarts")
totals.aggs.metric("num_shadw_starts", "sum", field="NumShadowStarts")

ROWS_AGGS.append(Aggregation(
            A("bucket_script",
              buckets_path={"num_ss" : "num_shadw_starts",
                            "unique_job_ids"  : "uniq_job_ids"},
              script="params.num_ss / params.unique_job_ids"                 
             ),
            "shadw_starts_per_id",
            "Shadow Starts / ID",
            "metric",
))

## calculate exec attempts / shadow start
search.aggs["projects"].metric("num_exec_attempts", "sum", field="NumJobStarts")
totals.aggs.metric("num_exec_attempts", "sum", field="NumJobStarts")

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"num_ea" : "num_exec_attempts",
                                "num_ss"  : "num_shadw_starts"},
                  script="params.num_ea / params.num_ss"                 
                 ),
                "exec_att_per_shadw_start",
                "Exec Att / Shadow Start",
                "metric"
))

# calculate holds / job id
# numholds is a text field, need to cast to int
CAST_HOLDS_SCRIPT_SRC = """
 if(doc.containsKey("numholds.keyword") && doc["numholds.keyword"].size() > 0) {
    emit(Double.parseDouble(doc["numholds.keyword"].value)); 
 }
"""
add_runtime_script(search, "numHolds", CAST_HOLDS_SCRIPT_SRC)
add_runtime_script(totals, "numHolds", CAST_HOLDS_SCRIPT_SRC)

search.aggs["projects"].metric("num_holds", "sum", field="numHolds")
totals.aggs.metric("num_holds", "sum", field="numHolds")

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"holds" : "num_holds",
                                "unique_job_ids"  : "uniq_job_ids"},
                  script="params.holds / params.unique_job_ids"                 
                 ),
                "hold_per_id",
                "Holds / ID",
                "metric"
))

# compute % short jobs (<= 60 secs)
short_job_filt = Q("range", lastremotewallclocktime={"lte" : 60})
search.aggs["projects"].metric("short_jobs", "filter", filter=short_job_filt)
totals.aggs.metric("short_jobs", "filter", filter=short_job_filt)

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"num_short" : "short_jobs._count",
                                "unique_job_ids"  : "uniq_job_ids"},
                  script="params.num_short / params.unique_job_ids * 100"
                ),
                "percent_short_jobs",
                "% Short",
                "pipeline",
))

## compute % of jobs with > 1 exec attempt
restarted_filt = Q("range", NumJobStarts={"gt" : 1})
search.aggs["projects"].metric("restarted_jobs", "filter", filter=restarted_filt)
totals.aggs.metric("restarted_jobs", "filter", filter=restarted_filt)

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"num_restarted" : "restarted_jobs._count",
                                "unique_job_ids"  : "uniq_job_ids"},
                  script="params.num_restarted / params.unique_job_ids * 100"
                ),
                "percent_restarted",
                "% Restarted",
                "pipeline",
))

# computer % of jobs with > 1 hold
one_hold_filt = Q("range", numholds={"gt" : 0})
search.aggs["projects"].metric("held_jobs", "filter", filter=one_hold_filt)
totals.aggs.metric("held_jobs", "filter", filter=one_hold_filt)

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"num_held" : "held_jobs._count",
                                "unique_job_ids"  : "uniq_job_ids"},
                  script="params.num_held / params.unique_job_ids * 100"
                ),
                "percent_held",
                "% Held",
                "pipeline",
))

# compute % jobs over requested disk
over_disk_script = { "source" : "doc[\"DiskUsage\"].value > doc[\"RequestDisk\"].value" }
over_disk_filt = Q("bool", filter=[Q("script", script=over_disk_script)])

search.aggs["projects"].metric("over_disk_jobs", "filter", filter=over_disk_filt)
totals.aggs.metric("over_disk_jobs", "filter", filter=over_disk_filt)

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"num_over_disk" : "over_disk_jobs._count",
                                "unique_job_ids"  : "uniq_job_ids"},
                  script="params.num_over_disk / params.unique_job_ids * 100"
                ),
                "percent_over_disk",
                "% Over Req. Disk",
                "pipeline",
))

# compute % of jobs using singularity
sty_filt = Q("bool", filter=[Q("exists", field="SingularityImage")])
search.aggs["projects"].metric("sty_jobs", "filter", filter=sty_filt)
totals.aggs.metric("sty_jobs", "filter", filter=sty_filt)

ROWS_AGGS.append(Aggregation(
                A("bucket_script",
                  buckets_path={"num_sty_jobs" : "sty_jobs._count",
                                "unique_job_ids"  : "uniq_job_ids"},
                  script="params.num_sty_jobs / params.unique_job_ids * 100"
                ),
                "percent_sty",
                "% S'ty Jobs",
                "pipeline",
))

# compute mean activation hours
# need to cast activationduration to a double
CAST_ACTV_DURATION_SCRIPT_SRC = """
    if(doc.containsKey("activationduration.keyword") && doc["activationduration.keyword"].size() > 0) {
        emit(Double.parseDouble(doc["activationduration.keyword"].value) / (double)3600);
    }
"""

add_runtime_script(search, "ActivationDuration", CAST_ACTV_DURATION_SCRIPT_SRC)
add_runtime_script(totals, "ActivationDuration", CAST_ACTV_DURATION_SCRIPT_SRC)

ROWS_AGGS.append(Aggregation(A("avg", field="ActivationDuration"), "mean_act_hrs", "Mean Actv Hours", "metric"))
TOTALS_AGGS.append(ROWS_AGGS[-1])

# =========== add aggregations to the two queries ==============

for agg in ROWS_AGGS: 
    getattr(search.aggs["projects"], agg.type)(agg.name, agg.object)

for agg in TOTALS_AGGS:
    getattr(totals.aggs, agg.type)(agg.name, agg.object)


# =========== execute query and display results ===========

# construct the final runtime mappings dict
maps = {
   "runtime_mappings" : {},   
}

for script in RUNTIME_MAPPINGS:
    maps["runtime_mappings"].update(script)

search.update_from_dict(maps)
totals.update_from_dict(maps)

# run the queries
print(f"{datetime.now()} - Running query...")
try:
   response = search.execute() 
   totals_response = totals.execute()
except Exception as err:
    print(err.info)
    raise err

# extract the final data
table_rows = []
for bucket in response.aggregations.projects.buckets:
    proj_name = bucket["key"]

    # extract data into a row
    # COL_AGG_NAMES defined at top of file
    row = {"Project" : proj_name}
    for agg in ROWS_AGGS:
        row.update({agg.pretty_name : bucket[agg.name]["value"]})
   
    table_rows.append(row)

# create the totals row
totals_row = {"Project" : "Totals"}
totals_raw = totals_response.aggregations.to_dict()

for a in TOTALS_AGGS:
    totals_row.update({a.pretty_name : totals_raw[a.name]["value"]})

final_totals = calc_totals_percents(totals_response)
totals_row.update(final_totals)

# check if something went wrong
if len(table_rows) == 0:
    pprint(response.to_dict())
    exit(1)

table_rows.append(totals_row)

pprint(totals_row)
print()
pprint(table_rows[0])
print()

# sort by # of job ids in descending order
table_rows.sort(key=itemgetter("# Jobs"), reverse=True)

# compute a table (for now)
table(table_rows)
