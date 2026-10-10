"""
CSV Summariser - AWS Lambda function (Python 3.14, arm64)

Serves a small web page and summarises uploaded CSV files.
Raw uploads are stored in S3 under uploads/<user-key>/, summaries under
results/<user-key>/. Each browser has its own random user key, so people only
ever see and download their own files.

Routes (reached through the Lambda Function URL):
  GET  /                     -> the web page
  POST /api/summarise        -> summarise an uploaded CSV
  GET  /api/history          -> list recent summaries
  GET  /api/result?id=...    -> fetch one saved summary
  GET  /api/download?id=...  -> short-lived presigned link to the original CSV
"""

import base64
import csv
import io
import json
import math
import os
import re
import statistics
import uuid
from collections import Counter
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
BUCKET = os.environ["BUCKET_NAME"]        # set in Lambda environment variables
MAX_BYTES = 5 * 1024 * 1024               # 5 MB cap (Function URL request limit is 6 MB)
HISTORY_LIMIT = 20                        # how many past summaries to show
LINK_EXPIRY_SECONDS = 300                 # download links expire after 5 minutes
MISSING = {"", "na", "n/a", "null", "none", "nan", "-"}
ID_PATTERN = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}")
USER_KEY_PATTERN = re.compile(r"[0-9a-f]{32}")

# Created once per container and reused across requests (faster warm starts)
s3 = boto3.client(
    "s3",
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    config=Config(signature_version="s3v4"),
)

# Two-sided 95% t critical values for 1 to 30 degrees of freedom.
# Above 30, the normal value 1.96 is close enough.
T95 = [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
       2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
       2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]


class UserError(Exception):
    """A problem with the user's input. Returned as a 400 with a clear message."""


# ---------------------------------------------------------------------------
# Entry point: Lambda calls this for every request
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("rawPath", "/")
    params = event.get("queryStringParameters") or {}

    try:
        if method == "GET" and path == "/":
            return respond(200, PAGE, "text/html; charset=utf-8")

        # Every API request must carry the browser's user key
        user = user_key(event)
        if method == "POST" and path == "/api/summarise":
            return summarise(event, params, user)
        if method == "GET" and path == "/api/history":
            return respond_json(200, {"items": history(user)})
        if method == "GET" and path == "/api/result":
            return get_result(params, user)
        if method == "GET" and path == "/api/download":
            return download_link(params, user)
        return respond_json(404, {"error": "Page not found."})
    except UserError as e:
        return respond_json(400, {"error": str(e)})
    except Exception as e:
        print(f"ERROR: {e!r}")  # written to CloudWatch Logs for debugging
        return respond_json(500, {"error": "The server could not process the request."})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
def summarise(event, params, user):
    """Read the uploaded CSV, compute statistics, save both files to S3."""
    body = event.get("body") or ""
    raw = base64.b64decode(body) if event.get("isBase64Encoded") else body.encode("utf-8")

    if not raw:
        raise UserError("The file is empty.")
    if len(raw) > MAX_BYTES:
        raise UserError("The file is larger than 5 MB.")

    filename = clean_filename(params.get("filename", "upload.csv"))
    summary = build_summary(decode(raw))

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    summary.update({
        "id": run_id,
        "filename": filename,
        "created_at": now.isoformat(timespec="seconds"),
        "size_bytes": len(raw),
    })

    # Original file: kept for download, archived and expired by the lifecycle rule
    s3.put_object(
        Bucket=BUCKET, Key=f"uploads/{user}/{run_id}.csv", Body=raw,
        ContentType="text/csv", Metadata={"filename": filename},
    )
    # Summary: small JSON, read every time the history loads
    s3.put_object(
        Bucket=BUCKET, Key=f"results/{user}/{run_id}.json",
        Body=json.dumps(summary).encode("utf-8"), ContentType="application/json",
    )
    return respond_json(200, summary)


def history(user):
    """Return basic details of this user's most recent summaries.
    Only this user's folder is listed, so other people's files never appear."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"results/{user}/")
    # IDs start with a timestamp, so sorting keys newest-first sorts by date
    keys = sorted((o["Key"] for o in resp.get("Contents", [])), reverse=True)
    items = []
    for key in keys[:HISTORY_LIMIT]:
        s = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        items.append({k: s.get(k) for k in
                      ("id", "filename", "created_at", "row_count", "column_count")})
    return items


def get_result(params, user):
    """Return one of this user's saved summaries by its ID."""
    run_id = valid_id(params.get("id"))
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"results/{user}/{run_id}.json")
    except ClientError:
        return respond_json(404, {"error": "That summary could not be found."})
    return respond(200, obj["Body"].read().decode("utf-8"), "application/json")


def download_link(params, user):
    """Create a presigned URL so the browser can download the original CSV
    directly from S3 for 5 minutes, without the bucket ever being public."""
    run_id = valid_id(params.get("id"))
    key = f"uploads/{user}/{run_id}.csv"
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError:
        return respond_json(404, {"error": "The original file is no longer stored. "
                                           "Uploads are deleted after 90 days."})
    filename = head.get("Metadata", {}).get("filename", "upload.csv")
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": key,
                "ResponseContentDisposition": f'attachment; filename="{filename}"'},
        ExpiresIn=LINK_EXPIRY_SECONDS,
    )
    return respond_json(200, {"url": url, "expires_in": LINK_EXPIRY_SECONDS})


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------
def build_summary(text):
    try:
        rows = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    except csv.Error:
        raise UserError("The file could not be read as a CSV.")
    if len(rows) < 2:
        raise UserError("The CSV needs a header row and at least one data row.")

    header = [h.strip() or f"column_{i + 1}" for i, h in enumerate(rows[0])]
    data = rows[1:]
    columns = []
    for i, name in enumerate(header):
        values = [r[i].strip() if i < len(r) else "" for r in data]
        columns.append(summarise_column(name, values))

    return {"row_count": len(data), "column_count": len(header), "columns": columns}


def summarise_column(name, values):
    """Numeric if every non-missing value is a number, otherwise text."""
    present = [v for v in values if v.lower() not in MISSING]
    missing = len(values) - len(present)
    numbers = [to_number(v) for v in present]
    if present and all(n is not None for n in numbers):
        return numeric_summary(name, numbers, missing)
    return text_summary(name, present, missing)


def numeric_summary(name, nums, missing):
    n = len(nums)
    mean = statistics.fmean(nums)
    sd = statistics.stdev(nums) if n > 1 else 0.0
    result = {
        "name": name, "type": "numeric", "count": n, "missing": missing,
        "mean": mean, "median": statistics.median(nums), "std_dev": sd,
        "min": min(nums), "max": max(nums), "ci95": None,
    }
    if n > 1:
        # 95% confidence interval for the mean: t value for small samples
        crit = T95[n - 2] if n - 1 <= 30 else 1.96
        half_width = crit * sd / math.sqrt(n)
        result["ci95"] = [mean - half_width, mean + half_width]
    return result


def text_summary(name, present, missing):
    counts = Counter(present)
    return {
        "name": name, "type": "text", "count": len(present), "missing": missing,
        "unique": len(counts), "top_values": counts.most_common(3),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def to_number(value):
    """Convert '1,200' or '$45.50' to a float. Returns None if not a number."""
    try:
        x = float(value.replace(",", "").replace("$", ""))
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def decode(raw):
    """UTF-8 first (with Excel's BOM removed), Latin-1 as a fallback."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def clean_filename(name):
    """Keep only safe characters so the name is safe to store and display."""
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name).strip()[:100]
    return name or "upload.csv"


def user_key(event):
    """Read the browser's private key from the X-User-Key header. It must be
    32 random hex characters, which is practically impossible to guess."""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    key = headers.get("x-user-key", "")
    if not USER_KEY_PATTERN.fullmatch(key):
        raise UserError("Missing or invalid user key. Refresh the page and try again.")
    return key


def valid_id(run_id):
    """Only accept IDs in the exact format this service creates, so a request
    can never read an arbitrary key from the bucket."""
    if not run_id or not ID_PATTERN.fullmatch(run_id):
        raise UserError("Invalid summary ID.")
    return run_id


def respond(status, body, content_type):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": content_type,
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        },
        "body": body,
    }


def respond_json(status, data):
    return respond(status, json.dumps(data), "application/json")


# ---------------------------------------------------------------------------
# Web page (served by this same function, so no separate web server is needed)
# ---------------------------------------------------------------------------
PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CSV Summariser</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #F2F4F7; --surface: #FFFFFF; --ink: #1C2533; --muted: #5B6675;
    --line: #D8DEE6; --accent: #1D5C8C; --band: #A9CBE6;
    --err: #B42318; --ok: #1E7B4B;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #11161D; --surface: #19202A; --ink: #E4E9EF; --muted: #98A3B2;
      --line: #2C3542; --accent: #74AEDB; --band: #2E5677;
      --err: #F0917F; --ok: #7CCB9E;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font: 16px/1.5 "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  main { max-width: 1200px; margin: 0 auto; padding: 48px 20px 64px; }
  h1 { font-size: 2rem; font-weight: 600; margin: 0 0 6px; letter-spacing: -0.01em; }
  h2 { font-size: 1.15rem; font-weight: 600; margin: 0 0 12px; }
  p.lead { color: var(--muted); margin: 0 0 32px; max-width: 60ch; }
  section { margin-top: 40px; }

  .drop {
    display: flex; flex-wrap: wrap; align-items: center; gap: 16px;
    background: var(--surface); border: 2px dashed var(--line);
    border-radius: 10px; padding: 24px;
  }
  .drop.over { border-color: var(--accent); }
  .drop input { position: absolute; width: 1px; height: 1px; opacity: 0; }
  .pick {
    border: 1px solid var(--line); border-radius: 6px; padding: 9px 14px;
    cursor: pointer; background: var(--bg);
  }
  .fname { color: var(--muted); flex: 1; min-width: 160px; }
  button {
    font: inherit; font-weight: 500; border: 0; border-radius: 6px;
    padding: 10px 18px; background: var(--accent); color: #fff; cursor: pointer;
  }
  button.link {
    background: none; color: var(--accent); padding: 4px 6px; font-weight: 500;
  }
  button:disabled { opacity: 0.5; cursor: default; }
  :focus-visible { outline: 3px solid var(--accent); outline-offset: 2px; }
  #status { min-height: 1.5em; margin: 12px 0 0; }
  #status.err { color: var(--err); }
  #status.ok { color: var(--ok); }

  .meta { color: var(--muted); margin: -6px 0 16px; }
  .scroll { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 10px; }
  table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
  th, td { padding: 10px 12px; text-align: right; white-space: nowrap; border-bottom: 1px solid var(--line); }
  th { font-weight: 500; color: var(--muted); font-size: 0.875rem; }
  th:first-child, td:first-child { text-align: left; font-weight: 500; }
  tr:last-child td { border-bottom: 0; }
  td.left { text-align: left; }
  .sub { margin-top: 24px; }

  /* The one visual flourish: a range bar showing min to max,
     the 95% confidence band, and a tick at the mean */
  .range { position: relative; width: 110px; height: 8px; border-radius: 4px; background: var(--line); display: inline-block; vertical-align: middle; }
  .range .band { position: absolute; top: 0; bottom: 0; background: var(--band); border-radius: 4px; min-width: 2px; }
  .range .tick { position: absolute; top: -4px; bottom: -4px; width: 2px; background: var(--accent); }

  .hist li { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 16px; padding: 12px 16px; border-bottom: 1px solid var(--line); }
  .hist li:last-child { border-bottom: 0; }
  .hist ul { list-style: none; margin: 0; padding: 0; background: var(--surface); border: 1px solid var(--line); border-radius: 10px; }
  .hist .name { font-weight: 500; flex: 1; min-width: 180px; overflow-wrap: anywhere; }
  .hist .when { color: var(--muted); font-size: 0.9rem; }
  .empty { color: var(--muted); padding: 16px; }
</style>
</head>
<body>
<main>
  <h1>CSV Summariser</h1>
  <p class="lead">Upload a CSV file to get statistics for every column. Each summary is saved privately to this browser, so you can reopen it or download the original file later.</p>

  <div class="drop" id="drop">
    <label class="pick" for="file">Choose CSV file</label>
    <input type="file" id="file" accept=".csv,text/csv">
    <span class="fname" id="fname">or drag a file here (up to 5 MB)</span>
    <button id="go">Summarise</button>
  </div>
  <p id="status" role="status"></p>

  <section id="results" hidden>
    <h2 id="r-title"></h2>
    <p class="meta" id="r-meta"></p>
    <div id="num-wrap">
      <div class="scroll"><table>
        <thead><tr><th>Numeric column</th><th>Count</th><th>Missing</th><th>Mean</th><th>95% CI for mean</th><th>Min to max</th><th>Median</th><th>Std dev</th><th>Min</th><th>Max</th></tr></thead>
        <tbody id="num-body"></tbody>
      </table></div>
    </div>
    <div id="txt-wrap" class="sub">
      <div class="scroll"><table>
        <thead><tr><th>Text column</th><th>Count</th><th>Missing</th><th>Unique</th><th style="text-align:left">Most common</th></tr></thead>
        <tbody id="txt-body"></tbody>
      </table></div>
    </div>
  </section>

  <section class="hist">
    <h2>Past summaries</h2>
    <ul id="hist"><li class="empty">Loading...</li></ul>
  </section>
</main>

<script>
const MAX_BYTES = 5 * 1024 * 1024;
const $ = (id) => document.getElementById(id);
let chosen = null;

// Each browser gets its own random 32-character key, remembered between visits.
// It is sent with every request so the server only returns this browser's files.
function newKey() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}
let USER_KEY;
try {
  USER_KEY = localStorage.getItem("csvSummariserKey");
  if (!/^[0-9a-f]{32}$/.test(USER_KEY || "")) {
    USER_KEY = newKey();
    localStorage.setItem("csvSummariserKey", USER_KEY);
  }
} catch (e) {
  USER_KEY = newKey();  // storage blocked: history lasts for this visit only
}
function api(url, options) {
  const opts = options || {};
  opts.headers = Object.assign({ "X-User-Key": USER_KEY }, opts.headers || {});
  return fetch(url, opts);
}

// Build elements with textContent so CSV content is never run as HTML
function el(tag, text, cls) {
  const e = document.createElement(tag);
  if (text !== undefined && text !== null) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}
function fmt(x) {
  if (x === null || x === undefined) return "n/a";
  if (typeof x !== "number") return String(x);
  return x.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
function setStatus(msg, kind) {
  const s = $("status");
  s.textContent = msg;
  s.className = kind || "";
}
function pick(file) {
  chosen = file || null;
  $("fname").textContent = chosen ? chosen.name : "or drag a file here (up to 5 MB)";
}

$("file").addEventListener("change", (e) => pick(e.target.files[0]));
const drop = $("drop");
drop.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("over"); });
drop.addEventListener("dragleave", () => drop.classList.remove("over"));
drop.addEventListener("drop", (e) => {
  e.preventDefault(); drop.classList.remove("over");
  pick(e.dataTransfer.files[0]);
});

$("go").addEventListener("click", async () => {
  if (!chosen) { setStatus("Choose a CSV file first.", "err"); return; }
  if (chosen.size > MAX_BYTES) { setStatus("That file is larger than 5 MB. Choose a smaller file.", "err"); return; }
  $("go").disabled = true;
  setStatus("Summarising " + chosen.name + "...");
  try {
    const res = await api("/api/summarise?filename=" + encodeURIComponent(chosen.name), {
      method: "POST", headers: { "Content-Type": "text/csv" }, body: chosen,
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "The summary failed.");
    render(data);
    setStatus("Summarised and saved.", "ok");
    loadHistory();
  } catch (err) {
    setStatus(err.message, "err");
  } finally {
    $("go").disabled = false;
  }
});

function rangeBar(c) {
  const wrap = el("span", null, "range");
  const span = c.max - c.min;
  if (!c.ci95 || span <= 0) return wrap;
  const pos = (v) => Math.min(100, Math.max(0, ((v - c.min) / span) * 100));
  const band = el("span", null, "band");
  band.style.left = pos(c.ci95[0]) + "%";
  band.style.width = (pos(c.ci95[1]) - pos(c.ci95[0])) + "%";
  const tick = el("span", null, "tick");
  tick.style.left = "calc(" + pos(c.mean) + "% - 1px)";
  wrap.append(band, tick);
  wrap.title = "Shaded: 95% CI for the mean. Line: mean.";
  return wrap;
}

function render(s) {
  $("results").hidden = false;
  $("r-title").textContent = s.filename;
  $("r-meta").textContent = s.row_count.toLocaleString() + " rows, " +
    s.column_count + " columns. Summarised " + new Date(s.created_at).toLocaleString() + ".";

  const nums = s.columns.filter((c) => c.type === "numeric");
  const txts = s.columns.filter((c) => c.type === "text");
  const nb = $("num-body"), tb = $("txt-body");
  nb.replaceChildren(); tb.replaceChildren();

  nums.forEach((c) => {
    const tr = el("tr");
    const ci = c.ci95 ? fmt(c.ci95[0]) + " to " + fmt(c.ci95[1]) : "n/a";
    const barCell = el("td");
    barCell.append(rangeBar(c));
    tr.append(el("td", c.name), el("td", fmt(c.count)), el("td", fmt(c.missing)),
      el("td", fmt(c.mean)), el("td", ci), barCell, el("td", fmt(c.median)),
      el("td", fmt(c.std_dev)), el("td", fmt(c.min)), el("td", fmt(c.max)));
    nb.append(tr);
  });
  txts.forEach((c) => {
    let top = c.top_values.map((v) => v[0] + " (" + v[1] + ")").join(", ") || "n/a";
    if (c.count > 1 && c.unique === c.count) top = "Every value is unique (likely an ID column)";
    const tr = el("tr");
    tr.append(el("td", c.name), el("td", fmt(c.count)), el("td", fmt(c.missing)),
      el("td", fmt(c.unique)), el("td", top, "left"));
    tb.append(tr);
  });
  $("num-wrap").hidden = nums.length === 0;
  $("txt-wrap").hidden = txts.length === 0;
  $("results").scrollIntoView({ behavior: "smooth", block: "start" });
}

async function openSummary(id) {
  setStatus("Loading summary...");
  try {
    const res = await api("/api/result?id=" + encodeURIComponent(id));
    const data = await res.json();
    if (!res.ok) throw new Error(data.error);
    render(data);
    setStatus("");
  } catch (err) { setStatus(err.message, "err"); }
}

async function downloadOriginal(id) {
  try {
    const res = await api("/api/download?id=" + encodeURIComponent(id));
    const data = await res.json();
    if (!res.ok) throw new Error(data.error);
    window.location.href = data.url;  // presigned S3 link, valid for 5 minutes
  } catch (err) { setStatus(err.message, "err"); }
}

async function loadHistory() {
  const list = $("hist");
  try {
    const res = await api("/api/history");
    const data = await res.json();
    if (!res.ok) throw new Error(data.error);
    list.replaceChildren();
    if (data.items.length === 0) {
      list.append(el("li", "No summaries yet. Upload a CSV above to create one.", "empty"));
      return;
    }
    data.items.forEach((it) => {
      const li = el("li");
      const view = el("button", "View summary", "link");
      view.addEventListener("click", () => openSummary(it.id));
      const dl = el("button", "Download original", "link");
      dl.addEventListener("click", () => downloadOriginal(it.id));
      li.append(el("span", it.filename, "name"),
        el("span", it.row_count + " rows, " + it.column_count + " columns, " +
          new Date(it.created_at).toLocaleString(), "when"), view, dl);
      list.append(li);
    });
  } catch (err) {
    list.replaceChildren(el("li", "Past summaries could not be loaded: " + err.message, "empty"));
  }
}

loadHistory();
</script>
</body>
</html>
"""
