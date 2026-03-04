#!/usr/bin/env python3
"""
Audit log loader for CircleCI streaming audit logs.

Downloads JSON audit log files from S3, flattens them, and loads
into a PostgreSQL table for Grafana dashboards.

Modes:
  s3      - Pull logs from an S3 bucket
  local   - Load from a local directory of JSON files
  seed    - Generate realistic sample data for local testing
"""

import os
import sys
import json
import logging
import argparse
import hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import random

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS circleci_audit_logs (
    id              VARCHAR(255) PRIMARY KEY,
    version         INTEGER,
    action          VARCHAR(255) NOT NULL,
    actor_id        VARCHAR(255),
    actor_type      VARCHAR(100),
    actor_name      VARCHAR(255),
    target_id       VARCHAR(255),
    target_type     VARCHAR(100),
    target_name     VARCHAR(255),
    scope_id        VARCHAR(255),
    scope_type      VARCHAR(100),
    scope_name      VARCHAR(255),
    success         BOOLEAN,
    request_id      VARCHAR(255),
    payload         JSONB,
    metadata        JSONB,
    created_at      TIMESTAMP NOT NULL,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_action     ON circleci_audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_actor_name ON circleci_audit_logs(actor_name);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON circleci_audit_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_success    ON circleci_audit_logs(success);
"""


def connect_pg(host: str, port: int, database: str, user: str, password: str):
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    logger.info("Connected to PostgreSQL")
    return conn


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    logger.info("Audit log schema created")


def parse_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a single CircleCI audit log JSON event into a row dict."""
    actor = raw.get("actor") or {}
    target = raw.get("target") or {}
    scope = raw.get("scope") or {}
    request = raw.get("request") or {}

    return {
        "id":           raw.get("id"),
        "version":      raw.get("version"),
        "action":       raw.get("action"),
        "actor_id":     actor.get("id"),
        "actor_type":   actor.get("type"),
        "actor_name":   actor.get("name"),
        "target_id":    target.get("id"),
        "target_type":  target.get("type"),
        "target_name":  target.get("name"),
        "scope_id":     scope.get("id"),
        "scope_type":   scope.get("type"),
        "scope_name":   scope.get("name"),
        "success":      raw.get("success"),
        "request_id":   request.get("id"),
        "payload":      json.dumps(raw.get("payload")) if raw.get("payload") else None,
        "metadata":     json.dumps(raw.get("metadata")) if raw.get("metadata") else None,
        "created_at":   raw.get("occurred_at"),
    }


COLUMNS = [
    "id", "version", "action", "actor_id", "actor_type", "actor_name",
    "target_id", "target_type", "target_name", "scope_id", "scope_type",
    "scope_name", "success", "request_id", "payload", "metadata", "created_at",
]


def upsert_rows(conn, rows: List[Dict[str, Any]]) -> int:
    """Insert rows, skipping duplicates on id conflict."""
    if not rows:
        return 0

    values = [tuple(r[c] for c in COLUMNS) for r in rows]
    sql = f"""
        INSERT INTO circleci_audit_logs ({', '.join(COLUMNS)})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(values)


# ---------------------------------------------------------------------------
# S3 mode
# ---------------------------------------------------------------------------
def load_from_s3(conn, bucket: str, prefix: str, region: str, profile: Optional[str]):
    try:
        import boto3
    except ImportError:
        logger.error("boto3 is required for S3 mode: pip install boto3")
        return 1

    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    if region:
        session_kwargs["region_name"] = region

    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=bucket, Prefix=prefix)

    total = 0
    for page in page_iter:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or "connectivity_test" in key:
                continue

            logger.info(f"Processing s3://{bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read().decode("utf-8")

            rows = []
            for line in body.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    rows.append(parse_event(event))
                except json.JSONDecodeError:
                    try:
                        event = json.loads(body)
                        rows.append(parse_event(event))
                        break
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping unparseable content in {key}")
                        break

            if rows:
                inserted = upsert_rows(conn, rows)
                total += inserted
                logger.info(f"  Loaded {inserted} events from {key}")

    logger.info(f"S3 load complete: {total} events total")
    return 0


# ---------------------------------------------------------------------------
# Local file mode
# ---------------------------------------------------------------------------
def load_from_local(conn, directory: str):
    total = 0
    for fname in sorted(os.listdir(directory)):
        fpath = os.path.join(directory, fname)
        if not os.path.isfile(fpath):
            continue
        if "connectivity_test" in fname:
            continue

        logger.info(f"Processing {fpath}")
        with open(fpath, "r") as f:
            body = f.read()

        rows = []
        for line in body.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                rows.append(parse_event(event))
            except json.JSONDecodeError:
                try:
                    event = json.loads(body)
                    rows.append(parse_event(event))
                    break
                except json.JSONDecodeError:
                    logger.warning(f"Skipping unparseable file {fname}")
                    break

        if rows:
            inserted = upsert_rows(conn, rows)
            total += inserted
            logger.info(f"  Loaded {inserted} events")

    logger.info(f"Local load complete: {total} events total")
    return 0


# ---------------------------------------------------------------------------
# Seed mode — generate realistic sample data
# ---------------------------------------------------------------------------
ACTIONS = [
    "workflow.job.start",
    "workflow.job.finish",
    "workflow.job.scheduled",
    "workflow.job.approve",
    "workflow.start",
    "context.create",
    "context.delete",
    "context.env_var.store",
    "context.env_var.delete",
    "context.secrets.accessed",
    "project.settings.update",
    "project.env_var.create",
    "project.env_var.delete",
    "user.create",
    "user.logged_in",
    "user.logged_out",
    "org_member.remove",
    "trigger_event.create",
]

ACTORS = [
    {"id": "a1", "type": "user", "name": "Nick Martino"},
    {"id": "a2", "type": "user", "name": "Aaron Stillwell"},
    {"id": "a3", "type": "user", "name": "Vijay Raghavan"},
    {"id": "a4", "type": "user", "name": "Sarah Chen"},
    {"id": "a5", "type": "system", "name": "circleci-scheduler"},
]

PROJECTS = [
    "smarter-testing-go-and-jest",
    "flaky-todo-list",
    "ceratf-deployment-monorepo",
    "circleci-usage-reporter",
    "notifications-service",
    "demo-react-app",
]

CONTEXTS = ["aws-prod", "docker-hub", "signing-keys", "deploy-staging"]


def _uuid_from_seed(seed: str) -> str:
    h = hashlib.md5(seed.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def generate_seed_data(days: int = 30, events_per_day: int = 40) -> List[Dict[str, Any]]:
    events = []
    now = datetime.now(timezone.utc)

    for day_offset in range(days, 0, -1):
        day_base = now - timedelta(days=day_offset)
        count = events_per_day + random.randint(-10, 10)

        for i in range(max(count, 5)):
            action = random.choice(ACTIONS)
            actor = random.choice(ACTORS)
            ts = day_base + timedelta(
                hours=random.randint(8, 20),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            if action.startswith("context"):
                target = {"id": f"ctx-{random.randint(1,4)}", "type": "context", "name": random.choice(CONTEXTS)}
            elif action.startswith("project") or action.startswith("workflow") or action.startswith("trigger"):
                proj = random.choice(PROJECTS)
                target = {"id": f"proj-{proj[:8]}", "type": "project", "name": proj}
            elif action.startswith("user") or action == "org_member.remove":
                target = {"id": f"u-{random.randint(100,999)}", "type": "user", "name": random.choice(ACTORS)["name"]}
            else:
                target = {"id": "unknown", "type": "unknown", "name": "unknown"}

            event_id = _uuid_from_seed(f"{day_offset}-{i}-{action}")

            events.append({
                "id": event_id,
                "version": 1,
                "action": action,
                "actor": actor,
                "target": target,
                "scope": {"id": "org-AwesomeCICD", "type": "organization", "name": "AwesomeCICD"},
                "success": random.random() > 0.03,
                "request": {"id": _uuid_from_seed(f"req-{day_offset}-{i}")},
                "payload": {},
                "metadata": {},
                "occurred_at": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            })

    return events


def seed(conn, days: int, events_per_day: int):
    events = generate_seed_data(days, events_per_day)
    rows = [parse_event(e) for e in events]
    inserted = upsert_rows(conn, rows)
    logger.info(f"Seeded {inserted} audit log events ({days} days, ~{events_per_day}/day)")
    return 0


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(conn):
    queries = {
        "total": "SELECT COUNT(*) FROM circleci_audit_logs",
        "date_range": "SELECT MIN(created_at), MAX(created_at) FROM circleci_audit_logs",
        "by_action": """
            SELECT action, COUNT(*) as cnt
            FROM circleci_audit_logs
            GROUP BY action ORDER BY cnt DESC LIMIT 10
        """,
        "by_actor": """
            SELECT actor_name, COUNT(*) as cnt
            FROM circleci_audit_logs
            GROUP BY actor_name ORDER BY cnt DESC LIMIT 5
        """,
    }
    with conn.cursor() as cur:
        cur.execute(queries["total"])
        total = cur.fetchone()[0]
        cur.execute(queries["date_range"])
        dr = cur.fetchone()
        cur.execute(queries["by_action"])
        actions = cur.fetchall()
        cur.execute(queries["by_actor"])
        actors = cur.fetchall()

    print(f"\n=== Audit Log Summary ===")
    print(f"Total events: {total:,}")
    print(f"Date range:   {dr[0]} → {dr[1]}")
    print(f"\nTop actions:")
    for action, cnt in actions:
        print(f"  {action}: {cnt:,}")
    print(f"\nTop actors:")
    for name, cnt in actors:
        print(f"  {name}: {cnt:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="CircleCI audit log loader")
    sub = parser.add_subparsers(dest="mode", required=True)

    # Shared DB args
    db_args = argparse.ArgumentParser(add_help=False)
    db_args.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    db_args.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
    db_args.add_argument("--database", default=os.getenv("PGDATABASE", "circleci_usage"))
    db_args.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    db_args.add_argument("--password", default=None)
    db_args.add_argument("--create-schema", action="store_true")
    db_args.add_argument("--summary", action="store_true")

    # s3
    s3_parser = sub.add_parser("s3", parents=[db_args], help="Load from S3 bucket")
    s3_parser.add_argument("--bucket", required=True)
    s3_parser.add_argument("--prefix", default="")
    s3_parser.add_argument("--region", default="us-east-2")
    s3_parser.add_argument("--profile", default=None)

    # local
    local_parser = sub.add_parser("local", parents=[db_args], help="Load from local directory")
    local_parser.add_argument("--directory", required=True)

    # seed
    seed_parser = sub.add_parser("seed", parents=[db_args], help="Generate sample data")
    seed_parser.add_argument("--days", type=int, default=30)
    seed_parser.add_argument("--events-per-day", type=int, default=40)

    args = parser.parse_args()

    password = args.password or os.getenv("PGPASSWORD", "postgres")
    conn = connect_pg(args.host, args.port, args.database, args.user, password)

    try:
        if args.create_schema:
            create_schema(conn)

        if args.mode == "s3":
            rc = load_from_s3(conn, args.bucket, args.prefix, args.region, args.profile)
        elif args.mode == "local":
            rc = load_from_local(conn, args.directory)
        elif args.mode == "seed":
            rc = seed(conn, args.days, args.events_per_day)
        else:
            rc = 1

        if args.summary:
            print_summary(conn)

        return rc
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
