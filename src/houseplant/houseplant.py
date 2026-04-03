"""Main module."""

import json
import os
import sys
from datetime import datetime
from typing import Literal

import yaml
from rich.console import Console
from rich.table import Table

from .clickhouse_client import ClickHouseClient
from .utils import MIGRATIONS_DIR, get_migration_files


class EnvVars:
    """Helper class to access environment variables via attribute syntax."""

    def __getattr__(self, name: str) -> str:
        return os.getenv(name, "")


# Internal event representation shared by Rich and JSON renderers.
EventType = Literal["applied", "deferred", "empty", "rolled_back", "warning"]


def _make_event(event_type: EventType, version: str, file: str) -> dict:
    return {"type": event_type, "version": version, "file": file}


def _render_event_rich(console: Console, event: dict) -> None:
    """Render a single migration event to the Rich console."""
    icons = {
        "applied": "[green]✓[/green]",
        "rolled_back": "[green]✓[/green]",
        "deferred": "[yellow]⊘[/yellow]",
        "empty": "[yellow]⚠[/yellow]",
        "warning": "[yellow]⚠[/yellow]",
    }
    verbs = {
        "applied": "Applied",
        "rolled_back": "Rolled back",
        "deferred": "Deferred",
        "empty": "Empty",
        "warning": "Warning",
    }
    icon = icons.get(event["type"], "")
    verb = verbs.get(event["type"], event["type"].capitalize())
    console.print(f"{icon} {verb} migration {event['file']}")


def _events_to_json_result(events: list[dict]) -> dict:
    """Group events by type into a JSON-serialisable result dict."""
    result: dict[str, list] = {}
    for event in events:
        result.setdefault(event["type"], []).append(
            {"version": event["version"], "file": event["file"]}
        )
    return result


class Houseplant:
    def __init__(self):
        self.console = Console()
        self.db = ClickHouseClient()
        self.env = os.getenv("HOUSEPLANT_ENV", "development")

    def _check_migrations_dir(self):
        """Check if migrations directory exists and raise formatted error if not."""
        if not os.path.exists(MIGRATIONS_DIR):
            self.console.print("[red]Error:[/red] Migrations directory not found")
            self.console.print(
                "\nPlease run [bold]houseplant init[/bold] to create a new project "
                "or ensure you're in the correct directory."
            )
            raise SystemExit(1)

    def _emit(self, events: list[dict], event: dict, json_output: bool) -> None:
        """Append event to the shared list; print immediately when not in JSON mode."""
        events.append(event)
        if not json_output:
            _render_event_rich(self.console, event)

    def _output(self, events: list[dict], json_output: bool) -> None:
        """Emit final output: JSON dump or nothing (Rich already printed inline)."""
        if json_output:
            print(json.dumps(_events_to_json_result(events)))

    def init(self):
        """Initialize a new houseplant project."""
        with self.console.status("[bold green]Initializing new houseplant project..."):
            os.makedirs("ch/migrations", exist_ok=True)
            open("ch/schema.sql", "a").close()

            self.db.init_migrations_table()

        self.console.print("✨ Project initialized successfully!")

    def migrate_status(self, json_output: bool = False):
        """Show status of database migrations."""
        applied_migrations = {
            version[0] for version in self.db.get_applied_migrations()
        }

        migration_files = get_migration_files()
        if not migration_files:
            if json_output:
                print(json.dumps({"database": self.db.client.connection.database, "migrations": []}))
            else:
                self.console.print("[yellow]No migrations found.[/yellow]")
            return

        migrations = []
        for migration_file in migration_files:
            version = migration_file.split("_")[0]
            with open(os.path.join(MIGRATIONS_DIR, migration_file), "r") as f:
                migration_data = yaml.safe_load(f)
            skip_envs = migration_data.get("skip_envs") or []
            is_deferred = self.env in skip_envs
            if version in applied_migrations:
                status = "up"
            elif is_deferred:
                status = "hold"
            else:
                status = "down"
            name = " ".join(migration_file.split("_")[1:]).replace(".yml", "")
            migrations.append({"version": version, "name": name, "file": migration_file, "status": status})

        if json_output:
            print(json.dumps({"database": self.db.client.connection.database, "migrations": migrations}))
            return

        self.console.print(f"\nDatabase: {self.db.client.connection.database}\n")
        table = Table()
        table.add_column("Status", justify="center", style="cyan", no_wrap=True)
        table.add_column("Migration ID", justify="left", style="magenta")
        table.add_column("Migration Name", justify="left", style="green")
        status_markup = {"up": "[green]up[/green]", "hold": "[yellow]hold[/yellow]", "down": "[red]down[/red]"}
        for m in migrations:
            table.add_row(status_markup[m["status"]], m["version"], m["name"])
        self.console.print(table)
        self.console.print("")

    def migrate_up(self, version: str | None = None, json_output: bool = False):
        """Run migrations up to specified version."""
        if version and version.startswith("VERSION="):
            version = version.replace("VERSION=", "")

        migration_files = get_migration_files()
        if not migration_files:
            if json_output:
                print(json.dumps({}))
            else:
                self.console.print("[yellow]No migrations found.[/yellow]")
            return

        applied_migrations = {
            version[0] for version in self.db.get_applied_migrations()
        }

        if version:
            matching_files = [f for f in migration_files if f.split("_")[0] == version]
            if not matching_files:
                if json_output:
                    print(json.dumps({"error": f"Migration version {version} not found"}), file=sys.stderr)
                    raise SystemExit(1)
                self.console.print(f"[red]Migration version {version} not found[/red]")
                return

        events: list[dict] = []

        def _run():
            for migration_file in migration_files:
                migration_version = migration_file.split("_")[0]

                if version and migration_version != version:
                    continue

                if migration_version in applied_migrations:
                    continue

                with open(os.path.join(MIGRATIONS_DIR, migration_file), "r") as f:
                    migration = yaml.safe_load(f)

                skip_envs = migration.get("skip_envs") or []
                if self.env in skip_envs:
                    self._emit(events, _make_event("deferred", migration_version, migration_file), json_output)
                    continue

                table = migration.get("table", "").strip()
                if not table:
                    if json_output:
                        print(json.dumps({"error": "'table' field is required in migration file", **_events_to_json_result(events)}), file=sys.stderr)
                        raise SystemExit(1)
                    self.console.print(
                        "[red]✗[/red] Migration [bold red]failed[/bold red]: "
                        "'table' field is required in migration file"
                    )
                    return

                database = migration.get("database", "").strip() or self.db.database
                table_definition = migration.get("table_definition", "").strip()
                table_settings = migration.get("table_settings", "").strip()

                format_args = {"table": table, "database": database, "env": EnvVars()}
                if table_definition and table_settings:
                    format_args.update({"table_definition": table_definition, "table_settings": table_settings})

                sink_table = migration.get("sink_table", "").strip()
                view_definition = migration.get("view_definition", "").strip()
                view_query = migration.get("view_query", "").strip()
                if sink_table and view_definition and view_query:
                    format_args.update({"sink_table": sink_table, "view_definition": view_definition, "view_query": view_query})

                migration_env: dict = migration.get(self.env, {})
                migration_sql = migration_env.get("up", "").format(**format_args).strip()

                if migration_sql:
                    self.db.execute_migration(migration_sql, migration_env.get("query_settings"))
                    self.db.mark_migration_applied(migration_version)
                    self._emit(events, _make_event("applied", migration_version, migration_file), json_output)
                else:
                    self._emit(events, _make_event("empty", migration_version, migration_file), json_output)

                if version and migration_version == version:
                    self.update_schema()
                    break

        if json_output:
            _run()
        else:
            with self.console.status(f"[bold green]Running migration version: {version}..."):
                _run()

        self._output(events, json_output)

    def migrate_down(self, version: str | None = None, json_output: bool = False):
        """Roll back migrations to specified version."""
        if version and version.startswith("VERSION="):
            version = version.replace("VERSION=", "")

        applied_migrations = sorted(
            [version[0] for version in self.db.get_applied_migrations()], reverse=True
        )

        if not applied_migrations:
            if json_output:
                print(json.dumps({}))
            else:
                self.console.print("[yellow]No migrations to roll back.[/yellow]")
            return

        events: list[dict] = []

        def _run():
            for migration_version in applied_migrations:
                if version and migration_version < version:
                    break

                migration_file = next(
                    (f for f in os.listdir(MIGRATIONS_DIR) if f.startswith(migration_version) and f.endswith(".yml")),
                    None,
                )

                if not migration_file:
                    self._emit(events, _make_event("warning", migration_version, f"(file not found for {migration_version})"), json_output)
                    continue

                with open(os.path.join(MIGRATIONS_DIR, migration_file), "r") as f:
                    migration = yaml.safe_load(f)

                table = migration.get("table", "").strip()
                if not table:
                    if json_output:
                        print(json.dumps({"error": "'table' field is required in migration file", **_events_to_json_result(events)}), file=sys.stderr)
                        raise SystemExit(1)
                    self.console.print(
                        "[red]✗[/red] [bold red] Migration failed[/bold red]: "
                        "'table' field is required in migration file"
                    )
                    return

                database = migration.get("database", "").strip() or self.db.database
                migration_env = migration.get(self.env, {})
                migration_sql = (
                    migration_env.get("down", {})
                    .format(table=table, database=database, env=EnvVars())
                    .strip()
                )

                if migration_sql:
                    self.db.execute_migration(migration_sql, migration_env.get("query_settings"))
                    self.db.mark_migration_rolled_back(migration_version)
                    self.update_schema()
                    self._emit(events, _make_event("rolled_back", migration_version, migration_file), json_output)
                    return

                self._emit(events, _make_event("empty", migration_version, migration_file), json_output)

        if json_output:
            _run()
        else:
            with self.console.status(f"[bold green]Rolling back to version: {version}..."):
                _run()

        self._output(events, json_output)

    def migrate(self, version: str | None = None, json_output: bool = False):
        """Run migrations up to specified version."""
        self.migrate_up(version, json_output=json_output)

    def generate(self, name: str):
        """Generate a new migration."""
        with self.console.status("[bold green]Generating migration..."):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            migration_name = name.replace(" ", "_").replace("-", "_").lower()
            migration_file = f"ch/migrations/{timestamp}_{migration_name}.yml"

            with open(migration_file, "w") as f:
                f.write(f"""version: "{timestamp}"
name: {migration_name}
table:
# database:  # Optional: specify database name (defaults to CLICKHOUSE_DB)
# skip_envs: []  # Optional: list of environments to defer this migration in

development: &development
  up: |
  down: |
    DROP TABLE {{table}}

test:
  <<: *development

production:
  up: |
  down: |
    DROP TABLE {{table}}
""")

            self.console.print(f"✨ Generated migration: {migration_file}")

    def db_schema_load(self):
        """Load schema migrations from migration files without applying them."""
        migration_files = get_migration_files()
        if not migration_files:
            self.console.print("[yellow]No migrations found.[/yellow]")
            return

        with self.console.status("[bold green]Loading schema migrations..."):
            for migration_file in migration_files:
                migration_version = migration_file.split("_")[0]
                self.db.mark_migration_applied(migration_version)
                self.console.print(
                    f"[green]✓[/green] Loaded migration {migration_file}"
                )

        self.console.print("✨ Schema migrations loaded successfully!")

    def db_schema_dump(self):
        """Dump the current database schema to ch/schema.sql."""
        with self.console.status("[bold green]Dumping database schema..."):
            self.update_schema()
        self.console.print("✨ Schema dumped to ch/schema.sql")

    def update_schema(self):
        """Update the schema file with the current database schema."""

        applied_migrations = self.db.get_applied_migrations()
        migration_files = get_migration_files()
        latest_version = applied_migrations[-1][0] if applied_migrations else "0"

        processed_tables = set()
        table_statements = []
        mv_statements = []
        dict_statements = []

        for migration_version in applied_migrations:
            matching_file = next(
                (f for f in migration_files if f.startswith(migration_version[0])), None
            )

            if not matching_file:
                continue

            migration_file = f"ch/migrations/{matching_file}"
            with open(migration_file) as f:
                migration_data = yaml.safe_load(f)

            table_name = migration_data.get("table")
            if not table_name:
                continue

            database = migration_data.get("database", "").strip() or self.db.database
            qualified_name = f"{database}.{table_name}" if database else table_name

            if qualified_name in processed_tables:
                continue

            tables = self.db.get_database_tables(database)
            materialized_views = self.db.get_database_materialized_views(database)
            dictionaries = self.db.get_database_dictionaries(database)

            for table in tables:
                if table[0] == table_name:
                    table_ref = f"{database}.{table_name}" if database else table_name
                    create_stmt = self.db.client.execute(f"SHOW CREATE TABLE {table_ref}")[0][0]
                    table_statements.append(create_stmt)
                    processed_tables.add(qualified_name)
                    break

            for mv in materialized_views:
                if mv[0] == table_name:
                    mv_ref = f"{database}.{table_name}" if database else table_name
                    create_stmt = self.db.client.execute(f"SHOW CREATE VIEW {mv_ref}")[0][0]
                    mv_statements.append(create_stmt)
                    processed_tables.add(qualified_name)
                    break

            for ch_dict in dictionaries:
                if ch_dict[0] == table_name:
                    dict_ref = f"{database}.{table_name}" if database else table_name
                    create_stmt = self.db.client.execute(f"SHOW CREATE DICTIONARY {dict_ref}")[0][0]
                    dict_statements.append(create_stmt)
                    processed_tables.add(qualified_name)
                    break

        with open("ch/schema.sql", "w") as f:
            f.write(f"-- version: {latest_version}\n\n")
            if table_statements:
                f.write("-- TABLES\n\n")
                f.write("\n;\n\n".join(table_statements) + ";")
            if mv_statements:
                f.write("\n\n-- MATERIALIZED VIEWS\n\n")
                f.write("\n;\n\n".join(mv_statements) + ";")
            if dict_statements:
                f.write("\n\n-- DICTIONARIES\n\n")
                f.write("\n;\n\n".join(dict_statements) + ";")
