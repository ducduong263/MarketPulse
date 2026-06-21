"""
dag_pipeline_config — Airflow DAG for managing pipeline_config table.

This DAG provides operational tasks to inspect and update the dynamic
configuration store used by all ingestion services.

--- Available tasks ---

  1. show_config          : Print all current pipeline_config values to logs
  2. update_symbol_filter : Update symbol filter parameters (only filled fields)
  3. update_flush_settings: Update flush/performance parameters (only filled fields)
  4. reset_to_defaults    : Reset all config back to default values
  5. verify_config        : Validate the current config after updates

--- How to trigger ---

  ALL params default to None (empty in the UI form).
  Leave a field empty = keep the current DB value unchanged.
  Fill in only the fields you want to update.

  Example: only change index filter
  {
    "symbol_filter_indexes": "VN30,VN100"
  }

  The first task (show_config) always prints the CURRENT DB values to logs,
  so you can see what is active before deciding what to change.

--- Symbol filter params (leave empty = keep current DB value) ---
  symbol_filter_mode     : "db" or "static"
  symbol_filter_indexes  : e.g. "VN30,VN100,HNX30"
  symbol_filter_groups   : e.g. "FU"
  symbol_filter_status   : e.g. "NO_HALT"
  symbol_filter_sanction : e.g. "NRM"
  symbol_filter_board_id : e.g. "G1"
  symbol_filter_market   : e.g. "" (empty = all markets)
  symbol_filter_admin    : e.g. "NRM"

--- Flush/performance params (leave empty = keep current DB value) ---
  flush_batch_size       : integer, e.g. "100"
  flush_timeout_seconds  : float, e.g. "2.0"
  stats_flush_interval   : integer (seconds), e.g. "30"

After updating, producers will pick up changes within 60s (symbol_filter group)
or 300s (flush/connection groups) — no container restart needed.
"""

from __future__ import annotations

from airflow.sdk import dag, task, Param


# ── All params default to None — leave empty = keep current DB value ──────────
_OPT = {"type": ["null", "string"]}

_SYMBOL_PARAMS = {
    "symbol_filter_mode":     Param(None, **_OPT, description="db or static  [empty = keep current]"),
    "symbol_filter_indexes":  Param(None, **_OPT, description="Comma-sep index names  [empty = keep current]"),
    "symbol_filter_groups":   Param(None, **_OPT, description="Comma-sep security_group_id  [empty = keep current]"),
    "symbol_filter_status":   Param(None, **_OPT, description="Comma-sep security_status  [empty = keep current]"),
    "symbol_filter_sanction": Param(None, **_OPT, description="Comma-sep trading_sanction_status  [empty = keep current]"),
    "symbol_filter_board_id": Param(None, **_OPT, description="board_id filter  [empty = keep current]"),
    "symbol_filter_market":   Param(None, **_OPT, description="Comma-sep market_id (empty string = all markets)  [empty = keep current]"),
    "symbol_filter_admin":    Param(None, **_OPT, description="Comma-sep admin_status  [empty = keep current]"),
}

_FLUSH_PARAMS = {
    "flush_batch_size":      Param(None, **_OPT, description="Consumer batch size  [empty = keep current]"),
    "flush_timeout_seconds": Param(None, **_OPT, description="Consumer batch timeout (seconds)  [empty = keep current]"),
    "stats_flush_interval":  Param(None, **_OPT, description="StatsReporter flush interval (seconds)  [empty = keep current]"),
}

_ALL_KEYS = set(_SYMBOL_PARAMS) | set(_FLUSH_PARAMS)


@dag(
    dag_id="dag_pipeline_config",
    schedule=None,  # Manual trigger only
    start_date=None,
    catchup=False,
    tags=["marketpulse", "config", "operations"],
    doc_md=__doc__,
    params={**_SYMBOL_PARAMS, **_FLUSH_PARAMS},
)
def dag_pipeline_config():

    @task()
    def show_config() -> dict:
        """
        Print all CURRENT pipeline_config values from DB.

        Use this to see what is active before deciding what to change.
        All fields in the trigger form are empty by default — only fill in
        fields you want to change; the rest will remain unchanged.
        """
        from utils.config_manager import get_all_configs
        configs = get_all_configs()

        print("\n" + "=" * 70)
        print("  CURRENT PIPELINE CONFIG  (fill trigger form to change)")
        print("=" * 70)

        current_group = None
        for key, meta in configs.items():
            if meta["group_name"] != current_group:
                current_group = meta["group_name"]
                print(f"\n  [{current_group}]")
            updated = meta["updated_at"] or "never"
            print(f"    {key:<30} = {meta['value']!r:<25} (updated: {updated[:19]})")

        print("=" * 70 + "\n")
        return configs

    @task()
    def update_symbol_filter(**context) -> dict:
        """
        Update symbol filter config from trigger params.

        Only updates keys that the user explicitly filled in (non-empty).
        Leave a field empty = keep the current DB value.
        """
        from utils.config_manager import set_configs, get_all_configs

        params = context.get("params", {})

        updates = {}
        for key in _SYMBOL_PARAMS:
            val = params.get(key)
            # Skip entirely if user left the field empty (None or blank)
            if val is None:
                continue
            val_str = str(val).strip()
            # We allow explicit empty string "" (e.g. to clear symbol_filter_market)
            updates[key] = val_str

        if not updates:
            print("[CONFIG] No symbol filter fields filled in — nothing updated")
            print("[CONFIG] Current DB values were shown in show_config task above.")
            return {}

        print("\n[CONFIG] Applying symbol filter updates:")
        for k, v in updates.items():
            print(f"  {k} = {v!r}")

        n = set_configs(updates)
        print(f"\n[CONFIG] Updated {n} config key(s). Producers will reload within 60s.")

        return get_all_configs()

    @task()
    def update_flush_settings(**context) -> dict:
        """
        Update flush/performance config from trigger params.

        Only updates keys that the user explicitly filled in (non-empty).
        Leave a field empty = keep the current DB value.
        """
        from utils.config_manager import set_configs, get_all_configs

        params = context.get("params", {})

        updates = {}
        for key in _FLUSH_PARAMS:
            val = params.get(key)
            if val is None:
                continue
            val_str = str(val).strip()
            if not val_str:
                continue  # flush fields should never be blank
            updates[key] = val_str

        if not updates:
            print("[CONFIG] No flush fields filled in — nothing updated")
            return {}

        print("\n[CONFIG] Applying flush setting updates:")
        for k, v in updates.items():
            print(f"  {k} = {v!r}")

        n = set_configs(updates)
        print(f"\n[CONFIG] Updated {n} config key(s). Services will reload within 300s.")

        return get_all_configs()

    @task()
    def reset_to_defaults() -> int:
        """Reset all pipeline_config keys to their default values."""
        from utils.config_manager import reset_to_defaults as _reset

        print("\n[CONFIG] Resetting all config to defaults...")
        n = _reset()
        print(f"[CONFIG] Reset {n} keys. Producers will reload within 60s.")
        return n

    @task()
    def verify_config(configs: dict) -> None:
        """Validate that the updated config has sensible values."""
        # configs may be empty dict if no updates were made — fetch fresh from DB
        if not configs:
            from utils.config_manager import get_all_configs
            configs = get_all_configs()

        warnings = []

        indexes = configs.get("symbol_filter_indexes", {}).get("value", "")
        groups  = configs.get("symbol_filter_groups",  {}).get("value", "")
        mode    = configs.get("symbol_filter_mode",    {}).get("value", "static")

        if mode == "db" and not indexes and not groups:
            warnings.append(
                "WARNING: mode=db but both symbol_filter_indexes and "
                "symbol_filter_groups are empty. This will subscribe ALL "
                "active instruments — may be very large!"
            )

        batch = configs.get("flush_batch_size", {}).get("value", "100")
        try:
            if int(batch) < 1 or int(batch) > 10000:
                warnings.append(f"WARNING: flush_batch_size={batch} is unusual (expected 1-10000)")
        except (ValueError, TypeError):
            warnings.append(f"WARNING: flush_batch_size={batch!r} is not an integer")

        if warnings:
            print("\n" + "\n".join(warnings))
        else:
            print("[CONFIG] Validation OK")

    # ── DAG wiring ─────────────────────────────────────────────────────────
    # Step 1: Show current DB values (user reference before any changes)
    current = show_config()

    # Step 2: Apply updates (only filled-in fields)
    sym_result   = update_symbol_filter()
    flush_result = update_flush_settings()

    # Step 3: Show final state + validate
    final = show_config()
    verify_config(final)

    # Reset is independent — user triggers separately when needed
    reset_to_defaults()


dag_pipeline_config()
