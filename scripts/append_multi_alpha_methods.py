"""Append Multi-Alpha helper methods to qe_evolution_service.py."""

CODE = '''

    # ── Multi-Alpha Phase 3: helper methods ──────────────────────────────

    def _detect_alpha_mode(self, task: dict) -> str:
        """Detect alpha_mode from task or base experiment records."""
        base_exp_id = task.get("base_experiment_id")
        if base_exp_id:
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT alpha_mode FROM qe_experiments WHERE experiment_id = %s",
                            (base_exp_id,),
                        )
                        row = cur.fetchone()
                        if row and row[0] and row[0] != "single":
                            return row[0]
            except Exception:
                pass
        return "single"

    async def _submit_multi_alpha_loop(
        self,
        task: dict,
        task_id: str,
        loop_index: int,
        evolution_loop_db_id: str,
    ):
        """Submit a Multi-Alpha evolution loop.

        Phase 3: generates sub-experiments for each alpha group, stores results.
        Actual Qlib execution dispatch is handled by the existing node infrastructure.
        """
        logger.info(f"Multi-Alpha loop {loop_index} for task {task_id}")

        # Create LOOP record
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO qe_evolution_loops "
                    "(loop_id, task_id, loop_index, status) "
                    "VALUES (%s, %s, %s, 'running') "
                    "ON CONFLICT (loop_id) DO UPDATE SET status = 'running', updated_at = NOW()",
                    (evolution_loop_db_id, task_id, loop_index),
                )
            conn.commit()

        try:
            base_exp_id = task.get("base_experiment_id")
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT multi_alpha_config, factor_names, model_id, strategy_id, "
                        "data_split, custom_params, strategy_params "
                        "FROM qe_experiments WHERE experiment_id = %s",
                        (base_exp_id,),
                    )
                    exp_row = cur.fetchone()

            if not exp_row or not exp_row[0]:
                raise ValueError(f"Base experiment {base_exp_id} has no multi_alpha_config")

            from .experiment_config_builders import build_config_from_multi_alpha

            multi_alpha_raw = exp_row[0]
            if isinstance(multi_alpha_raw, str):
                multi_alpha_raw = json.loads(multi_alpha_raw)

            data_split = exp_row[4]
            if isinstance(data_split, str):
                data_split = json.loads(data_split)
            strat_params = exp_row[6]
            if isinstance(strat_params, str):
                strat_params = json.loads(strat_params)

            cfg = build_config_from_multi_alpha(
                multi_alpha_config=multi_alpha_raw,
                data_split=data_split,
                strategy_id=exp_row[3],
                strategy_params=strat_params,
                node_id=task.get("node_id"),
                experiment_name=f"{task_id}_Loop{loop_index}",
            )

            from .multi_alpha_engine import MultiAlphaEngine

            engine = MultiAlphaEngine(cfg)
            result = engine.run()

            config_json = {
                "alpha_mode": "multi",
                "multi_alpha_config": (
                    cfg.multi_alpha_config.model_dump() if cfg.multi_alpha_config else None
                ),
                "group_configs": result.get("group_configs"),
                "meta_method": result.get("meta_method"),
            }
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops "
                        "SET status = 'completed', config_json = %s, updated_at = NOW() "
                        "WHERE loop_id = %s",
                        (json.dumps(config_json, default=str), evolution_loop_db_id),
                    )
                    cur.execute(
                        "UPDATE qe_evolution_tasks "
                        "SET current_loop = %s, updated_at = NOW() "
                        "WHERE task_id = %s",
                        (loop_index, task_id),
                    )
                conn.commit()

            logger.info(
                f"Multi-Alpha loop {loop_index} completed: "
                f"{result['total_groups']} groups"
            )
            return evolution_loop_db_id

        except Exception as e:
            logger.error(
                f"Multi-Alpha loop {loop_index} failed: {e}", exc_info=True
            )
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE qe_evolution_loops "
                        "SET status = 'failed', error_message = %s, updated_at = NOW() "
                        "WHERE loop_id = %s",
                        (str(e)[:2000], evolution_loop_db_id),
                    )
                conn.commit()
            return None
'''

target = '/mnt/f/Dev/AIstock/backend/services/quantevolver/qe_evolution_service.py'

with open(target, 'r') as f:
    content = f.read()

# Check if already appended
if 'def _detect_alpha_mode' in content:
    print("Methods already appended, skipping")
else:
    with open(target, 'a') as f:
        f.write(CODE)
    print(f"Appended {len(CODE)} chars to {target}")
