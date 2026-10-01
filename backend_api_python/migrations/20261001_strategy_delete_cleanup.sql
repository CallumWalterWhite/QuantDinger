-- Remove stale strategy runtime data and make future strategy deletes atomic.
-- The same statements are included in init.sql for automatic upgrades.

CREATE OR REPLACE FUNCTION qd_cleanup_deleted_strategy()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qd_execution_events AS event
    SET processed_at = COALESCE(event.processed_at, NOW()),
        process_error = 'strategy_deleted',
        next_attempt_at = NOW()
    WHERE event.processed_at IS NULL
      AND EXISTS (
        SELECT 1
        FROM qd_live_order_bindings AS binding
        WHERE binding.strategy_id = OLD.id
          AND binding.credential_id = event.credential_id
          AND LOWER(binding.exchange_id) = LOWER(event.exchange_id)
          AND (
            (event.exchange_order_id <> '' AND binding.exchange_order_id = event.exchange_order_id)
            OR (event.client_order_id <> '' AND binding.client_order_id = event.client_order_id)
          )
      );

    DELETE FROM pending_orders WHERE strategy_id = OLD.id;
    DELETE FROM qd_live_order_bindings WHERE strategy_id = OLD.id;
    DELETE FROM strategy_runtime_locks
    WHERE strategy_run_id IN (
        SELECT id FROM strategy_runs WHERE strategy_id = OLD.id
    );
    DELETE FROM strategy_order_fills WHERE strategy_id = OLD.id;
    DELETE FROM strategy_order_intents WHERE strategy_id = OLD.id;
    DELETE FROM strategy_runtime_state WHERE strategy_id = OLD.id;
    DELETE FROM strategy_runtime_events WHERE strategy_id = OLD.id;
    DELETE FROM strategy_runs WHERE strategy_id = OLD.id;
    DELETE FROM qd_strategy_commands WHERE strategy_id = OLD.id;
    DELETE FROM qd_strategy_runtime_leases WHERE strategy_id = OLD.id;

    UPDATE qd_backtest_runs SET strategy_id = NULL WHERE strategy_id = OLD.id;
    UPDATE qd_backtest_trades SET strategy_id = NULL WHERE strategy_id = OLD.id;
    UPDATE qd_indicator_codes SET source_strategy_id = NULL WHERE source_strategy_id = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cleanup_deleted_strategy ON qd_strategies_trading;
CREATE TRIGGER trg_cleanup_deleted_strategy
BEFORE DELETE ON qd_strategies_trading
FOR EACH ROW EXECUTE FUNCTION qd_cleanup_deleted_strategy();

UPDATE qd_execution_events AS event
SET processed_at = COALESCE(event.processed_at, NOW()),
    process_error = 'strategy_deleted',
    next_attempt_at = NOW()
WHERE event.processed_at IS NULL
  AND EXISTS (
    SELECT 1
    FROM qd_live_order_bindings AS binding
    WHERE binding.strategy_id > 0
      AND NOT EXISTS (
        SELECT 1 FROM qd_strategies_trading AS strategy
        WHERE strategy.id = binding.strategy_id
      )
      AND binding.credential_id = event.credential_id
      AND LOWER(binding.exchange_id) = LOWER(event.exchange_id)
      AND (
        (event.exchange_order_id <> '' AND binding.exchange_order_id = event.exchange_order_id)
        OR (event.client_order_id <> '' AND binding.client_order_id = event.client_order_id)
      )
  );

DELETE FROM strategy_runtime_locks AS runtime_lock
WHERE runtime_lock.strategy_run_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM strategy_runs AS run
    WHERE run.id = runtime_lock.strategy_run_id
  );

DELETE FROM strategy_runtime_locks AS runtime_lock
WHERE runtime_lock.strategy_run_id IN (
    SELECT run.id
    FROM strategy_runs AS run
    WHERE run.strategy_id > 0
      AND NOT EXISTS (
        SELECT 1 FROM qd_strategies_trading AS strategy
        WHERE strategy.id = run.strategy_id
      )
);

DELETE FROM pending_orders AS pending
WHERE pending.strategy_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = pending.strategy_id
  );

DELETE FROM qd_live_order_bindings AS binding
WHERE binding.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = binding.strategy_id
  );

DELETE FROM strategy_order_fills AS fill
WHERE fill.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = fill.strategy_id
  );

DELETE FROM strategy_order_intents AS intent
WHERE intent.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = intent.strategy_id
  );

DELETE FROM strategy_runtime_state AS runtime_state
WHERE runtime_state.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = runtime_state.strategy_id
  );

DELETE FROM strategy_runtime_events AS runtime_event
WHERE runtime_event.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = runtime_event.strategy_id
  );

DELETE FROM strategy_runs AS run
WHERE run.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = run.strategy_id
  );

DELETE FROM qd_strategy_commands AS command
WHERE command.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = command.strategy_id
  );

DELETE FROM qd_strategy_runtime_leases AS lease
WHERE lease.strategy_id > 0
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = lease.strategy_id
  );

UPDATE qd_backtest_runs AS run
SET strategy_id = NULL
WHERE run.strategy_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = run.strategy_id
  );

UPDATE qd_backtest_trades AS trade
SET strategy_id = NULL
WHERE trade.strategy_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = trade.strategy_id
  );

UPDATE qd_indicator_codes AS listing
SET source_strategy_id = NULL
WHERE listing.source_strategy_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM qd_strategies_trading AS strategy
    WHERE strategy.id = listing.source_strategy_id
  );
