const { initDb, execSql, runSql, saveDb, closeDb } = require('./connection');

const SCHEMA_SQL = `
-- Clients (multi-tenant)
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT DEFAULT 'admin',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clients (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  sticky_base_url TEXT NOT NULL,
  sticky_username TEXT NOT NULL,
  sticky_password TEXT NOT NULL,
  alert_threshold REAL DEFAULT 5.0,
  analysis_window_days INTEGER DEFAULT 90,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Gateways / MIDs (confirmed fields from gateway_view)
CREATE TABLE IF NOT EXISTS gateways (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  gateway_id INTEGER NOT NULL,
  gateway_alias TEXT,
  gateway_provider TEXT,
  gateway_descriptor TEXT,
  gateway_active INTEGER DEFAULT 1,
  gateway_created DATETIME,
  gateway_type TEXT,
  gateway_currency TEXT,
  mid_group TEXT,
  global_monthly_cap REAL,
  monthly_sales REAL,
  -- Manual fields (user entry in config UI)
  processor_name TEXT,
  bank_name TEXT,
  mcc_code TEXT,
  mcc_label TEXT,
  acquiring_bin TEXT,
  -- Lifecycle tracking
  lifecycle_state TEXT DEFAULT 'active' CHECK(lifecycle_state IN ('active','ramp-up','degrading','closed')),
  last_checked DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(client_id, gateway_id)
);

-- Orders / Transactions (confirmed fields from order_view)
CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  order_id INTEGER NOT NULL,
  customer_id INTEGER,
  contact_id INTEGER,
  is_anonymous_decline INTEGER DEFAULT 0,
  campaign_id INTEGER,
  gateway_id INTEGER,
  gateway_descriptor TEXT,
  cc_first_6 TEXT,
  cc_type TEXT,
  order_status INTEGER,
  order_total REAL,
  decline_reason TEXT,
  decline_reason_details TEXT,
  decline_category TEXT,
  acquisition_date DATETIME,
  billing_cycle INTEGER DEFAULT 0,
  is_cascaded INTEGER DEFAULT 0,
  retry_attempt INTEGER DEFAULT 0,
  is_recurring INTEGER DEFAULT 0,
  tx_type TEXT,
  product_ids TEXT,
  ancestor_id INTEGER,
  products_json TEXT,
  billing_country TEXT,
  billing_state TEXT,
  billing_city TEXT,
  billing_postcode TEXT,
  is_fraud INTEGER DEFAULT 0,
  is_3d_protected INTEGER DEFAULT 0,
  is_blacklisted INTEGER DEFAULT 0,
  cc_last_4 TEXT,
  cc_expires TEXT,
  affiliate TEXT,
  afid TEXT,
  sid TEXT,
  transaction_type TEXT,
  cycle_number INTEGER,
  attempt_number INTEGER,
  ip_address TEXT,
  is_test INTEGER DEFAULT 0,
  recurring_date TEXT,
  auth_id TEXT,
  transaction_id TEXT,
  -- Columns from migrations (included in CREATE for fresh installs)
  prepaid TEXT DEFAULT '0',
  prepaid_match TEXT DEFAULT 'No',
  original_gateway_id INTEGER DEFAULT NULL,
  original_decline_reason TEXT DEFAULT NULL,
  derived_product_role TEXT DEFAULT NULL,
  derived_attempt INTEGER DEFAULT NULL,
  processing_gateway_id INTEGER DEFAULT NULL,
  is_internal_test INTEGER DEFAULT 0,
  derived_cycle INTEGER DEFAULT NULL,
  product_group_id INTEGER DEFAULT NULL,
  product_group_name TEXT DEFAULT NULL,
  product_type_classified TEXT DEFAULT NULL,
  upsell_parent_order_id INTEGER DEFAULT NULL,
  upsell_position INTEGER DEFAULT NULL,
  requires_bank_change INTEGER DEFAULT 0,
  offer_name TEXT DEFAULT NULL,
  derived_initial_attempt INTEGER DEFAULT NULL,
  cascade_chain TEXT DEFAULT NULL,
  -- Extended fields (Tier 1-3)
  email_address TEXT DEFAULT NULL,
  preserve_gateway INTEGER DEFAULT 0,
  is_chargeback INTEGER DEFAULT 0,
  chargeback_date TEXT DEFAULT NULL,
  is_refund INTEGER DEFAULT 0,
  refund_amount REAL DEFAULT 0,
  refund_date TEXT DEFAULT NULL,
  is_void INTEGER DEFAULT 0,
  void_amount REAL DEFAULT 0,
  void_date TEXT DEFAULT NULL,
  amount_refunded_to_date REAL DEFAULT 0,
  click_id TEXT DEFAULT NULL,
  utm_source TEXT DEFAULT NULL,
  utm_medium TEXT DEFAULT NULL,
  utm_campaign TEXT DEFAULT NULL,
  utm_content TEXT DEFAULT NULL,
  utm_term TEXT DEFAULT NULL,
  device_category TEXT DEFAULT NULL,
  created_by TEXT DEFAULT NULL,
  billing_model_id INTEGER DEFAULT NULL,
  billing_model_name TEXT DEFAULT NULL,
  offer_id INTEGER DEFAULT NULL,
  subscription_id TEXT DEFAULT NULL,
  coupon_id TEXT DEFAULT NULL,
  coupon_discount_amount REAL DEFAULT 0,
  decline_salvage_discount_percent REAL DEFAULT 0,
  rebill_discount_percent REAL DEFAULT 0,
  stop_after_next_rebill INTEGER DEFAULT 0,
  on_hold INTEGER DEFAULT 0,
  hold_date TEXT DEFAULT NULL,
  order_confirmed TEXT DEFAULT NULL,
  parent_id INTEGER DEFAULT NULL,
  child_id INTEGER DEFAULT NULL,
  is_in_trial INTEGER DEFAULT 0,
  order_subtotal REAL DEFAULT 0,
  shipping_total REAL DEFAULT 0,
  tax_total REAL DEFAULT 0,
  c1 TEXT DEFAULT NULL,
  c2 TEXT DEFAULT NULL,
  c3 TEXT DEFAULT NULL,
  affid TEXT DEFAULT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(client_id, order_id)
);

-- BIN performance aggregations
CREATE TABLE IF NOT EXISTS bin_performance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  bin TEXT NOT NULL,
  cc_type TEXT,
  gateway_id INTEGER NOT NULL,
  mcc_code TEXT,
  transaction_type TEXT,
  period_start DATE,
  period_end DATE,
  total_transactions INTEGER DEFAULT 0,
  approved_count INTEGER DEFAULT 0,
  declined_count INTEGER DEFAULT 0,
  issuer_declines INTEGER DEFAULT 0,
  processor_declines INTEGER DEFAULT 0,
  soft_declines INTEGER DEFAULT 0,
  approval_rate REAL,
  weighted_approval_rate REAL,
  tier INTEGER CHECK(tier IN (1,2,3)),
  calculated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(client_id, bin, gateway_id, transaction_type, period_end)
);

-- Recommendations
CREATE TABLE IF NOT EXISTS recommendations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  bin TEXT NOT NULL,
  cc_type TEXT,
  mcc_code TEXT,
  transaction_type TEXT,
  current_gateway_id INTEGER,
  recommended_gateway_id INTEGER,
  current_approval_rate REAL,
  recommended_approval_rate REAL,
  expected_lift REAL,
  confidence_score REAL,
  transaction_volume INTEGER,
  priority_score REAL,
  summary TEXT,
  status TEXT DEFAULT 'open' CHECK(status IN (
    'open','implemented','confirmed','inconclusive','regression','dismissed'
  )),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Implementation tracking
CREATE TABLE IF NOT EXISTS implementations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  recommendation_id INTEGER NOT NULL REFERENCES recommendations(id),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  marked_at DATETIME NOT NULL,
  baseline_snapshot_json TEXT,
  comparison_start_date DATE,
  comparison_end_date DATE,
  result TEXT DEFAULT 'waiting' CHECK(result IN (
    'waiting','comparing','confirmed','inconclusive','regression'
  )),
  result_data_json TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Alerts
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  priority TEXT NOT NULL CHECK(priority IN ('P0','P1','P2','P3')),
  alert_type TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  gateway_id INTEGER,
  bin TEXT,
  is_resolved INTEGER DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  resolved_at DATETIME
);

-- TX type mapping rules
CREATE TABLE IF NOT EXISTS tx_type_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER REFERENCES clients(id),
  campaign_id INTEGER,
  product_id INTEGER,
  assigned_type TEXT,
  is_cp_simulation INTEGER DEFAULT 0,
  notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Cycle grouping
CREATE TABLE IF NOT EXISTS cycle_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER REFERENCES clients(id),
  group_name TEXT NOT NULL,
  min_cycle INTEGER NOT NULL,
  max_cycle INTEGER,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Campaigns
CREATE TABLE IF NOT EXISTS campaigns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  campaign_id INTEGER NOT NULL,
  campaign_name TEXT,
  is_payment_routed INTEGER,
  payment_router_id INTEGER,
  gateway_ids TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(client_id, campaign_id)
);

-- Customers
CREATE TABLE IF NOT EXISTS customers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  customer_id INTEGER NOT NULL,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(client_id, customer_id)
);

-- Change log
CREATE TABLE IF NOT EXISTS change_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER,
  entity_type TEXT NOT NULL,
  entity_id INTEGER,
  action TEXT NOT NULL,
  field_name TEXT,
  old_value TEXT,
  new_value TEXT,
  changed_by TEXT DEFAULT 'system',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Product catalog (synced from Sticky)
CREATE TABLE IF NOT EXISTS products_catalog (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  product_id TEXT NOT NULL,
  product_name TEXT,
  last_synced DATETIME,
  UNIQUE(client_id, product_id)
);

-- Product groups
CREATE TABLE IF NOT EXISTS product_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  group_name TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Product group assignments
CREATE TABLE IF NOT EXISTS product_group_assignments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  product_group_id INTEGER NOT NULL REFERENCES product_groups(id),
  product_id TEXT NOT NULL,
  product_type TEXT CHECK(product_type IN ('initial','rebill','straight_sale','initial_rebill')),
  UNIQUE(client_id, product_id)
);

-- BIN lookup enrichment
CREATE TABLE IF NOT EXISTS bin_lookup (
  bin TEXT PRIMARY KEY,
  issuer_bank TEXT,
  card_brand TEXT,
  card_type TEXT,
  card_level TEXT,
  is_prepaid INTEGER DEFAULT 0,
  source TEXT,
  last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Sync state tracking
CREATE TABLE IF NOT EXISTS sync_state (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  sync_type TEXT NOT NULL,
  last_sync_at DATETIME,
  records_synced INTEGER DEFAULT 0,
  status TEXT DEFAULT 'idle',
  error_message TEXT,
  UNIQUE(client_id, sync_type)
);

-- Analytics cache (persistent, survives server restart)
CREATE TABLE IF NOT EXISTS analytics_cache (
  client_id INTEGER NOT NULL,
  output_type TEXT NOT NULL,
  cache_key TEXT NOT NULL,
  result_json TEXT,
  computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (client_id, output_type, cache_key)
);

-- Rule splits — tracks every split/merge event for both Beast and Flow Optix
CREATE TABLE IF NOT EXISTS rule_splits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL,
  rule_type TEXT NOT NULL,
  parent_rule_id INTEGER NOT NULL,
  child_rule_id INTEGER NOT NULL,
  split_level INTEGER NOT NULL,
  split_reason TEXT,
  variance_pp REAL,
  attempts_at_split INTEGER,
  bins_at_split TEXT,
  split_confirmed_by TEXT DEFAULT 'manual',
  split_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  merged_back_at DATETIME DEFAULT NULL,
  merged_back_reason TEXT DEFAULT NULL,
  is_active INTEGER DEFAULT 1
);

-- Flow Optix rules — rebill routing rules (mirrors beast_rules for rebills)
CREATE TABLE IF NOT EXISTS flow_optix_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER,
  rule_id TEXT,
  rule_name TEXT,
  tx_group TEXT,
  group_type TEXT,
  group_conditions TEXT,
  target_type TEXT,
  target_value TEXT,
  weightage_config TEXT,
  stage INTEGER DEFAULT 1,
  status TEXT DEFAULT 'recommended',
  baseline_rate REAL,
  current_rate REAL,
  predicted_rate REAL,
  predicted_lift_pp REAL,
  actual_lift_pp REAL,
  monthly_revenue_impact REAL,
  attempts_needed INTEGER DEFAULT 200,
  attempts_since_active INTEGER DEFAULT 0,
  implemented_at DATETIME,
  last_evaluated_at DATETIME,
  promoted_at DATETIME,
  next_action TEXT,
  next_action_threshold INTEGER,
  notes TEXT,
  split_from_rule_id INTEGER DEFAULT NULL,
  split_at DATETIME DEFAULT NULL,
  split_variance_pp REAL DEFAULT NULL,
  split_attempts INTEGER DEFAULT NULL,
  bins_at_split TEXT DEFAULT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
-- Processor intelligence — ranked acquisition priorities
CREATE TABLE IF NOT EXISTS processor_intelligence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL,
  processor_name TEXT NOT NULL,
  revenue_unlock_monthly REAL DEFAULT 0,
  coverage_pct REAL DEFAULT 0,
  groups_covered INTEGER DEFAULT 0,
  avg_approval_rate REAL DEFAULT 0,
  data_source TEXT DEFAULT 'historical',
  rank_order INTEGER DEFAULT 0,
  computed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- MID volume tracking — monthly cap utilization
CREATE TABLE IF NOT EXISTS mid_volume_tracking (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL,
  gateway_id INTEGER NOT NULL,
  month TEXT NOT NULL,
  volume_usd REAL DEFAULT 0,
  transaction_count INTEGER DEFAULT 0,
  cap_pct REAL DEFAULT 0,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Degradation alerts — approval rate drops
-- Beast rules — CRM routing rules with lifecycle tracking
CREATE TABLE IF NOT EXISTS beast_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL,
  rule_id TEXT,
  rule_name TEXT,
  tx_group TEXT,
  group_type TEXT,
  group_conditions TEXT,
  target_type TEXT,
  target_value TEXT,
  stage INTEGER DEFAULT 1,
  status TEXT DEFAULT 'recommended',
  baseline_rate REAL,
  current_rate REAL,
  predicted_rate REAL,
  predicted_lift_pp REAL,
  actual_lift_pp REAL,
  attempts_needed INTEGER DEFAULT 200,
  attempts_since_active INTEGER DEFAULT 0,
  split_from_rule_id INTEGER DEFAULT NULL,
  split_at DATETIME DEFAULT NULL,
  split_variance_pp REAL DEFAULT NULL,
  split_attempts INTEGER DEFAULT NULL,
  bins_at_split TEXT DEFAULT NULL,
  implemented_at DATETIME,
  last_evaluated_at DATETIME,
  notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS degradation_alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL,
  gateway_id INTEGER NOT NULL,
  issuer_bank TEXT,
  card_brand TEXT,
  rate_last_14d REAL,
  rate_prev_14d REAL,
  drop_pp REAL,
  alert_type TEXT,
  is_issuer_level INTEGER DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  resolved_at DATETIME DEFAULT NULL
);

-- Playbook implementation tracking — tracks routing rules marked as implemented
CREATE TABLE IF NOT EXISTS playbook_implementations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  issuer_bank TEXT NOT NULL,
  is_prepaid INTEGER NOT NULL DEFAULT 0,
  card_brand TEXT DEFAULT NULL,
  card_type TEXT DEFAULT NULL,
  rule_level TEXT NOT NULL DEFAULT 'bank' CHECK(rule_level IN ('bank','l4')),
  rule_type TEXT NOT NULL CHECK(rule_type IN (
    'initial_routing','cascade','upsell_routing','rebill_routing','salvage'
  )),
  recommended_processor TEXT,
  recommended_gateway_ids TEXT,
  recommended_detail_json TEXT,
  actual_processor TEXT,
  actual_gateway_ids TEXT,
  actual_detail_json TEXT,
  split_config_json TEXT DEFAULT NULL,
  baseline_json TEXT NOT NULL,
  baseline_period_start DATE,
  baseline_period_end DATE,
  status TEXT NOT NULL DEFAULT 'waiting' CHECK(status IN (
    'waiting','collecting','evaluating','confirmed','inconclusive',
    'regression','rolled_back','superseded','archived'
  )),
  collecting_start_date DATE,
  min_sample_target INTEGER NOT NULL DEFAULT 50,
  latest_checkpoint_json TEXT,
  verdict_at DATETIME,
  verdict_reason TEXT,
  rolled_back_at DATETIME,
  rollback_to_processor TEXT,
  rollback_to_gateway_ids TEXT,
  superseded_by_id INTEGER DEFAULT NULL,
  implemented_at DATETIME NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Implementation checkpoints — progressive snapshots over time
CREATE TABLE IF NOT EXISTS implementation_checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  implementation_id INTEGER NOT NULL REFERENCES playbook_implementations(id),
  checkpoint_day INTEGER NOT NULL,
  checked_at DATETIME NOT NULL,
  post_attempts INTEGER NOT NULL DEFAULT 0,
  post_approvals INTEGER NOT NULL DEFAULT 0,
  post_approval_rate REAL,
  post_avg_rpa REAL,
  new_side_attempts INTEGER,
  new_side_approvals INTEGER,
  new_side_rate REAL,
  old_side_attempts INTEGER,
  old_side_approvals INTEGER,
  old_side_rate REAL,
  baseline_rate REAL NOT NULL,
  lift_pp REAL NOT NULL DEFAULT 0,
  cohort_customers_acquired INTEGER,
  cohort_first_rebills_attempted INTEGER,
  cohort_first_rebills_approved INTEGER,
  meets_minimum_sample INTEGER DEFAULT 0,
  confounding_factors_json TEXT,
  status_at_checkpoint TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Implementation network feedback — cross-client learning from outcomes
CREATE TABLE IF NOT EXISTS implementation_network_feedback (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  implementation_id INTEGER NOT NULL REFERENCES playbook_implementations(id),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  normalized_bank TEXT NOT NULL,
  normalized_processor TEXT NOT NULL,
  rule_type TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK(outcome IN ('confirmed','inconclusive','regression')),
  lift_pp REAL,
  post_attempts INTEGER,
  applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- AI training feature table — denormalized transaction features for ML scoring
CREATE TABLE IF NOT EXISTS tx_features (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id INTEGER NOT NULL,
  client_id INTEGER NOT NULL,
  sticky_order_id INTEGER NOT NULL,

  -- LABEL
  outcome TEXT NOT NULL,

  -- ACQUIRING SIDE
  processor_name TEXT,
  acquiring_bank TEXT,
  mcc_code TEXT,

  -- ISSUING SIDE
  issuer_bank TEXT,
  card_brand TEXT,
  card_type TEXT,
  is_prepaid INTEGER DEFAULT 0,

  -- TRANSACTION
  amount REAL,
  tx_class TEXT,
  attempt_number INTEGER,
  cycle_depth TEXT,
  hour_of_day INTEGER,
  day_of_week INTEGER,
  prev_decline_reason TEXT,

  -- RELATIONSHIP
  initial_processor TEXT,

  -- VELOCITY (Layer 2, NULL until computed)
  mid_velocity_daily INTEGER,
  mid_velocity_weekly INTEGER,
  customer_history_on_proc INTEGER,
  bin_velocity_weekly INTEGER,

  -- METADATA
  acquisition_date DATETIME,
  computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  feature_version INTEGER DEFAULT 1,

  UNIQUE(client_id, sticky_order_id)
);
`;

const INDEXES_SQL = `
CREATE INDEX IF NOT EXISTS idx_orders_client_bin ON orders(client_id, cc_first_6);
CREATE INDEX IF NOT EXISTS idx_orders_client_gateway ON orders(client_id, gateway_id);
CREATE INDEX IF NOT EXISTS idx_orders_client_status ON orders(client_id, order_status);
CREATE INDEX IF NOT EXISTS idx_orders_client_date ON orders(client_id, acquisition_date);
CREATE INDEX IF NOT EXISTS idx_orders_client_txtype ON orders(client_id, tx_type);
CREATE INDEX IF NOT EXISTS idx_orders_client_cascade ON orders(client_id, is_cascaded);
CREATE INDEX IF NOT EXISTS idx_orders_client_anon ON orders(client_id, is_anonymous_decline);
CREATE INDEX IF NOT EXISTS idx_orders_client_contact ON orders(client_id, contact_id);
CREATE INDEX IF NOT EXISTS idx_bin_perf_client_bin ON bin_performance(client_id, bin);
CREATE INDEX IF NOT EXISTS idx_bin_perf_client_gw ON bin_performance(client_id, gateway_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_client ON recommendations(client_id, status);
CREATE INDEX IF NOT EXISTS idx_alerts_client ON alerts(client_id, is_resolved);
CREATE INDEX IF NOT EXISTS idx_gateways_client ON gateways(client_id, lifecycle_state);
CREATE INDEX IF NOT EXISTS idx_orders_derived_role ON orders(client_id, derived_product_role, order_status);
CREATE INDEX IF NOT EXISTS idx_orders_derived_cycle ON orders(client_id, derived_cycle, derived_attempt);
CREATE INDEX IF NOT EXISTS idx_orders_processing_gw ON orders(client_id, processing_gateway_id, order_status);
CREATE INDEX IF NOT EXISTS idx_orders_customer_product ON orders(client_id, customer_id, product_group_id);
CREATE INDEX IF NOT EXISTS idx_orders_date_test ON orders(acquisition_date, is_test, is_internal_test);
CREATE INDEX IF NOT EXISTS idx_orders_role_date ON orders(derived_product_role, acquisition_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer_cascade ON orders(customer_id, is_cascaded, order_status);
CREATE INDEX IF NOT EXISTS idx_orders_pgw ON orders(processing_gateway_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer_role_cycle ON orders(client_id, customer_id, derived_product_role, derived_cycle, derived_attempt);
CREATE INDEX IF NOT EXISTS idx_pb_impl_client_status ON playbook_implementations(client_id, status);
CREATE INDEX IF NOT EXISTS idx_pb_impl_client_bank ON playbook_implementations(client_id, issuer_bank, is_prepaid, rule_type);
CREATE INDEX IF NOT EXISTS idx_impl_checkpoints_impl ON implementation_checkpoints(implementation_id, checkpoint_day);
CREATE INDEX IF NOT EXISTS idx_impl_network_bank ON implementation_network_feedback(normalized_bank, normalized_processor, rule_type);
CREATE INDEX IF NOT EXISTS idx_txf_client_outcome ON tx_features(client_id, outcome);
CREATE INDEX IF NOT EXISTS idx_txf_processor ON tx_features(processor_name, outcome);
CREATE INDEX IF NOT EXISTS idx_txf_issuer ON tx_features(issuer_bank, outcome);
CREATE INDEX IF NOT EXISTS idx_txf_order ON tx_features(order_id);
CREATE INDEX IF NOT EXISTS idx_txf_version ON tx_features(feature_version);
`;

/**
 * Safe startup — CREATE IF NOT EXISTS. Never drops data.
 * Used by server.js and all normal operations.
 */
async function initializeDatabase() {
  const db = await initDb();
  const statements = SCHEMA_SQL.trim().split(';').filter(s => s.trim());
  for (const stmt of statements) {
    // Force IF NOT EXISTS on all CREATE TABLE statements for safe startup
    const safe = stmt.replace(/CREATE TABLE(?!\s+IF\s+NOT\s+EXISTS)\b/gi, 'CREATE TABLE IF NOT EXISTS');
    execSql(safe);
  }
  const indexStatements = INDEXES_SQL.trim().split(';').filter(s => s.trim());
  for (const stmt of indexStatements) {
    execSql(stmt);
  }

  // Update SQLite query planner statistics for optimal index selection
  execSql('ANALYZE');

  // Migrations — ALTER TABLE for existing tables (safe to re-run)
  const migrations = [
    "ALTER TABLE orders ADD COLUMN prepaid TEXT DEFAULT '0'",
    "ALTER TABLE orders ADD COLUMN prepaid_match TEXT DEFAULT 'No'",
    // Gateways — cap tracking + warming up
    'ALTER TABLE gateways ADD COLUMN monthly_cap REAL DEFAULT NULL',
    'ALTER TABLE gateways ADD COLUMN cap_warning_sent INTEGER DEFAULT 0',
    'ALTER TABLE gateways ADD COLUMN is_warming_up INTEGER DEFAULT 0',
    'ALTER TABLE gateways ADD COLUMN warming_up_since DATETIME DEFAULT NULL',
    // Flow Optix rules — fallback chain + processor selection
    'ALTER TABLE flow_optix_rules ADD COLUMN primary_gateway_id INTEGER DEFAULT NULL',
    'ALTER TABLE flow_optix_rules ADD COLUMN secondary_gateway_id INTEGER DEFAULT NULL',
    'ALTER TABLE flow_optix_rules ADD COLUMN tertiary_gateway_id INTEGER DEFAULT NULL',
    'ALTER TABLE flow_optix_rules ADD COLUMN chain_updated_at DATETIME DEFAULT NULL',
    'ALTER TABLE flow_optix_rules ADD COLUMN processor_selection_type TEXT DEFAULT NULL',
    // Cascade data — original gateway that declined before cascade
    'ALTER TABLE orders ADD COLUMN original_gateway_id INTEGER DEFAULT NULL',
    'ALTER TABLE orders ADD COLUMN original_decline_reason TEXT DEFAULT NULL',
    // Product sequence — main vs upsell per product group
    "ALTER TABLE product_groups ADD COLUMN product_sequence TEXT DEFAULT NULL",
    // Derived product role — single source of truth for order classification
    "ALTER TABLE orders ADD COLUMN derived_product_role TEXT DEFAULT NULL",
    // Campaign type — main, reprocessing, recovery, etc.
    "ALTER TABLE campaigns ADD COLUMN campaign_type TEXT DEFAULT NULL",
    // Derived attempt — which attempt within a subscription cycle
    "ALTER TABLE orders ADD COLUMN derived_attempt INTEGER DEFAULT NULL",
    // Processing gateway — true first-attempt gateway (corrected for cascades)
    "ALTER TABLE orders ADD COLUMN processing_gateway_id INTEGER DEFAULT NULL",
    // Internal test flag — filter test orders from analytics
    "ALTER TABLE orders ADD COLUMN is_internal_test INTEGER DEFAULT 0",
    // Gateway exclusion from analysis
    "ALTER TABLE gateways ADD COLUMN exclude_from_analysis INTEGER DEFAULT 0",
    // Client cascade flag — whether this client uses cascade routing
    "ALTER TABLE clients ADD COLUMN uses_cascade INTEGER DEFAULT NULL",
    // Client sticky domain — extracted from sticky_base_url for cascade CSV matching
    "ALTER TABLE clients ADD COLUMN sticky_domain TEXT DEFAULT NULL",
    // Client min sample size for analytics
    "ALTER TABLE clients ADD COLUMN min_sample_size INTEGER DEFAULT 30",
    // Subscription features (Layer 2.5) — rebill-specific signals
    "ALTER TABLE tx_features ADD COLUMN consecutive_approvals INTEGER DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN days_since_last_charge REAL DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN days_since_initial REAL DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN lifetime_charges INTEGER DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN lifetime_revenue REAL DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN initial_amount REAL DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN amount_ratio REAL DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN prior_declines_in_cycle INTEGER DEFAULT NULL",
    // Cascade chain — full cascade gateway sequence from system notes
    "ALTER TABLE orders ADD COLUMN cascade_chain TEXT DEFAULT NULL",
    // Extended order fields — captured from order_view for AI, profitability, and salvage
    "ALTER TABLE orders ADD COLUMN email_address TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN preserve_gateway INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN is_chargeback INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN chargeback_date TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN is_refund INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN refund_amount REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN refund_date TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN is_void INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN void_amount REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN void_date TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN amount_refunded_to_date REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN click_id TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN utm_source TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN utm_medium TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN utm_campaign TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN utm_content TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN utm_term TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN device_category TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN created_by TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN billing_model_id INTEGER DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN billing_model_name TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN offer_id INTEGER DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN subscription_id TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN coupon_id TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN coupon_discount_amount REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN decline_salvage_discount_percent REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN rebill_discount_percent REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN stop_after_next_rebill INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN on_hold INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN hold_date TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN order_confirmed TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN parent_id INTEGER DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN child_id INTEGER DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN is_in_trial INTEGER DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN order_subtotal REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN shipping_total REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN tax_total REAL DEFAULT 0",
    "ALTER TABLE orders ADD COLUMN c1 TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN c2 TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN c3 TEXT DEFAULT NULL",
    "ALTER TABLE orders ADD COLUMN affid TEXT DEFAULT NULL",
    // Cascade chain features for AI model
    "ALTER TABLE tx_features ADD COLUMN cascade_depth INTEGER DEFAULT 0",
    "ALTER TABLE tx_features ADD COLUMN cascade_processors_tried TEXT DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN cascade_decline_reasons TEXT DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN mid_age_days REAL DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN offer_name TEXT DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN training_client_id TEXT DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN billing_state TEXT DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN last_approved_processor TEXT DEFAULT NULL",
    "ALTER TABLE tx_features ADD COLUMN parent_declined_processor TEXT DEFAULT NULL",
  ];
  for (const m of migrations) {
    try { execSql(m); } catch (e) {
      // "duplicate column name" is expected on subsequent runs — ignore
      if (!e.message || !e.message.includes('duplicate column')) throw e;
    }
  }

  saveDb();
  console.log('Database schema initialized successfully.');
  return db;
}

/**
 * Clean rebuild — DROP then CREATE. Destroys all data except clients.
 * Only called explicitly via CLI: node schema.js reset
 */
async function resetDatabase() {
  const db = await initDb();
  const dropOrder = [
    'tx_features',
    'bin_lookup', 'product_group_assignments', 'product_groups', 'products_catalog',
    'implementations', 'recommendations', 'bin_performance',
    'change_log', 'alerts', 'sync_state', 'orders',
    'tx_type_rules', 'cycle_groups', 'campaigns', 'customers', 'gateways'
  ];
  for (const table of dropOrder) {
    try { execSql(`DROP TABLE IF EXISTS ${table}`); } catch {}
  }
  const statements = SCHEMA_SQL.trim().split(';').filter(s => s.trim());
  for (const stmt of statements) {
    execSql(stmt);
  }
  const indexStatements = INDEXES_SQL.trim().split(';').filter(s => s.trim());
  for (const stmt of indexStatements) {
    execSql(stmt);
  }
  saveDb();
  console.log('Database RESET — all tables dropped and recreated (clients preserved).');
  return db;
}

if (require.main === module) {
  const cmd = process.argv[2];
  const fn = cmd === 'reset' ? resetDatabase : initializeDatabase;
  fn()
    .then(() => { closeDb(); console.log('Done.'); })
    .catch(err => { console.error('Schema error:', err); process.exit(1); });
}

module.exports = { initializeDatabase, resetDatabase };
