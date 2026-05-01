/**
 * order-columns.js — single source of truth for the INSERT and ON CONFLICT
 * column lists used by both _insertOrderSafe (ingestion.js) and
 * staging-merge.js. Prevents drift between the two write paths.
 */

// The 134 columns that are INSERTed from the Sticky.io API.
// Order must match the VALUES(?,?,?...) parameter list in _insertOrderSafe.
const INSERT_COLUMNS = [
  'client_id', 'order_id', 'customer_id', 'contact_id', 'is_anonymous_decline',
  'campaign_id', 'gateway_id', 'gateway_descriptor',
  'cc_first_6', 'cc_type', 'order_status', 'order_total',
  'decline_reason', 'decline_reason_details',
  'acquisition_date', 'date_created', 'billing_cycle', 'is_cascaded', 'retry_attempt',
  'is_recurring', 'tx_type', 'product_ids', 'ancestor_id',
  'billing_country', 'billing_state', 'ip_address',
  'prepaid', 'prepaid_match',
  'email_address', 'preserve_gateway',
  'is_chargeback', 'chargeback_date', 'is_refund', 'refund_amount', 'refund_date',
  'is_void', 'void_amount', 'void_date', 'amount_refunded_to_date',
  'click_id', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'device_category',
  'created_by', 'billing_model_id', 'billing_model_name', 'offer_id', 'subscription_id',
  'coupon_id', 'coupon_discount_amount', 'decline_salvage_discount_percent', 'rebill_discount_percent',
  'stop_after_next_rebill', 'on_hold', 'hold_date', 'order_confirmed',
  'parent_id', 'child_id', 'is_in_trial', 'order_subtotal', 'shipping_total', 'tax_total',
  'c1', 'c2', 'c3', 'affid',
  'time_stamp', 'is_test_cc', 'retry_date', 'tracking_number', 'shipping_date',
  'billing_first_name', 'billing_last_name', 'billing_street_address', 'billing_street_address2', 'billing_company_name', 'billing_state_id',
  'first_name', 'last_name', 'customers_telephone',
  'shipping_first_name', 'shipping_last_name', 'shipping_street_address', 'shipping_street_address2', 'shipping_company_name',
  'shipping_city', 'shipping_country', 'shipping_state', 'shipping_state_id', 'shipping_postcode', 'shipping_method_name', 'shipping_id',
  'cc_orig_first_6', 'cc_orig_last_4',
  'check_account_last_4', 'check_routing_last_4', 'check_ssn_last_4', 'check_transitnum',
  'main_product_id', 'main_product_quantity', 'upsell_product_id', 'upsell_product_quantity',
  'next_subscription_product', 'next_subscription_product_id', 'is_any_product_recurring', 'shippable',
  'aid', 'opt', 'sub_affiliate', 'created_by_user_name', 'credit_applied', 'promo_code',
  'current_rebill_discount_percent', 'order_confirmed_date', 'order_sales_tax', 'order_sales_tax_amount', 'shipping_amount', 'on_hold_by',
  'is_rma', 'rma_number', 'rma_reason', 'return_reason',
  'consent_required', 'consent_received', 'order_customer_types', 'website_received', 'website_sent', 'ip_address_lookup',
  'employee_notes', 'system_notes', 'custom_fields',
];

// Columns updated on ON CONFLICT(client_id, order_id) — all API-sourced
// columns EXCEPT client_id, order_id (the PK), and is_anonymous_decline
// (derived). Derived columns like cascade_chain, derived_product_role,
// processing_gateway_id, etc. are NOT in this list and thus preserved.
const ON_CONFLICT_SET_COLUMNS = [
  'customer_id', 'contact_id',
  'campaign_id', 'gateway_id', 'gateway_descriptor',
  'cc_first_6', 'cc_type', 'order_status', 'order_total',
  'decline_reason', 'decline_reason_details',
  'acquisition_date', 'date_created',
  'billing_cycle', 'is_cascaded', 'retry_attempt',
  'is_recurring', 'tx_type', 'product_ids', 'ancestor_id',
  'billing_country', 'billing_state', 'ip_address',
  'prepaid', 'prepaid_match',
  'email_address', 'preserve_gateway',
  'is_chargeback', 'chargeback_date', 'is_refund', 'refund_amount', 'refund_date',
  'is_void', 'void_amount', 'void_date', 'amount_refunded_to_date',
  'click_id', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'device_category',
  'created_by', 'billing_model_id', 'billing_model_name', 'offer_id', 'subscription_id',
  'coupon_id', 'coupon_discount_amount', 'decline_salvage_discount_percent', 'rebill_discount_percent',
  'stop_after_next_rebill', 'on_hold', 'hold_date', 'order_confirmed',
  'parent_id', 'child_id', 'is_in_trial', 'order_subtotal', 'shipping_total', 'tax_total',
  'c1', 'c2', 'c3', 'affid',
  'time_stamp', 'is_test_cc', 'retry_date', 'tracking_number', 'shipping_date',
  'billing_first_name', 'billing_last_name', 'billing_street_address', 'billing_street_address2', 'billing_company_name', 'billing_state_id',
  'first_name', 'last_name', 'customers_telephone',
  'shipping_first_name', 'shipping_last_name', 'shipping_street_address', 'shipping_street_address2', 'shipping_company_name',
  'shipping_city', 'shipping_country', 'shipping_state', 'shipping_state_id', 'shipping_postcode', 'shipping_method_name', 'shipping_id',
  'cc_orig_first_6', 'cc_orig_last_4',
  'check_account_last_4', 'check_routing_last_4', 'check_ssn_last_4', 'check_transitnum',
  'main_product_id', 'main_product_quantity', 'upsell_product_id', 'upsell_product_quantity',
  'next_subscription_product', 'next_subscription_product_id', 'is_any_product_recurring', 'shippable',
  'aid', 'opt', 'sub_affiliate', 'created_by_user_name', 'credit_applied', 'promo_code',
  'current_rebill_discount_percent', 'order_confirmed_date', 'order_sales_tax', 'order_sales_tax_amount', 'shipping_amount', 'on_hold_by',
  'is_rma', 'rma_number', 'rma_reason', 'return_reason',
  'consent_required', 'consent_received', 'order_customer_types', 'website_received', 'website_sent', 'ip_address_lookup',
  'employee_notes', 'system_notes', 'custom_fields',
];

// Derived columns pre-computed by staging-post-sync.js before merge.
// These are NOT in INSERT_COLUMNS (API doesn't set them) but exist in
// both staging and main orders tables.
const DERIVED_MERGE_COLUMNS = [
  'product_type_classified', 'derived_product_role', 'cascade_chain',
  'processing_gateway_id', 'product_group_id', 'product_group_name',
];

// MERGE_COLUMNS = INSERT_COLUMNS + DERIVED_MERGE_COLUMNS (140 total).
// Used by staging-merge.js to carry pre-computed derived values from
// staging into main during INSERT. For existing rows, COALESCE logic
// in MERGE_ON_CONFLICT_SET_SQL preserves main DB values when staging
// values are NULL.
const MERGE_COLUMNS = [...INSERT_COLUMNS, ...DERIVED_MERGE_COLUMNS];

// Pre-built SQL fragments for reuse
const INSERT_COLUMNS_SQL = INSERT_COLUMNS.join(', ');
const INSERT_PLACEHOLDERS_SQL = Array(INSERT_COLUMNS.length).fill('?').join(',');
const ON_CONFLICT_SET_SQL = ON_CONFLICT_SET_COLUMNS
  .map(c => `${c}=excluded.${c}`)
  .join(', ');

const MERGE_COLUMNS_SQL = MERGE_COLUMNS.join(', ');

// COALESCE prevents accidental clearing of derived values during merge.
// If staging pre-computed a value, it takes precedence. If staging has NULL
// (order wasn't classified in staging because it already existed in main),
// the existing main DB value is preserved.
// If intentional re-classification is needed, use explicit UPDATE statements
// or a dedicated reclassify function — do not set derived columns to NULL in
// staging and rely on merge to clear them.
const MERGE_ON_CONFLICT_SET_SQL = ON_CONFLICT_SET_SQL + ', ' +
  DERIVED_MERGE_COLUMNS
    .map(c => `${c}=COALESCE(excluded.${c}, orders.${c})`)
    .join(', ');

module.exports = {
  INSERT_COLUMNS,
  ON_CONFLICT_SET_COLUMNS,
  DERIVED_MERGE_COLUMNS,
  MERGE_COLUMNS,
  INSERT_COLUMNS_SQL,
  INSERT_PLACEHOLDERS_SQL,
  ON_CONFLICT_SET_SQL,
  MERGE_COLUMNS_SQL,
  MERGE_ON_CONFLICT_SET_SQL,
};
