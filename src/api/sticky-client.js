const axios = require('axios');

// ──────────────────────────────────────────────
// Token-bucket rate limiter — enforces Sticky.io 120 req/min limit
// ──────────────────────────────────────────────
class RateLimiter {
  constructor(maxPerMinute = 120, burstSize = 10) {
    this.tokens = burstSize;
    this.maxTokens = maxPerMinute;
    this.refillRate = maxPerMinute / 60; // tokens per second
    this.lastRefill = Date.now();
    this.queue = []; // FIFO queue of resolve callbacks
  }

  async acquire() {
    this._refill();
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return;
    }
    // No tokens — wait in queue
    return new Promise(resolve => {
      this.queue.push(resolve);
      // Schedule a drain check
      if (this.queue.length === 1) this._scheduleDrain();
    });
  }

  _refill() {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.maxTokens, this.tokens + elapsed * this.refillRate);
    this.lastRefill = now;
  }

  _scheduleDrain() {
    const waitMs = Math.ceil(1000 / this.refillRate); // time for ~1 token
    setTimeout(() => {
      this._refill();
      while (this.queue.length > 0 && this.tokens >= 1) {
        this.tokens -= 1;
        this.queue.shift()();
      }
      if (this.queue.length > 0) this._scheduleDrain();
    }, waitMs);
  }
}

// ──────────────────────────────────────────────
// Error classification for retry decisions
// ──────────────────────────────────────────────
function classifyError(err) {
  if (err.code === 'ENOTFOUND') return { type: 'dns', retryable: true, baseDelay: 5000 };
  if (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT') return { type: 'timeout', retryable: true, baseDelay: 3000 };
  if (err.code === 'ECONNRESET' || err.code === 'ECONNREFUSED') return { type: 'network', retryable: true, baseDelay: 2000 };
  if (err.response) {
    const status = err.response.status;
    if (status === 429) return { type: 'rate_limit', retryable: true, baseDelay: 2000 };
    if (status >= 500) return { type: 'server', retryable: true, baseDelay: 2000 };
    // 4xx (except 429) — not retryable
    return { type: 'client_error', retryable: false, baseDelay: 0 };
  }
  return { type: 'unknown', retryable: true, baseDelay: 2000 };
}

/**
 * Sticky.io API v1 client.
 * All calls are POST with application/x-www-form-urlencoded body + Basic auth.
 * Rate-limited to 120 req/min via token bucket.
 */
class StickyClient {
  constructor({ baseUrl, username, password }) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.auth = { username, password };
    this.rateLimiter = new RateLimiter(120, 10);
  }

  async _post(method, params = {}, retries = 5) {
    const url = `https://${this.baseUrl}/api/v1/${method}`;
    const formData = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null) {
        formData.append(key, String(value));
      }
    }

    for (let attempt = 1; attempt <= retries; attempt++) {
      await this.rateLimiter.acquire();

      try {
        const response = await axios.post(url, formData.toString(), {
          auth: this.auth,
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: 120000,
        });
        return response.data;
      } catch (err) {
        const { type, retryable, baseDelay } = classifyError(err);

        // Non-retryable (4xx client errors) — fail immediately
        if (!retryable) {
          const msg = `Sticky API ${method}: ${err.response?.status} - ${JSON.stringify(err.response?.data)}`;
          throw new Error(msg);
        }

        // Last attempt — throw
        if (attempt >= retries) {
          if (err.response) {
            throw new Error(`Sticky API ${method}: ${err.response.status} after ${retries} attempts`);
          }
          throw err;
        }

        // Timeout errors: fewer retries (each costs 120s)
        if (type === 'timeout' && attempt >= 3) {
          throw new Error(`Sticky API ${method}: timeout after ${attempt} attempts`);
        }

        // Exponential backoff with jitter
        const delay = Math.min(baseDelay * Math.pow(2, attempt - 1) + Math.random() * 1000, 30000);
        console.log(`[StickyClient] ${method} ${type} error (${err.code || err.message}), retry ${attempt}/${retries} in ${Math.round(delay)}ms...`);
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }

  // ──────────────────────────────────────────────
  // ORDER METHODS — campaign_id="all" + return_type="order_view"
  // ──────────────────────────────────────────────

  /**
   * Historical import: order_find with campaign_id="all", return_type="order_view".
   * Returns full order details inline — no separate order_view calls needed.
   */
  async orderFindAll(startDate, endDate, page = 1, resultsPerPage = 500) {
    return this._post('order_find', {
      campaign_id: 'all',
      start_date: startDate,
      end_date: endDate,
      start_time: '',
      end_time: '',
      date_type: 'create',
      criteria: 'all',
      search_type: 'all',
      return_type: 'order_view',
      results_per_page: resultsPerPage,
      page,
    });
  }

  /**
   * Daily incremental: order_find_updated with campaign_id="all".
   * Catches orders modified since last sync.
   */
  async orderFindUpdatedAll(startDate, endDate, page = 1, resultsPerPage = 500) {
    return this._post('order_find_updated', {
      campaign_id: 'all',
      start_date: startDate,
      end_date: endDate,
      start_time: '',
      end_time: '',
      return_type: 'order_view',
      results_per_page: resultsPerPage,
      page,
    });
  }

  /**
   * Single order view (fallback).
   */
  async orderView(orderId) {
    return this._post('order_view', { order_id: orderId });
  }

  /**
   * Extract orders array from order_find response.
   * Response can have orders as: numbered keys {"0": {...}, "1": {...}},
   * data array, or order_id list.
   */
  parseOrdersFromResponse(data) {
    if (!data || data.response_code !== '100') return [];

    // Check for data object/array
    if (data.data && typeof data.data === 'object') {
      return Array.isArray(data.data) ? data.data : Object.values(data.data);
    }

    // Check for numbered keys (Sticky.io returns orders as "0", "1", "2"...)
    const numericKeys = Object.keys(data).filter(k => /^\d+$/.test(k));
    if (numericKeys.length > 0) {
      return numericKeys.sort((a, b) => parseInt(a) - parseInt(b)).map(k => data[k]);
    }

    return [];
  }

  /**
   * Normalize a raw order into our DB schema fields.
   * Uses confirmed field names from Sticky.io API.
   */
  normalizeOrder(raw) {
    // Extract product_ids from products array
    let productIds = null;
    if (raw.products && Array.isArray(raw.products)) {
      productIds = JSON.stringify(raw.products.map(p => p.product_id).filter(Boolean));
    }

    // Classify transaction type from native fields
    const billingCycle = parseInt(raw.billing_cycle, 10) || 0;
    const isRecurring = raw.is_recurring === '1';
    const retryAttempt = parseInt(raw.retry_attempt, 10) || 0;
    const isCascaded = raw.is_cascaded === '1';

    const isAnonymousDecline = (raw.customer_id === '0' || raw.customer_id === '' || !raw.customer_id)
      && parseInt(raw.order_status, 10) === 7;

    let txType = null;
    if (isAnonymousDecline) {
      txType = 'anonymous_decline';
    } else if (retryAttempt > 0) {
      txType = 'salvage_attempt';
    } else if (billingCycle === 0 && !isRecurring) {
      txType = 'cp_initial';
    } else if (billingCycle === 0 && isRecurring) {
      txType = 'cp_upsell';
    } else if (billingCycle === 1) {
      txType = 'trial_conversion';
    } else if (billingCycle >= 2) {
      txType = 'recurring_rebill';
    }

    // Extract product-level fields from first product
    const firstProduct = (raw.products && Array.isArray(raw.products) && raw.products.length > 0)
      ? raw.products[0] : null;

    // UTM info
    const utm = raw.utm_info || {};

    // Totals breakdown
    const totals = raw.totals_breakdown || {};

    return {
      order_id: parseInt(raw.order_id, 10),
      customer_id: raw.customer_id === '0' || raw.customer_id === '' || !raw.customer_id
        ? null
        : parseInt(raw.customer_id, 10),
      contact_id: parseInt(raw.contact_id, 10) || null,
      is_anonymous_decline: (raw.customer_id === '0' || raw.customer_id === '' || !raw.customer_id)
        && parseInt(raw.order_status, 10) === 7 ? 1 : 0,
      campaign_id: parseInt(raw.campaign_id, 10) || null,
      gateway_id: parseInt(raw.gateway_id, 10) || null,
      gateway_descriptor: raw.gateway_descriptor || null,
      cc_first_6: raw.cc_first_6 || null,
      cc_type: raw.cc_type ? raw.cc_type.toLowerCase() : null,
      cc_last_4: raw.cc_last_4 || null,
      cc_expires: raw.cc_expires || null,
      order_status: parseInt(raw.order_status, 10) || null,
      order_total: parseFloat(raw.order_total) || 0,
      decline_reason: raw.decline_reason || null,
      decline_reason_details: raw.decline_reason_details || null,
      acquisition_date: raw.acquisition_date || null,
      date_created: raw.date_created || null,
      billing_cycle: raw.billing_cycle || '0',
      is_cascaded: isCascaded ? 1 : 0,
      retry_attempt: raw.retry_attempt || '0',
      is_recurring: raw.is_recurring || '0',
      tx_type: txType,
      product_ids: productIds,
      products_json: raw.products ? JSON.stringify(raw.products) : null,
      billing_country: raw.billing_country || null,
      billing_state: raw.billing_state || null,
      billing_city: raw.billing_city || null,
      billing_postcode: raw.billing_postcode || null,
      is_fraud: parseInt(raw.is_fraud, 10) || 0,
      is_3d_protected: raw.is_3d_protected === 'yes' ? 1 : 0,
      is_blacklisted: parseInt(raw.is_blacklisted, 10) || 0,
      affiliate: raw.affiliate || null,
      ancestor_id: parseInt(raw.ancestor_id, 10) || null,
      afid: raw.afid || null,
      sid: raw.sid || null,
      ip_address: raw.ip_address || null,
      prepaid: raw.prepaid || '0',
      prepaid_match: raw.prepaid_match || 'No',
      // Extended fields
      email_address: raw.email_address || null,
      preserve_gateway: raw.preserve_gateway === '1' ? 1 : 0,
      is_chargeback: raw.is_chargeback === '1' ? 1 : 0,
      chargeback_date: raw.chargeback_date || null,
      is_refund: raw.is_refund === 'yes' || raw.is_refund === '1' ? 1 : 0,
      refund_amount: parseFloat(raw.refund_amount) || 0,
      refund_date: raw.refund_date || null,
      is_void: raw.is_void === 'yes' || raw.is_void === '1' ? 1 : 0,
      void_amount: parseFloat(raw.void_amount) || 0,
      void_date: raw.void_date || null,
      amount_refunded_to_date: parseFloat(raw.amount_refunded_to_date) || 0,
      click_id: raw.click_id || null,
      utm_source: utm.source || null,
      utm_medium: utm.medium || null,
      utm_campaign: utm.campaign || null,
      utm_content: utm.content || null,
      utm_term: utm.term || null,
      device_category: utm.device_category || null,
      created_by: raw.created_by_employee_name || null,
      billing_model_id: firstProduct?.billing_model?.id ? parseInt(firstProduct.billing_model.id) : null,
      billing_model_name: firstProduct?.billing_model?.name || null,
      offer_id: firstProduct?.offer?.id ? parseInt(firstProduct.offer.id) : null,
      subscription_id: firstProduct?.subscription_id || null,
      coupon_id: raw.coupon_id || null,
      coupon_discount_amount: parseFloat(raw.coupon_discount_amount) || 0,
      decline_salvage_discount_percent: parseFloat(raw.decline_salvage_discount_percent) || 0,
      rebill_discount_percent: parseFloat(raw.rebill_discount_percent) || 0,
      stop_after_next_rebill: raw.stop_after_next_rebill === '1' ? 1 : 0,
      on_hold: raw.on_hold === '1' ? 1 : 0,
      hold_date: raw.hold_date || null,
      order_confirmed: raw.order_confirmed || null,
      time_stamp: raw.time_stamp || null,
      is_test_cc: raw.is_test_cc === '1' ? 1 : 0,
      retry_date: raw.retry_date || null,
      tracking_number: raw.tracking_number || null,
      shipping_date: raw.shipping_date || null,
      // Identity / billing address
      billing_first_name: raw.billing_first_name || null,
      billing_last_name: raw.billing_last_name || null,
      billing_street_address: raw.billing_street_address || null,
      billing_street_address2: raw.billing_street_address2 || null,
      billing_company_name: raw.billing_company_name || null,
      billing_state_id: raw.billing_state_id || null,
      first_name: raw.first_name || null,
      last_name: raw.last_name || null,
      customers_telephone: raw.customers_telephone || null,
      // Shipping address
      shipping_first_name: raw.shipping_first_name || null,
      shipping_last_name: raw.shipping_last_name || null,
      shipping_street_address: raw.shipping_street_address || null,
      shipping_street_address2: raw.shipping_street_address2 || null,
      shipping_company_name: raw.shipping_company_name || null,
      shipping_city: raw.shipping_city || null,
      shipping_country: raw.shipping_country || null,
      shipping_state: raw.shipping_state || null,
      shipping_state_id: raw.shipping_state_id || null,
      shipping_postcode: raw.shipping_postcode || null,
      shipping_method_name: raw.shipping_method_name || null,
      shipping_id: raw.shipping_id || null,
      // Card original BIN
      cc_orig_first_6: raw.cc_orig_first_6 || null,
      cc_orig_last_4: raw.cc_orig_last_4 || null,
      // ACH / check
      check_account_last_4: raw.check_account_last_4 || null,
      check_routing_last_4: raw.check_routing_last_4 || null,
      check_ssn_last_4: raw.check_ssn_last_4 || null,
      check_transitnum: raw.check_transitnum || null,
      // Product / subscription
      main_product_id: parseInt(raw.main_product_id, 10) || null,
      main_product_quantity: parseInt(raw.main_product_quantity, 10) || null,
      upsell_product_id: parseInt(raw.upsell_product_id, 10) || null,
      upsell_product_quantity: parseInt(raw.upsell_product_quantity, 10) || null,
      next_subscription_product: raw.next_subscription_product || null,
      next_subscription_product_id: parseInt(raw.next_subscription_product_id, 10) || null,
      is_any_product_recurring: raw.is_any_product_recurring === '1' ? 1 : 0,
      shippable: raw.shippable === '1' ? 1 : 0,
      // Order metadata
      aid: raw.aid || null,
      opt: raw.opt || null,
      sub_affiliate: raw.sub_affiliate || null,
      created_by_user_name: raw.created_by_user_name || null,
      credit_applied: parseFloat(raw.credit_applied) || 0,
      promo_code: raw.promo_code || null,
      current_rebill_discount_percent: parseFloat(raw.current_rebill_discount_percent) || 0,
      order_confirmed_date: raw.order_confirmed_date || null,
      order_sales_tax: parseFloat(raw.order_sales_tax) || 0,
      order_sales_tax_amount: parseFloat(raw.order_sales_tax_amount) || 0,
      shipping_amount: parseFloat(raw.shipping_amount) || 0,
      on_hold_by: raw.on_hold_by || null,
      // Returns
      is_rma: raw.is_rma === '1' || raw.is_rma === 'yes' ? 1 : 0,
      rma_number: raw.rma_number || null,
      rma_reason: raw.rma_reason || null,
      return_reason: raw.return_reason || null,
      // Compliance / misc
      consent_required: raw.consent_required === '1' ? 1 : 0,
      consent_received: raw.consent_received === '1' ? 1 : 0,
      order_customer_types: raw.order_customer_types || null,
      website_received: raw.website_received || null,
      website_sent: raw.website_sent || null,
      ip_address_lookup: raw.ip_Address_lookup || null,
      // Notes
      employee_notes: raw.employeeNotes ? JSON.stringify(raw.employeeNotes) : null,
      system_notes: raw.systemNotes ? JSON.stringify(raw.systemNotes) : null,
      custom_fields: raw.custom_fields ? JSON.stringify(raw.custom_fields) : null,
      parent_id: parseInt(raw.parent_id, 10) || null,
      child_id: raw.child_id ? parseInt(raw.child_id, 10) : null,
      is_in_trial: firstProduct?.is_in_trial === '1' ? 1 : 0,
      order_subtotal: parseFloat(totals.subtotal) || 0,
      shipping_total: parseFloat(totals.shipping) || 0,
      tax_total: parseFloat(totals.tax) || 0,
      c1: raw.c1 || null,
      c2: raw.c2 || null,
      c3: raw.c3 || null,
      affid: raw.affid || null,
    };
  }

  // ──────────────────────────────────────────────
  // PRODUCT METHODS
  // ──────────────────────────────────────────────

  /**
   * Fetch all products from Sticky.io via v2/products (paginated).
   * Returns array of { product_id, product_name, sku, price }.
   */
  async productIndex() {
    const products = [];
    let page = 1;
    while (true) {
      await this.rateLimiter.acquire();

      const url = `https://${this.baseUrl}/api/v2/products?page=${page}`;
      const response = await axios.get(url, {
        auth: this.auth,
        timeout: 30000,
      });
      const data = response.data;
      if (data.status !== 'SUCCESS' || !data.data || data.data.length === 0) break;

      for (const p of data.data) {
        products.push({
          product_id: String(p.id),
          product_name: p.name || null,
          sku: p.sku || null,
          price: p.price || null,
        });
      }

      if (page >= (data.last_page || 1)) break;
      page++;
    }
    return { response_code: '100', products };
  }

  // ──────────────────────────────────────────────
  // GATEWAY METHODS
  // ──────────────────────────────────────────────

  async gatewayView(gatewayId) {
    return this._post('gateway_view', { gateway_id: gatewayId });
  }

  /**
   * Scan gateways dynamically in batches of 50.
   * No hardcoded upper limit — keeps scanning until a full batch returns 0.
   * Returns { gateways: [...], highestId: number }.
   */
  async scanGateways(startId = 1) {
    const gateways = [];
    let batchStart = startId;
    const BATCH = 50;

    while (true) {
      const batchEnd = batchStart + BATCH;
      const batch = [];
      for (let j = batchStart; j < batchEnd; j++) {
        batch.push(
          this.gatewayView(j)
            .then(data => (data && data.response_code === '100') ? { gateway_id: j, ...data } : null)
            .catch(() => null)
        );
      }
      const results = await Promise.all(batch);
      const found = results.filter(Boolean);
      gateways.push(...found);

      console.log(`[StickyClient] Gateway batch ${batchStart}-${batchEnd - 1}: ${found.length} found`);

      // Stop if entire batch returned zero
      if (found.length === 0) break;

      batchStart = batchEnd;
    }

    const highestId = gateways.length > 0
      ? Math.max(...gateways.map(g => g.gateway_id))
      : 0;

    return { gateways, highestId };
  }

  // ──────────────────────────────────────────────
  // CAMPAIGN METHODS
  // ──────────────────────────────────────────────

  async campaignFindActive() {
    return this._post('campaign_find_active');
  }

  async getCampaign(campaignId) {
    await this.rateLimiter.acquire();

    const url = `https://${this.baseUrl}/api/v2/campaigns/${campaignId}`;
    try {
      const response = await axios.get(url, {
        auth: this.auth,
        timeout: 30000,
      });
      return response.data;
    } catch (err) {
      if (err.response && err.response.status === 404) return null;
      throw err;
    }
  }
}

module.exports = StickyClient;
