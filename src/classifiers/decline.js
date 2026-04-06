/**
 * Decline reason classifier.
 * Classifies decline_reason strings into buckets:
 * - crm_routing_rule: CRM blocked before reaching gateway (exclude from approval rates)
 * - processor: processor-side declines (primary optimization target)
 * - soft: soft declines (retry + reroute candidates)
 * - issuer: issuer-side declines (exclude from routing analysis)
 *
 * Special flags:
 * - CROSS_BANK_CASCADE_REQUIRED: declines where retrying the same bank = 0% recovery,
 *   but a different acquiring bank may succeed. Cascade must cross bank boundaries.
 *
 * Uses fuzzy text matching first, falls back to AI classification for unknowns.
 */

// ──────────────────────────────────────────────
// PATTERN DICTIONARIES
// ──────────────────────────────────────────────

const ISSUER_PATTERNS = [
  // Insufficient funds variants
  /insufficient\s*fund/i,
  /not\s*sufficient\s*fund/i,
  /nsf/i,
  /51\s*[-–—]\s*(declined|insufficient)/i,
  /^51\b/i,
  // Expired card
  /expir/i,
  /54\s*[-–—]/i,
  /^54\b/i,
  // Lost/stolen
  /lost/i,
  /stolen/i,
  /41\s*[-–—]/i,
  /43\s*[-–—]/i,
  /^41\b/i,
  /^43\b/i,
  // Do not honor
  /do\s*not\s*honor/i,
  /do\s*not\s*honour/i,
  /05\s*[-–—]/i,
  /^05\b/i,
  // Pickup card
  /pick\s*up/i,
  /04\s*[-–—]/i,
  // Cardholder requested stop
  /cardholder\s*(requested|stop)/i,
  // Invalid card / account
  /invalid\s*(card|account)\s*number/i,
  /no\s*such\s*(card|account|issuer)/i,
  /14\s*[-–—]/i,
  /^14\b/i,
  // Card not activated
  /not\s*activated/i,
  // Closed account
  /closed\s*account/i,
  /account\s*closed/i,
  // Exceeds withdrawal
  /exceeds?\s*withdrawal/i,
  /withdrawal\s*limit/i,
  // Suspected fraud by issuer
  /suspected\s*fraud/i,
  /fraud.*suspect/i,
  // Generic declined with issuer codes
  /declined.*issuer/i,
  /issuer.*declin/i,
  /refer\s*to\s*card\s*issuer/i,
  /^01\b/i,
  /contact\s*(card\s*)?issuer/i,
  // No account (without "no such" prefix)
  /^no\s*account$/i,
  // Customer/cardholder requested stop (all recurring)
  /customer\s*requested\s*stop/i,
  // Voice center referral
  /call\s*voice\s*center/i,
  // Generic transaction declined
  /^this\s*transaction\s*has\s*been\s*declined$/i,
  // Invalid credit card number (with optional REFID suffix)
  /invalid\s*credit\s*card\s*number/i,
  // Rejected / contact customer service
  /rejected.*contact.*cust/i,
  /contact\s*cust\s*serv/i,
  // Additional auth required (1A code)
  /add\s*auth\s*require/i,
  /1A\s*[-–—]\s*/i,
  // Account not recognized
  /account\s*not\s*recognized/i,
  // Generic decline or unable to parse
  /generic\s*decline/i,
  /unable\s*to\s*parse/i,
  // Cardholder's bank does not allow
  /cardholder.*bank.*does\s*not\s*allow/i,
  // Pin tries exceeded
  /pin\s*tries\s*exceeded/i,
];

const PROCESSOR_PATTERNS = [
  // Not permitted
  /not\s*permitted/i,
  /transaction\s*not\s*(permitted|allowed)/i,
  /57\s*[-–—]/i,
  /^57\b/i,
  // Restricted card
  /restricted\s*card/i,
  /restricted\s*merchant/i,
  /62\s*[-–—]/i,
  /^62\b/i,
  // Security violation
  /security\s*violation/i,
  /63\s*[-–—]/i,
  /^63\b/i,
  // Invalid merchant
  /invalid\s*merchant/i,
  /merchant\s*not\s*(permitted|allowed)/i,
  /58\s*[-–—]/i,
  // Decline CVV
  /cvv/i,
  /cv2/i,
  /cvc/i,
  /security\s*code/i,
  // AVS mismatch
  /avs/i,
  /address\s*(verification|mismatch)/i,
  // Processor decline
  /processor\s*declin/i,
  /gateway\s*reject/i,
  /gateway\s*declin/i,
  // Invalid transaction
  /invalid\s*transaction/i,
  /12\s*[-–—]/i,
  /^12\b/i,
  // Format error
  /format\s*error/i,
  /30\s*[-–—]/i,
  // MID/terminal issues
  /terminal/i,
  /mid\s*(not|error|invalid)/i,
  // Velocity / duplicate
  /duplicate\s*transaction/i,
  /velocity/i,
  // Gateway fraud filter — first-time card block
  /blocked[,\s]*first\s*used/i,
  // Invalid bankcard / merchant number
  /invalid\s*bankcard\s*merchant/i,
  // Card network restriction
  /credit\s*card\s*network\s*does\s*not\s*allow/i,
  // Authentication / 3DS failure
  /authentication\s*failed/i,
  // Invalid amount
  /invalid\s*amount/i,
  // Invalid billing address
  /invalid\s*billing\s*address/i,
  // Invalid zip
  /invalid\s*zip/i,
  // Card issuer does not allow this type of business
  /card\s*issuer\s*does\s*not\s*allow/i,
  // User not allowed to process (MID disabled, with REFID variants)
  /not\s*allowed\s*to\s*process/i,
  // Merchant account boarded incorrectly
  /merchant\s*account.*boarded/i,
  // Amount error
  /^amount\s*error$/i,
  // Payment type / currency not accepted (with REFID variants)
  /cc\s*payment\s*type.*not\s*accepted/i,
  /currency.*not\s*accepted/i,
  // Card number must contain only digits
  /card\s*number\s*must\s*contain/i,
  // Card security code never passed
  /card\s*security\s*code.*never/i,
];

const CRM_ROUTING_PATTERNS = [
  // CRM routing rule blocks — order never reaches a gateway
  /prepaid\s*(credit\s*)?cards?\s*(are|is)\s*not\s*accepted/i,
];

const SOFT_PATTERNS = [
  // Exceeds limit
  /exceeds?\s*(limit|amount)/i,
  /over\s*(limit|amount)/i,
  /61\s*[-–—]/i,
  /^61\b/i,
  // Frequency exceeded
  /frequency\s*exceed/i,
  /too\s*many\s*(attempt|transaction|request)/i,
  /82\s*[-–—]/i,
  // Try again
  /try\s*again/i,
  /re-?try/i,
  /please\s*retry/i,
  // Timeout
  /time\s*out/i,
  /timeout/i,
  /timed?\s*out/i,
  // System error (temporary)
  /system\s*error/i,
  /system\s*malfunction/i,
  /96\s*[-–—]/i,
  /^96\b/i,
  // Issuer unavailable
  /issuer.*unavailable/i,
  /unable\s*to\s*process/i,
  /network\s*(error|unavailable)/i,
  // Soft decline explicit
  /soft\s*decline/i,
  // Service not allowed
  /service\s*not\s*allowed/i,
  // Limit exceeded (reversed word order)
  /limit\s*exceed/i,
  /(activity|daily|monthly)\s*limit/i,
  // General / generic error
  /(general|generic)\s*error/i,
  // Re-enter transaction
  /re.?enter\s*transaction/i,
  // Unknown gateway response
  /unknown\s*response/i,
  // Bad BIN or host disconnect
  /bad\s*bin/i,
  /host\s*disconnect/i,
  // Daily threshold exceeded
  /daily\s*threshold/i,
  // Unknown error
  /^unknown\s*error$/i,
  // Cannot process transaction
  /cannot\s*process\s*transaction/i,
  // Internal error
  /^internal\s*error$/i,
  // Error processing transaction
  /error\s*processing\s*transaction/i,
  // Amount exceeds maximum ticket (with REFID variants)
  /amount\s*exceeds.*maximum\s*ticket/i,
  // Do not retry specific codes that are actually temporary
  /temporary/i,
  /intermittent/i,
];

// ──────────────────────────────────────────────
// CROSS-BANK CASCADE — retrying same bank = 0% recovery
// ──────────────────────────────────────────────

const CROSS_BANK_CASCADE_REQUIRED = [
  /blocked[,\s]*first\s*used/i,
];

/**
 * Check if a decline reason requires cascading to a different acquiring bank.
 * Same-bank retry or same-bank cascade = 0% recovery for these declines.
 * @param {string} declineReason
 * @returns {boolean}
 */
function requiresBankChange(declineReason) {
  if (!declineReason || typeof declineReason !== 'string') return false;
  const text = declineReason.trim();
  return CROSS_BANK_CASCADE_REQUIRED.some(pattern => pattern.test(text));
}

// ──────────────────────────────────────────────
// CLASSIFIER
// ──────────────────────────────────────────────

/**
 * Classify a decline reason string.
 * Returns: 'issuer' | 'processor' | 'soft' | null (unclassified)
 */
function classifyDecline(declineReason) {
  if (!declineReason || typeof declineReason !== 'string') return null;

  // Strip REFID suffix before matching — many processors append unique REFID:XXXXX
  const text = declineReason.trim().replace(/\s*REFID:\S*\s*$/i, '').trim();
  if (!text) return null;

  // Check CRM routing rules first — these never reached a gateway
  for (const pattern of CRM_ROUTING_PATTERNS) {
    if (pattern.test(text)) return 'crm_routing_rule';
  }

  // Check processor patterns first (more specific, fewer false positives)
  for (const pattern of PROCESSOR_PATTERNS) {
    if (pattern.test(text)) return 'processor';
  }

  // Then soft patterns (also specific)
  for (const pattern of SOFT_PATTERNS) {
    if (pattern.test(text)) return 'soft';
  }

  // Then issuer patterns (broadest catch)
  for (const pattern of ISSUER_PATTERNS) {
    if (pattern.test(text)) return 'issuer';
  }

  // Generic "DECLINED" without further detail — treat as issuer (most common)
  if (/^\d*\s*[-–—]?\s*declined?\s*$/i.test(text)) return 'issuer';

  return null; // Unclassified — candidate for AI classification
}

/**
 * Classify a batch of decline reasons.
 * Returns map of { declineReason: category }
 */
function classifyDeclineBatch(reasons) {
  const results = {};
  const unclassified = [];

  for (const reason of reasons) {
    const category = classifyDecline(reason);
    results[reason] = category;
    if (category === null) {
      unclassified.push(reason);
    }
  }

  return { results, unclassified };
}

/**
 * AI-assisted classification for unrecognized decline reasons.
 * Uses Anthropic API when available.
 */
async function classifyWithAI(reasons, apiKey) {
  if (!apiKey || reasons.length === 0) return {};

  // Lazy import to avoid dependency if not needed
  let axios;
  try {
    axios = require('axios');
  } catch {
    return {};
  }

  const prompt = `Classify each payment decline reason into exactly one category:
- "issuer": issuer-side declines (card issues, account issues, cardholder issues)
- "processor": processor-side declines (merchant/gateway config, security rules, format issues)
- "soft": soft/temporary declines (timeouts, system errors, rate limits, retry-able)

Decline reasons:
${reasons.map((r, i) => `${i + 1}. "${r}"`).join('\n')}

Respond with ONLY a JSON object mapping each decline reason string to its category. Example:
{"Insufficient funds": "issuer", "CVV mismatch": "processor"}`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json',
      },
      timeout: 15000,
    });

    const text = response.data.content[0].text;
    // Extract JSON from response
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      // Validate categories
      const validCategories = ['issuer', 'processor', 'soft'];
      const validated = {};
      for (const [key, value] of Object.entries(parsed)) {
        if (validCategories.includes(value)) {
          validated[key] = value;
        }
      }
      return validated;
    }
  } catch (err) {
    console.error('[DeclineClassifier] AI classification failed:', err.message);
  }

  return {};
}

module.exports = {
  classifyDecline,
  classifyDeclineBatch,
  classifyWithAI,
  requiresBankChange,
  // Exported for testing
  ISSUER_PATTERNS,
  PROCESSOR_PATTERNS,
  SOFT_PATTERNS,
  CRM_ROUTING_PATTERNS,
  CROSS_BANK_CASCADE_REQUIRED,
};
