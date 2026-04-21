<?php
/**
 * Config template for the BinRoute checkout extension.
 *
 * Drop this at the same path Beast's config lives:
 *   config/extensions/BinRoute.php
 *
 * Replace <API_KEY_HERE> with the plaintext key minted by BinRoute ops:
 *   ssh into the BinRoute server and run
 *     node scripts/issue-api-key.js 1 "kytsan-checkout"
 *   Copy the x-api-key from the printed block. The plaintext is shown ONCE.
 *
 * The `configurations` whitelist controls which funnel configId values the
 * extension activates on. Same pattern as Beast — keep tight during staging,
 * widen when ready.
 */

return [
    // Master kill switch. Flip to false to disable ALL hooks instantly
    // (no JS emitted, no AJAX calls, no CrmPayload mutation).
    'enable' => true,

    // BinRoute API base URL. No trailing slash.
    //   Production:  https://binroute.cswebform.cloud
    //   Staging/dev: http://127.0.0.1:3000
    'api_endpoint' => 'https://binroute.cswebform.cloud',

    // Numeric BinRoute client_id. Kytsan = 1.
    'client_id' => '1',

    // x-api-key value issued by scripts/issue-api-key.js on the BinRoute side.
    // NEVER commit a real key. Use env-var injection or a secrets store in
    // whatever mechanism Kytsan already uses for Beast's client_secret.
    'client_secret' => getenv( 'BINROUTE_API_KEY' ) ?: '<API_KEY_HERE>',

    // configIds on which this extension fires. Empty array = never fires.
    // Mirror the configurations array Beast uses so the extensions overlap
    // on the same funnel steps.
    'configurations' => [
        // 'your-checkout-config-id-here',
    ],

    // If your framework fires the CrmPayload hook twice on a single order
    // (parent + split), honour the main + skip splits. Match Beast's setting.
    'enable_split_order' => false,
];
