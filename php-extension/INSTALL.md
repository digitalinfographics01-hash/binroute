# BinRoute PHP Extension — Install Instructions

Stage-0 shadow-mode extension for Kytsan's checkout funnel.
Mirrors the structure of `Extension\BeastInsights\BeastInsights` and drops into
the same framework. **Never sets `forceGatewayId`** — only appends a
`BinRoute_shadow: id=<uuid> ...` marker to `customNotes` so our server can
reconcile the logged recommendation against the actual order outcome.

## What it does

1. When a customer types the first 6 digits of a card, JS fires an AJAX call to
   `extensions/binroute/bin-routing` on your server.
2. The PHP handler forwards to BinRoute's `/api/route` with API-key auth,
   receives a recommendation (`shadow_id`, `gateway_id`, `processor`,
   `confidence`, `reason`), and stores the response in session.
3. At form submit the page pings `extensions/binroute/submit-timing` via
   `navigator.sendBeacon` so we know whether our response arrived before
   Place Order was clicked.
4. Just before the CRM payload is submitted to Sticky.io, `addShadowNote()`
   appends the marker to `customNotes`. Sticky.io carries it into the imported
   order; BinRoute's post-sync reconciler matches `shadow_id` and fills in
   `actual_gateway_id`, `actual_outcome`, `would_match`.

**Beast Insights keeps routing the actual traffic.** This extension is pure
observability for Stage 0.

## File placement

Drop into the same directories you use for Beast Insights:

```
Extension/BinRoute/BinRoute.php        ← main class (namespace Extension\BinRoute)
config/extensions/BinRoute.php         ← config (matches Beast's config path)
```

Whatever mechanism auto-discovers `Extension\BeastInsights\BeastInsights`
will discover `Extension\BinRoute\BinRoute` the same way. No new registration
plumbing is required.

## Hook wiring

Four public methods, same lifecycle as Beast:

| Method           | Framework hook                                                        |
|------------------|-----------------------------------------------------------------------|
| `addJavaScript`  | Page-render hook — same point `BeastInsights::addJavaScript` fires.  |
| `BinRouting`     | AJAX path `extensions/binroute/bin-routing` (kebab → PascalCase).    |
| `SubmitTiming`   | AJAX path `extensions/binroute/submit-timing`.                       |
| `addShadowNote`  | CrmPayload pre-submission hook — same as `addForceGateway`.          |

If your framework uses a config file to list extension hook methods, add the
BinRoute class alongside BeastInsights for the same lifecycle events.

## Config setup

Edit `config/extensions/BinRoute.php`:

```php
'enable'         => true,
'api_endpoint'   => 'https://binroute.cswebform.cloud', // prod
'client_id'      => '1',                                 // Kytsan
'client_secret'  => getenv('BINROUTE_API_KEY') ?: '<paste key here>',
'configurations' => [
    'your-checkout-config-id-here',
],
'enable_split_order' => false,
```

### Getting an API key

Ask the BinRoute team to run:
```bash
node scripts/issue-api-key.js 1 "kytsan-checkout"
```
The plaintext key is shown **once** and is bcrypt-hashed at rest on our side.
Store it via the same secrets mechanism used for Beast's `client_secret`.

### configurations whitelist

List the funnel `configId` values where the extension should fire. Mirror the
same IDs you have in `BeastInsights`'s `configurations` array — we want both
extensions active on the same checkout steps.

## Staging test plan

1. Deploy extension + config to staging.
2. Set `'enable' => true` and add your staging config ID to `configurations`.
3. Load a staging checkout page, open browser devtools Network tab.
4. Type `1444444444444440` (test card, BIN `144444`) into the card field.
5. **Verify these 3 things:**
   - Network: XHR to `extensions/binroute/bin-routing?_b=144444` returns
     `{success: 1, data: {shadow_id: "<uuid>", latency_ms: <ms>}}`.
   - Beast's XHR to `extensions/beastinsights/bin-routing` still fires
     alongside ours.
   - Complete the form and click Place Order. A beacon should hit
     `extensions/binroute/submit-timing` with the `shadow_id`.
6. Pull the test order in Sticky.io. The `customNotes` field should contain
   both markers:
   ```
   BeastInsights: gateway_id=190; group_name=... | BinRoute_shadow: id=<uuid> rec_gw=190 rec_proc=Paysafe conf=0.2376 reason=lookup_ai_hybrid lat=35
   ```
7. Tell the BinRoute team — next sync cycle will reconcile the shadow row and
   the decision loop is closed.

## Kill switch

Set `'enable' => false` in config. Instantly:
- No JS emitted (no keyup hook, no beacon)
- AJAX handlers short-circuit and return before making any network call
- `addShadowNote()` skips without touching `CrmPayload`

Beast Insights keeps running untouched.

## What NOT to change

- **`addShadowNote()` must never call `CrmPayload::set('forceGatewayId', …)`**
  That's the entire point of shadow mode. Removing that guard before Stage 1
  is formally approved will route live traffic without gate validation.
- **The `BinRoute_shadow: id=<uuid>` marker format is load-bearing.**
  BinRoute's server-side reconciler matches the regex
  `/BinRoute_shadow:\s*id=([a-fA-F0-9-]{8,})/` in `src/pipeline/post-sync.js`.
  If you rename, reformat, or localize this string, shadow rows will never
  reconcile and the pass/fail gates stay blocked forever.

## Open items (to confirm empirically on first staging order)

- **Which notes field receives customNotes from Sticky.io?** Our reconciler
  scans `employee_notes`, `system_notes`, AND `custom_fields` defensively so
  it works regardless, but on the first reconciled test order we want to see
  which column actually gets populated so we can drop the two we don't need.
- **Amount field.** Our API accepts `amount` as an optional float. The
  extension attempts to pull it from `Campaign::find()[0]['price']` or
  `['offerPrice']`. If your product schema exposes amount under a different
  key, add it in `BinRouting()` before the POST. (Null is fine — the model
  handles missing amount, just with slightly weaker features.)
- **Upsell support.** Stage 0 is INITIALS only. `addJavaScript()` and
  `BinRouting()` both skip upsell pages. When Stage 1b opens upsell routing,
  extend both methods mirroring Beast's upsell branch.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| JS doesn't fire on card input | `configId` not in `configurations` array |
| AJAX returns `{data: {error: "http_401"}}` | `client_secret` wrong — re-mint key |
| AJAX returns `{data: {error: "http_400"}}` | BIN wasn't 6 digits or client_id header mismatched |
| AJAX times out (2s) | BinRoute server down or network blocked; check with BinRoute team |
| Order imports but `shadow_decisions.reconciled_at` stays NULL | `customNotes` landed in an unexpected field — ping BinRoute team, we'll widen the reconciler regex target |
| `BinRoute_shadow: id=...` shows up on every order but Beast's marker does not | Verify Beast is still enabled and `addForceGateway` is still wired |
