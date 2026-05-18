<?php
/**
 * BinRoute — shadow-mode checkout extension.
 *
 * Mirrors the structure of Extension\BeastInsights\BeastInsights so it drops
 * into the same framework alongside Beast. Critical difference: in Stage 0
 * shadow mode this extension NEVER calls CrmPayload::set('forceGatewayId').
 * It only appends a `BinRoute_shadow: id=<uuid> ...` marker to customNotes,
 * which BinRoute's post-sync reconciler matches against the logged decision
 * to close the loop and compute lift vs. actual outcome.
 *
 * Three hooks:
 *   - addJavaScript()   — injects the BIN-keyup ajax + submit-timing beacon.
 *   - BinRouting()      — AJAX endpoint: calls BinRoute /api/route, stores
 *                         response in Session, returns shadow_id to the page.
 *   - SubmitTiming()    — AJAX endpoint: proxies the navigator.sendBeacon ping
 *                         to BinRoute /api/route/submit-timing with API-key auth.
 *   - addShadowNote()   — CrmPayload hook (the equivalent of Beast's
 *                         addForceGateway): appends the shadow marker to notes.
 *                         Does NOT set forceGatewayId — Beast still routes.
 *
 * Wire via framework's extension loader the same way Beast is wired.
 */

namespace Extension\BinRoute;

use Application\Config;
use Application\CrmPayload;
use Application\Model\Campaign;
use Application\Model\Configuration;
use Application\Request;
use Application\Response;
use Application\Session;

class BinRoute
{
    /** @var array<string, mixed> Extension config from config/extensions/BinRoute.php */
    private $config;

    public function __construct()
    {
        $this->config = Config::extensionsConfig( 'BinRoute' );
    }

    // -----------------------------------------------------------------------
    // Hook 1: addJavaScript — inject BIN-keyup ajax + submit-timing beacon
    // -----------------------------------------------------------------------
    public function addJavaScript()
    {
        if( !$this->_enabled() )
        {
            return;
        }

        $allowedConfigs = $this->config['configurations'] ?? [];

        $configId = Session::get( 'steps.current.configId' );
        $pageType = preg_replace( '/\\d+/', '', Session::get( 'steps.current.pageType' ) );

        if( !in_array( $configId, $allowedConfigs ) )
        {
            return;
        }

        // Stage 0 covers INITIALS only — skip upsell pages. When we expand to
        // upsell routing (Stage 1b+), mirror Beast's upsell branch here.
        if( $pageType !== 'checkoutPage' )
        {
            return;
        }

        $ajax_path   = Request::getOfferPath() . AJAX_PATH . 'extensions/binroute/bin-routing';
        $timing_path = Request::getOfferPath() . AJAX_PATH . 'extensions/binroute/submit-timing';

print <<<SCRIPT
    <script type="text/javascript">
        $(function(){
            var _br_bin = "";
            var _br_start = null;
            var _br_shadow = null;
            var _br_ready = false;

            // 1. On BIN entry, fire ajax to our PHP handler which hits BinRoute /api/route.
            $('[name="creditCardNumber"]').on('keyup change', function () {
                var val = ($(this).val() || "").replace(/\\D/g, '').substring(0, 6);
                if (val.length === 6 && val !== _br_bin) {
                    _br_bin = val;
                    _br_start = Date.now();
                    _br_ready = false;
                    $.get("$ajax_path", { _b: val }).done(function (r) {
                        _br_ready = true;
                        // BinRouting() returns { success, data: { shadow_id, gateway_id, ... } }.
                        // On API error, data may contain { error: ... } and no shadow_id.
                        if (r && r.data && r.data.shadow_id) {
                            _br_shadow = r.data.shadow_id;
                        } else {
                            _br_shadow = null;
                        }
                    });
                }
            });

            // 2. On form submit, ping the timing endpoint so we know whether our
            //    response arrived before the customer clicked Place Order.
            //    sendBeacon is fire-and-forget and survives page navigation.
            $(document).on('submit', 'form', function () {
                if (!_br_shadow) { return; }
                var lat = _br_start ? (Date.now() - _br_start) : null;
                try {
                    var body = JSON.stringify({
                        shadow_id: _br_shadow,
                        latency_ms_client: lat,
                        arrived_before_submit: _br_ready ? 1 : 0
                    });
                    // sendBeacon is the most reliable way to ping during unload.
                    if (navigator.sendBeacon) {
                        navigator.sendBeacon("$timing_path",
                            new Blob([body], { type: 'application/json' }));
                    } else {
                        $.ajax({ url: "$timing_path", method: 'POST',
                                 data: body, contentType: 'application/json',
                                 async: false });
                    }
                } catch (e) { /* swallow — never block submit */ }
            });
        });
    </script>
SCRIPT;
    }

    // -----------------------------------------------------------------------
    // Hook 2: BinRouting — AJAX endpoint hit from the BIN-keyup JS
    // -----------------------------------------------------------------------
    public function BinRouting()
    {
        if( !$this->_enabled() )
        {
            return;
        }

        $pageType = preg_replace( '/\\d+/', '', Session::get( 'steps.current.pageType' ) );

        // Stage 0: INITIALS only.
        if( $pageType !== 'checkoutPage' )
        {
            return;
        }

        $endpoint       = $this->config['api_endpoint']  ?? "";  // e.g. https://binroute.cswebform.cloud
        $client_id      = $this->config['client_id']     ?? "";  // numeric BinRoute client id, e.g. "1" for Kytsan
        $client_secret  = $this->config['client_secret'] ?? "";  // the x-api-key issued by scripts/issue-api-key.js
        $allowedConfigs = $this->config['configurations'] ?? [];

        $card_bin = $_GET["_b"] ?? "";
        $email    = Session::get( "customer.email" );

        if( empty( $endpoint ) || empty( $client_id ) || empty( $client_secret ) )
        {
            return;
        }
        if( !preg_match( '/^\\d{6}$/', $card_bin ) )
        {
            return;
        }

        $configId = Session::get( 'steps.current.configId' );
        if( !in_array( $configId, $allowedConfigs ) )
        {
            return;
        }

        // Pull product + amount from the campaign the same way Beast resolves
        // product id. Amount is optional in BinRoute's payload but improves
        // feature quality (amount_vs_bin_avg).
        $configuration = new Configuration( $configId );
        $campaignIds   = $configuration->getCampaignIds();
        $product       = !empty( $campaignIds ) ? Campaign::find( $campaignIds[0], true ) : null;

        $payload = [
            "bin"        => $card_bin,
            "sales_type" => "INITIALS",
        ];
        if( !empty( $email ) )
        {
            $payload['email'] = $email;
        }
        if( isset( $product[0]['productId'] ) )
        {
            $payload['product_id'] = (int) $product[0]['productId'];
        }
        if( isset( $product[0]['price'] ) )
        {
            $payload['amount'] = (float) $product[0]['price'];
        }
        elseif( isset( $product[0]['offerPrice'] ) )
        {
            $payload['amount'] = (float) $product[0]['offerPrice'];
        }

        $headers = [
            "Content-Type: application/json",
            "x-client-id: $client_id",
            "x-api-key: $client_secret",
        ];

        $url = rtrim( $endpoint, '/' ) . '/api/route';
        $data = $this->post( $url, $payload, $headers );

        // Store full response in session for addShadowNote to pick up.
        Session::set( "extensions.BinRoute", $data );

        // Return the shadow_id to the page so the submit-timing beacon can
        // reference it. Do not expose api keys, full feature snapshot, etc.
        $publicData = [];
        if( is_array( $data ) )
        {
            if( isset( $data['shadow_id'] ) )  $publicData['shadow_id']  = $data['shadow_id'];
            if( isset( $data['latency_ms'] ) ) $publicData['latency_ms'] = $data['latency_ms'];
            if( isset( $data['error'] ) )      $publicData['error']      = $data['error'];
        }
        Response::send([
            'success' => 1,
            'data'    => $publicData,
        ]);
    }

    // -----------------------------------------------------------------------
    // Hook 3: SubmitTiming — AJAX endpoint for the sendBeacon ping
    // -----------------------------------------------------------------------
    public function SubmitTiming()
    {
        if( !$this->_enabled() )
        {
            return;
        }

        $endpoint      = $this->config['api_endpoint']  ?? "";
        $client_id     = $this->config['client_id']     ?? "";
        $client_secret = $this->config['client_secret'] ?? "";

        if( empty( $endpoint ) || empty( $client_id ) || empty( $client_secret ) )
        {
            return;
        }

        // sendBeacon sends the Blob as the raw POST body. Read php://input
        // and forward as-is — we don't trust or reshape.
        $raw = file_get_contents( 'php://input' );
        if( empty( $raw ) )
        {
            return;
        }

        $parsed = json_decode( $raw, true );
        if( !is_array( $parsed ) || empty( $parsed['shadow_id'] ) )
        {
            return;
        }

        $headers = [
            "Content-Type: application/json",
            "x-client-id: $client_id",
            "x-api-key: $client_secret",
        ];

        $url = rtrim( $endpoint, '/' ) . '/api/route/submit-timing';
        // Fire-and-forget. We don't care about the response — sendBeacon
        // already returned to the browser.
        $this->post( $url, $parsed, $headers );

        // Return a minimal 200 even though sendBeacon ignores it.
        Response::send([ 'success' => 1 ]);
    }

    // -----------------------------------------------------------------------
    // Hook 4: addShadowNote — CrmPayload pre-submission hook
    //   Mirrors the signature of Beast's addForceGateway(), wired into the
    //   same extension pipeline. THIS IS THE CRITICAL FUNCTION for shadow
    //   mode: it appends the marker but never touches forceGatewayId.
    // -----------------------------------------------------------------------
    public function addShadowNote()
    {
        if( !$this->_enabled() )
        {
            return;
        }

        $allowedConfigs = $this->config['configurations'] ?? [];
        $allowSplit     = !empty( $this->config['enable_split_order'] );

        $configId = CrmPayload::get( 'meta.configId' );
        if( !in_array( $configId, $allowedConfigs ) )
        {
            return;
        }
        if( !$allowSplit && CrmPayload::get( 'meta.isSplitOrder' ) == true )
        {
            return;
        }

        // If BinRoute API failed, append a diagnostic note (same pattern Beast
        // uses for its errors) and return without a shadow_id — the decision
        // never went out, so there's nothing to reconcile.
        if( Session::has( "extensions.BinRoute.error" ) )
        {
            $error = (string) Session::get( "extensions.BinRoute.error", "" );
            $notes = CrmPayload::has( "customNotes" )
                     ? sprintf( "%s | ", CrmPayload::get( "customNotes" ) ) : "";
            $notes .= "BinRoute: $error";
            CrmPayload::set( "customNotes", $notes );
            return;
        }

        $shadowId  = (string) Session::get( "extensions.BinRoute.shadow_id", "" );
        $gatewayId = (int)    Session::get( "extensions.BinRoute.gateway_id", 0 );
        $processor = (string) Session::get( "extensions.BinRoute.processor", "" );
        $confidenceRaw = Session::get( "extensions.BinRoute.confidence" );
        $confidence = is_numeric( $confidenceRaw ) ? (float) $confidenceRaw : null;
        $reason    = (string) Session::get( "extensions.BinRoute.reason", "" );
        $latencyMs = (int)    Session::get( "extensions.BinRoute.latency_ms", 0 );
        $expId     = Session::get( "extensions.BinRoute.experiment_id", "" );
        $variant   = (string) Session::get( "extensions.BinRoute.experiment_variant", "none" );
        $selBy     = (string) Session::get( "extensions.BinRoute.selected_by", "beast" );
        $wouldForce = Session::get( "extensions.BinRoute.would_force_gateway" ) ? 1 : 0;
        $forceReq   = Session::get( "extensions.BinRoute.force_gateway" ) ? 1 : 0;

        if( empty( $shadowId ) )
        {
            return;
        }

        // Compose the canonical marker. Reconciler regex in
        // src/pipeline/post-sync.js matches:  /BinRoute_shadow:\s*id=([a-fA-F0-9-]{8,})/
        // Keep the "id=<uuid>" portion verbatim — other fields are for
        // human readability only.
        $marker = sprintf(
            'BinRoute_shadow: id=%s rec_gw=%d rec_proc=%s conf=%s reason=%s lat=%d exp=%s var=%s selected_by=%s would_force=%d force_requested=%d',
            $shadowId,
            $gatewayId,
            $processor,
            $confidence !== null ? number_format( $confidence, 4, '.', '' ) : 'na',
            $reason !== '' ? $reason : 'na',
            $latencyMs,
            $expId !== '' ? $expId : 'na',
            $variant,
            $selBy,
            $wouldForce,
            $forceReq
        );

        $notes = CrmPayload::has( "customNotes" )
                 ? sprintf( "%s | ", CrmPayload::get( "customNotes" ) ) : "";
        $notes .= $marker;
        CrmPayload::set( "customNotes", $notes );

        // --- Release 2: Conditional forceGatewayId ---
        // When LIVE_FORCE_GATEWAY_ENABLED is set on the server and the API
        // returns force_gateway=true (treatment group), override Beast's routing.
        // Release 1: force_gateway is always false from the API, so this never fires.
        $forceGateway = Session::get( "extensions.BinRoute.force_gateway" );
        if( ($forceGateway === true || $forceGateway === 1 || $forceGateway === '1')
            && $variant === 'treatment'
            && $gatewayId > 0 )
        {
            CrmPayload::set( 'forceGatewayId', $gatewayId );
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Global kill switch — single config flag that disables every hook. */
    private function _enabled()
    {
        return !empty( $this->config['enable'] );
    }

    /** POST JSON, return decoded response array or ['error' => '...']. */
    private function post( $url, $params, $headers )
    {
        try
        {
            $ch = curl_init( $url );
            curl_setopt( $ch, CURLOPT_RETURNTRANSFER, true );
            curl_setopt( $ch, CURLOPT_POST,           true );
            curl_setopt( $ch, CURLOPT_POSTFIELDS,     json_encode( $params ) );
            curl_setopt( $ch, CURLOPT_HTTPHEADER,     $headers );
            // Hard caps so a network hiccup can't wedge the checkout page.
            curl_setopt( $ch, CURLOPT_CONNECTTIMEOUT_MS, 500 );
            curl_setopt( $ch, CURLOPT_TIMEOUT_MS,        2000 );

            $response = curl_exec( $ch );
            $errno    = curl_errno( $ch );
            $errmsg   = curl_error( $ch );
            $httpCode = curl_getinfo( $ch, CURLINFO_HTTP_CODE );
            curl_close( $ch );

            if( $errno )
            {
                return [ 'error' => $errmsg ];
            }
            if( $httpCode < 200 || $httpCode >= 300 )
            {
                return [ 'error' => "http_$httpCode" ];
            }

            $decoded = json_decode( $response, true );
            if( !is_array( $decoded ) )
            {
                return [ 'error' => 'bad_response' ];
            }
            return $decoded;
        }
        catch ( \Exception $th )
        {
            return [ 'error' => $th->getMessage() ];
        }
    }
}
