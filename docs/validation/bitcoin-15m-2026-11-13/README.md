# Bitcoin 15-minute pair validation

Date: 2026-11-13. Method: read-only official production REST requests and official exchange documents. No order was placed, no account was accessed, and no authenticated WebSocket session was tested.

## Verdict

Follow-up: [November 14 live-pair check](../bitcoin-15m-2026-11-14/README.md) found a matching open pair and captured nonempty public books on both venues. The missing market and empty depth below describe only this November 13 observation; they are not a continuing market-availability failure. No settlement disagreement was observed. The contractual guarantee question remains distinct from these practical discovery results.

**NOT ELIGIBLE under the approved spec's unconditional combined-payout requirement.** Normal settlement terms and the realized values match for the sampled completed pair. Exceptional source-error, missing-data, and review provisions do not establish a guaranteed $1 combined payout across the two venues. This is a failure to establish eligibility, not evidence that this sampled pair actually settled inconsistently.

**Executable paired depth: NOT VERIFIED.** The completed pair has empty books at inspection, and no matching Polymarket US Bitcoin window was returned for the then-open Kalshi window. No simultaneous executable pair, historical arbitrage spread, fill probability, or positive net return is established.

**Public API availability: VERIFIED for the sampled read endpoints.** Account-specific fees, authenticated streaming access, and empirical execution latency remain unverified.

## Exact pair inspected

The live discovery attempt began around 06:23 UTC. With no current matching US Bitcoin window returned, the latest preceding completed window was selected from official series/event metadata for contractual and realized-settlement inspection. This substitution does not establish live executable liquidity.

| Field | Kalshi | Polymarket US |
| --- | --- | --- |
| Exact identity | `KXBTC15M-26NOV130100-00` | ID `2464553`, slug `cpc-btc-updown-15m-2026-11-13-0545z` |
| Window start UTC | 2026-11-13 05:45:00 | 2026-11-13 05:45:00 |
| Window end UTC | 2026-11-13 06:00:00 | 2026-11-13 06:00:00 |
| Reference | CF Benchmarks BRTI | CF Benchmarks BRTI |
| Normal observation method | Mean of preceding 60 seconds | Mean of preceding 60 seconds at each boundary |
| Opening threshold USD | 68545.77 | 68545.77 |
| Closing value USD | 68070.53 | 68070.53 |
| YES / Up condition | Close greater than or equal to open | Close greater than or equal to open |
| Observed YES / long settlement | 0, finalized No | 0, resolved Down |
| Inspected depth after close | Empty YES/NO bid arrays | Empty bids/offers; expired state |

The sampled normal outcome is consistent with a $1 combined payoff for either opposing orientation. It does not prove identical behavior during source failure, corrections, review, or future windows. See [Kalshi market](https://external-api.kalshi.com/trade-api/v2/markets/KXBTC15M-26NOV130100-00), [US market](https://gateway.polymarket.us/v1/market/id/2464553), and the saved response records.

The US market's `endDate` is 06:00:14 UTC; its typed `assetPriceTerms.windowEnd` and description specify 06:00:00 UTC. Kalshi's expected expiration is later still. Match the measurement window using typed terms and actual rules, not creation time, administrative expiry, or slug parsing. This is directly relevant to rolling discovery.

## Settlement findings

The [Kalshi series response](https://external-api.kalshi.com/trade-api/v2/series/KXBTC15M) links **CRYPTO.pdf**, not the similarly named CRYPTO15M.pdf used as a preliminary reference in the draft. Use the actual series-linked contract and market-specific rules.

Kalshi's series-linked contract excludes source revisions made after expiration and provides a No result when source data is absent or incomplete at its contractual expiration, subject to review provisions. The actual market supplies the at-least comparison and two-decimal rounding. Source: [CRYPTO contract](https://assets.kalshi.com/contract_terms/CRYPTO.pdf), saved as `kalshi-contract.pdf`.

Polymarket US's official product listing identifies CPCBTC. The September 15 filing and October 20 amendment include provisions for postponing settlement for corrected erroneous values, first qualifying values when data is unavailable, and exchange review or changes to sources/terms. The October amendment clarifies rounding after calculation. Sources: [product listing](https://www.polymarketexchange.com/files/products), [notice listing](https://www.polymarketexchange.com/files/notices), [CPCBTC filing](https://www.polymarketexchange.com/files/products/PMUS%20-%20CPCBTC%20-%20%282026.09.15%29.pdf), and [October amendment](https://www.polymarketexchange.com/files/notices/Exchange%20Notice%20-%20CPCBTC%20Amendment%20%282026.10.20%29.pdf). Both PDFs were downloaded directly with HTTP 200; the web extraction tool could not fetch them, so local PDF text extraction was used. The first four pages of each contain the clean filing; later pages contain redlines. Local text extracts preserve those first four pages.

These provisions do not supply a shared exceptional-state settlement function or a common finality time. Different timing and correction treatment can permit different outcomes; no probability of such disagreement has been estimated. Treat a hypothetical disagreement as a stress scenario, not an observed loss or a measured frequency. Neither orientation is certified to have a $1 minimum payoff across all relevant states.

An older generic March CPC filing surfaced during research but is not the controlling reference used here. Shared BRTI branding, matching ordinary values, or a high historical agreement rate cannot substitute for the missing contractual lower bound.

## Discovery and API availability

Verified read endpoints returned HTTP 200 without credentials:

- Kalshi series, market list, exact market, and order-book endpoints at `https://external-api.kalshi.com/trade-api/v2`.
- Polymarket US production series, event list, exact market ID, and retired-market book endpoints at `https://gateway.polymarket.us`.

The official [US Get Markets](https://docs.polymarket.us/api-reference/markets/get-markets) and [Get Market Book](https://docs.polymarket.us/api-reference/markets/get-market-book) references identify the gateway as the public-data host. An unauthenticated request to `https://api.polymarket.us/v1/markets` returned 401; that is not evidence the official public gateway is unavailable. An initial ID request incorrectly used `/v1/markets/{id}` and returned 404; the documented `/v1/market/id/{id}` returned 200. The failed probe is not counted as a venue outage.

The discovered US series has ID `375`, slug `btc-updown-15m`. A current-only event query returned no events. A date-bounded query returned 68 events for November 13, including a gap between the 05:45 and 07:00 starts. Do not interpret `closed=true` alone as settled: the 07:00 future window was also closed, had no opening threshold, and its book endpoint returned 404 before activation.

Kalshi had an active 06:15–06:30 window, `KXBTC15M-26NOV130130-30`, with nonempty public depth. No corresponding US Bitcoin window was found in the date-bounded series response. The cause of the observed gap is unknown. It is not established to be the published weekly maintenance window. Do not silently substitute an Ethereum market, a different duration, or an adjacent Bitcoin window.

Market list ordering/filter behavior also needs care: the attempted `orderBy=startDate&orderDirection=desc` response still began with old records, and a guessed `marketTypes=updown` filter returned none. Those probes are not absence proofs. The date-bounded official series/event response, typed asset-price terms, and exact-market reads are the evidence used here.

Public REST availability does not establish working authenticated streams or compatibility with the pinned SDK in the installed bot environment. The [US markets WebSocket](https://docs.polymarket.us/api-reference/websocket/markets) requires handshake authentication. No expected venue credentials or local `.env` were present in this worktree; credential values were neither read nor recorded. No secrets are needed to inspect these public REST records.

## Depth and execution limitations

The selected completed pair's books returned HTTP 200 with empty depth. Its historical trade statistics prove past activity but cannot reconstruct an executable paired quote or pre-close quantities. The separate active Kalshi snapshot proves only one venue's depth at that instant. Requests were not synchronized, and a REST snapshot does not guarantee a fill after routing delay.

Before admitting any public paper orders, capture a currently active verified pair with timestamped full depth on both venues. Derive Kalshi YES asks from NO bids and NO asks from YES bids. On US single-instrument books, derive NO asks from complementary YES bids. Check source timestamps, snapshot age, observation skew, quantities, and entry instructions. Do not treat displayed outcome prices or post-settlement placeholder quotes as executable asks.

## Fees and quantity precision

The sampled Kalshi series reports `fee_type=quadratic` and `fee_multiplier=1`. The published general taker model is `0.07 * quantity * price * (1 - price)` before the venue's precision/rounding treatment. Official series metadata and the [fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf) support that public schedule. Direct PDF archival received HTTP 429 and was not retried aggressively; the schedule was independently retrieved through the web tool. Do not infer custom broker/account arrangements from the series rate.

The sampled US market reports `feeCoefficient=0.095`. The [current US fee schedule](https://docs.polymarket.us/fees) uses account tiers and onboarding overrides; standard fees follow `theta * quantity * price * (1 - price)` with per-order/per-fill rounding rules. Use 0.095 only as a conservative published-profile assumption for this research, not as verified account entitlement. Ignore unearned rebates. Saved fee documentation records effective dates and tier conditions.

US metadata supplies `minimumTradeQty=0.01` and a 0.01 price tick for the selected market. Kalshi publishes a tapered price grid, including 0.001 ticks near the extremes and 0.01 ticks in the middle. A minimum quantity is not by itself proof of the complete quantity increment contract. Confirm account quantity precision, fee balance precision, price ranges, and FOK/IOC support before activation. Account-specific fee and quantity calibration has not passed.

No paired cost or net spread is calculated because there is no admissible simultaneous depth snapshot for this pair. Fee formulas alone cannot certify a profitable opportunity.

## Required consequences for the spec

- Preserve the strict eligibility requirement; classify the inspected cross-exchange pair as not validated and keep public entry disabled.
- Permit recording, rolling discovery, and deterministic development fixtures without claiming contractual equivalence or economic profitability.
- Resolve the exceptional-state payout bound through authoritative clarification or genuinely compatible terms before order entry. Relaxing the guarantee would create settlement-mismatch exposure and requires an explicit strategy/spec decision.
- Require a fresh active two-venue depth capture and account/profile verification before declaring the public paper execution gate passed.
- Cover the observed missing-window gap, closed future windows, distinct administrative and measurement times, and actual series-linked rule documents in discovery tests.

## Evidence integrity and scope

API evidence files contain request URLs, local UTC request/receipt times, HTTP status, selected response headers, decoded JSON bodies, and the original response-body SHA-256 when available. They are research snapshots, not a complete bot capture or a byte-for-byte raw HTTP archive. `evidence-manifest.json` hashes the saved evidence files themselves. Public metadata only; no authentication headers, account responses, or credentials are stored.

PDF source records contain their official URL, retrieval time, status, and file hash. Saved `.txt` files are convenience extracts; the original PDFs are authoritative. Documentation snapshots are supporting references, not proof of future schedules. Check the manifest and timestamps before reusing these observations.

This report establishes an actionable no-go at the validation gate. It does not establish that all future pairs are incompatible, that the strategy can never work, or that missing live-depth/account evidence has been completed.
