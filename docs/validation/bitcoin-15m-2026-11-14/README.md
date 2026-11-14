# Live Bitcoin pair discovery follow-up

Checked 2026-11-14, around 02:13 UTC. This follow-up corrects the practical live-pair evidence gap in the November 13 investigation. It does not establish a contractual guarantee, a historical backtest, or executable net profit.

## Observed pair

- Kalshi: `KXBTC15M-26NOV132115-15`.
- Polymarket US: ID `2494627`, slug `cpc-btc-updown-15m-2026-11-14-0200z`.
- Measurement interval: 2026-11-14 02:00–02:15 UTC on both venues.
- Opening threshold: USD 68209.62 on both venues.
- Both markets were open/active and both public book endpoints returned nonempty depth with HTTP 200.

Discovery used the official Kalshi `series_ticker=KXBTC15M` market filter and the US event filter `seriesId=375&closed=false`, then verified typed window boundaries and exact market metadata. Thus, matching live production markets are discoverable through the public APIs. The previous missing result at one observation time must not be generalized into a platform limitation or a reason the strategy cannot be researched.

## Snapshot limitations

Saved book responses arrived approximately 0.106 seconds apart at 02:13:29 UTC. The US book's `transactTime` was 02:13:03.155077523 UTC, approximately 26 seconds before receipt. This may represent the last book change, but REST alone does not establish gap-free feed freshness or continued executable availability. The Kalshi response does not supply a comparable exchange event timestamp. Do not confuse nearby local receipt times with synchronized source observations.

`quote-diagnostic.json` computes top-of-book costs in both orientations solely to preserve the observed arithmetic. It excludes fees, rounding, latency, configured profit buffers, and settlement risk. It is not an admitted opportunity or evidence that both orders would fill. No orders, including simulated orders, were submitted.

## Shared reference source

The existing sampled contracts identify CF Benchmarks BRTI and matching normal minute-average mechanics. That supports normal settlement alignment. CF Benchmarks is an index provider; the two exchanges still apply their own contract terms to unavailable or corrected values. The prior report records those specific provisions. No actual settlement disagreement has been observed in these checks, and its probability has not been estimated. An unconditional payout guarantee remains unestablished; that is distinct from proving the strategy unprofitable.

## Historical backtesting feasibility

Public historical prices, candlesticks, and trades can support screening and settlement comparisons, but cannot uniquely reconstruct the full order book. Orders can be added and canceled without trading. Full historical replay therefore needs recorded depth snapshots plus subsequent ordered depth changes, or an official archive that supplies equivalent information. Private account order/trade history covers that account and does not reveal the whole exchange book.

The official [Kalshi historical-data reference](https://docs.kalshi.com/getting_started/historical_data) lists archived markets, candles, trades, and user order/fill history; it does not document a full historical depth-reconstruction endpoint. The [US price-history reference](https://docs.polymarket.us/api-reference/price-history/get-price-history.md) supplies sampled book-derived YES/NO display prices without full depth. The [US reporting guide](https://docs.polymarket.us/trader-guide/reporting) scopes trade searches to authorized accounts. No complete exchange-wide historical depth archive was identified through these references; availability through another licensed source remains an open question.

If only sampled quotes are available, label the result a preliminary quote-based screen with explicit depth/fill limitations. Never synthesize unobserved sizes, canceled orders, or guaranteed fills and call it a reconstructed-book backtest. Fresh paired-depth capture provides a defensible source for subsequent replay.

## Authentication

Public discovery and these book reads required no credentials. Account-specific access and authenticated streams remain untested. The user reports that private endpoints worked before significant repository changes; this is useful prior evidence, but it is not a current regression test. Accept credentials only through the secure secret prompt, keep any delivered files outside the repository, and use them only for read-only checks unless separately authorized. No credential was received or recorded during this follow-up.

## Evidence

Each market/book JSON record includes the official URL, local request/receipt timestamps, HTTP status, selected headers, response-body digest, and decoded public response. `evidence-manifest.json` hashes the saved records and the quote diagnostic. No credentials or private account data are present.
