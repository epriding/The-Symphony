> ## Documentation Index
> Fetch the complete documentation index at: https://docs.polymarket.us/llms.txt
> Use this file to discover all available pages before exploring further.

# Crypto FAQs

> Frequently asked questions about trading Cryptocurrency Contracts on Polymarket US

## General Rules

### What kinds of Cryptocurrency Contracts are offered?

Polymarket US offers Event Contracts that settle on the published value of a cryptocurrency reference index. Four automated market families are listed on a recurring schedule:

| Family | Question | Cadence |
| - | - | - |
| Up/Down | Will the Bitcoin price close the window at or above its open? | 15-minute and 60-minute windows, around the clock |
| Above/Below | Will the Bitcoin price be at or above \$X at expiry? | Hourly, daily (5:00 PM ET) and weekly (Friday 5:00 PM ET) ladders |
| Price Range | Will the Bitcoin price be between \$A and \$B at expiry? | Same expiries as Above/Below |
| One-Touch | Will the Bitcoin price reach (or dip to) \$X before the end of the month? | Monthly |

The exchange also lists longer-dated Bitcoin markets by hand, such as the year-end price range and "How high will Bitcoin get this year?" ladders.

> [!NOTE]
>
> The automated families are currently listed in the preprod environment and roll out to production as announced in the [changelog](https://docs.polymarket.us/changelog.md). For the fields that identify each market, see [Crypto Schema](https://docs.polymarket.us/trader-guide/crypto-schema.md).

### How are Cryptocurrency Contracts settled?

Every market settles on CF Benchmarks' [Bitcoin Real-Time Index (BRTI)](https://www.cfbenchmarks.com/data/indices/BRTI), a regulated benchmark published once per second. Prices on spot exchanges or other indices have no effect on settlement.

The statistic depends on the market family:

* **Up/Down, Above/Below and Price Range** use a simple average: the 60 BRTI prices published in the sixty seconds before the settlement instant, from `T − 60 s` up to but not including the instant itself (the interval `[T − 60 s, T)`), are collected and averaged, rounded to the nearest two decimal places. An Up/Down market computes this value twice, once at the window open and once at the close.
* **One-Touch** uses a rolling 60-second 20% trimmed mean: at each evaluation instant the 60 BRTI prices published in the sixty seconds ending at and including that instant are collected, the highest 20% and lowest 20% are removed, and the remaining values are averaged, rounded to two decimals. The statistic is evaluated continuously through the window.
* **Hand-listed markets** state their own statistic in the market rules. Always read the rules of the specific market.

### How do ties work?

* An Up/Down market whose close value equals its open value settles **Up**.
* An Above/Below market whose settlement value equals the strike settles **Yes**.
* Price Range buckets are inclusive on both bounds. Buckets tile the price line in one-cent steps (for example \$74,000.00 to \$74,499.99, then \$74,500.00 to \$74,999.99), so exactly one bucket settles Yes.
* A One-Touch market whose trimmed mean equals the target settles **Yes**.

### When does settlement occur?

Up/Down windows settle shortly after the window closes, typically within a minute. Above/Below and Price Range ladders settle within about fifteen minutes of expiry. One-Touch markets can settle Yes at any moment the target is touched; a No is confirmed shortly after the window closes.

The instrument's expiration is set 30 minutes after the settlement instant to leave time for resolution. Trading ends at the settlement instant regardless.

### Can a market settle early?

Only One-Touch markets. When the trimmed mean reaches the target, the market settles Yes at that time and trading stops. Up/Down, Above/Below and Price Range markets always settle at their stated instant.

### What happens if the index data is incomplete?

Settlement requires the complete set of index prices for the settlement minute (or, for One-Touch, gap-free data across the window before a No can be confirmed). If prices are missing, settlement is deferred until the data is complete or the exchange reviews the market. No market settles on partial data.

### Do Cryptocurrency Contracts trade around the clock?

Yes. Windows and ladders are listed continuously, including weekends. The exchange's trade-day roll at 5:00 PM ET applies as on every other market, and no ladder is created to expire during that hour; daily and weekly ladders instead expire exactly at 5:00 PM ET. See [Trading Hours](https://docs.polymarket.us/learn/trading/access-and-limits/trading-hours.md) for the recurring maintenance window.

### What happens during scheduled maintenance?

Up/Down windows that would overlap a scheduled exchange maintenance are not listed. Ladders whose expiry falls outside the maintenance window are unaffected and settle normally once the exchange is back.

## Market Families

### How do Up/Down markets work?

Each window is its own market. The open value is fixed a few seconds after the window starts and is published as the "price to beat" (`price_to_beat` on the Institutional API, `assetPriceTerms.priceToBeat` on the Retail API). At the close, the close value is computed the same way. The long side of the contract pays \$1.00 when the close is greater than or equal to the open; otherwise the short side pays.

15-minute windows are listed about 12 hours ahead and 60-minute windows about 24 hours ahead, so roughly 48 fifteen-minute and 24 hourly windows are open at any time. Each window opens for trading about 90 seconds before it starts.

### How do Above/Below ladders work?

A ladder is one event with one market per strike. Strikes are spaced \$100 apart on hourly ladders and \$500 apart on daily and weekly ladders, and the ladder is sized to cover the expected range of prices. Every strike settles independently: any strike at or below the settlement value settles Yes, every strike above it settles No.

Hourly ladders are listed about four hours before expiry; the daily ladder is listed about 25 hours ahead and expires at 5:00 PM ET; the weekly ladder is listed on Monday at midnight ET and expires Friday 5:00 PM ET. There is no hourly ladder expiring at 5:00 PM ET and no daily ladder on Fridays, because the daily and weekly ladders own those expiries.

### How do Price Range ladders work?

A price range ladder shares its expiry and grid with the Above/Below ladder of the same horizon: the bucket width equals that ladder's strike spacing. The lowest bucket is "or below", the highest is "or above", and the buckets in between are inclusive ranges one cent apart. Exactly one bucket settles Yes, so the event is listed as mutually exclusive; see [Mutually Exclusive Collateral Return](https://docs.polymarket.us/market-structure/mutually-exclusive-collateral-return.md) for how that reduces margin on offsetting short positions.

### How do One-Touch markets work?

Each month has two events: "Reaches \$X" targets above the price at listing and "Dips to \$X" targets below it, spaced \$2,500 apart. A market settles Yes the moment the rolling trimmed mean is at or above (or at or below) its target, and No at midnight ET on the last day of the month if it never was. Only index values inside the market's own window count.

## Finding the Markets

### How do I find crypto markets on the Retail API?

Filter by category: `GET /v1/markets?categories=crypto&closed=false`. Automated markets carry a typed `assetPriceTerms` object with the market type, window, strike or range bounds and, once known, the price to beat and the settlement price. Hand-listed markets show `assetPriceTerms: null` and `marketType: "futures"`. See [Asset Price Market Fields](https://docs.polymarket.us/api-reference/market/overview.md#asset-price-market-fields). Index price history is available from `GET /v1/asset-prices/history`; see [Asset Prices](https://docs.polymarket.us/api-reference/asset-prices/overview.md).

### How do I find crypto instruments on the Institutional API?

Filter reference data by category or clearing symbol: `POST /v1/refdata/instruments` with `{"eventCategory": "CRY"}` or `{"clearingSym": "CPC-BTC"}`, or by series such as `{"eventSeries": "btc-updown-15m"}`. Identify the market from `crypto_market_type` and the other fields documented in [Crypto Schema](https://docs.polymarket.us/trader-guide/crypto-schema.md). Never parse the symbol.

The reference data list is served from a cache, so a newly stamped `price_to_beat` or `crypto_settlement_price` can take up to an hour to appear there. Read fresh prices from the Retail market endpoints, and the final settlement from `GET /v1/instruments/{symbol}/settlement`.


This documentation is built and hosted on [Mintlify](https://mintlify.com), a developer documentation platform.
