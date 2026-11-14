> ## Documentation Index
> Fetch the complete documentation index at: https://docs.polymarket.us/llms.txt
> Use this file to discover all available pages before exploring further.

# Fee Schedule

> Trading fee schedule, rebates, and examples

> [!NOTE]
>
> Effective exchange-wide from 1:00 PM ET, Wednesday, November 11, 2026.
>
> **Combo maker fees remain zero until November 14, 2026 at 12:00 AM ET.** See [Combo maker fees](#maker-fees) for the schedule from that time.

## Taker fee tiers

Your taker fee tier is calculated daily using taker volume over the preceding **30 completed Eastern Time (ET) calendar days**, excluding today. Volume is measured as described in [How volume is measured](#how-volume-is-measured).

| Taker fee tier | Trailing 30-day taker volume | Taker coefficient (Θ) |
| - | - | - |
| Tier 1 | Below \$10,000 | 0.095 |
| Tier 2 | \$10,000 to less than \$50,000 | 0.085 |
| Tier 3 | \$50,000 to less than \$250,000 | 0.075 |
| Tier 4 | \$250,000 or more | 0.0695 |

This tier sets Θ in both the [standard taker fee formula](#standard-trading), including Table Tennis, and the [combo taker fee formula](#taker-fees). Combos use a separate fee curve.

### New-account onboarding window

The window applies to retail and institutional accounts across trading channels whose first fill is on or after launch:

* **Day 1** is the ET calendar day of the account's first filled order, maker or taker. There is no hourly proration.
* **Days 1–7:** taker fee coefficient `0.0695`, regardless of volume. This boost applies to taker fees only; maker and taker rebate tiers use trailing 30-day volume.
* **Days 8–30:** taker fee, maker rebate and taker rebate tiers use the higher of first-7-day volume × `30/7` and trailing 30-day volume. Maker volume sets the maker rebate tier; taker volume sets the taker fee and taker rebate tiers.
* **Day 31 onward:** all three tiers use their respective trailing 30-day volume.
* Accounts with their first fill in the six days before launch receive the remaining days of their window. During November, existing accounts receive the better of their trailing 30-day tier or the tier based on October volume.
* Manual overrides take precedence over the boost and acceleration.

The fee coefficient is set when an order is inserted. An order placed during the window keeps that coefficient if it fills later.

***

## Standard Trading

For combo fees, see [Combo Trading](#combo-trading).

Standard taker fees and maker rebates are computed using a symmetric formula that scales with price uncertainty:

```
Fee = Θ × C × p × (1 - p)
```

Where:

* **C** is the number of contracts
* **p** is the trade price (\$0.01 to \$0.99)
* **Θ** (theta) is the fee coefficient

| | Theta | Max (100-lot at p = \$0.50) |
| - | - | - |
| **Taker Fee (Tier 4)** | 0.0695 | \$1.74 |
| **Maker Rebate** | -0.0035 to -0.0104, by [maker tier](#maker-rebate-tiers) | -\$0.09 to -\$0.26 |

The taker coefficient depends on your [taker fee tier](#taker-fee-tiers). The schedule by price and standard taker fee examples below use the Tier 4 coefficient of `0.0695`. Maker rebates use the same tier coefficient in every market, including Table Tennis and crypto.

### Rebate Tiers

#### Maker rebate tiers

Maker rebates apply to standard trades and are credited at the fill. Combo fills carry no maker rebate. Your maker rebate tier is set by your qualifying maker volume. See [How tiers are set](#how-tiers-are-set) for the calculation window.

| Maker tier | Qualifying maker volume | Maker rebate coefficient (Θ) | Share of the standard taker fee |
| - | - | - | - |
| Tier 1 | Below \$10,000,000 | No maker rebate | 0% |
| Tier 2 | \$10,000,000 - \$49,999,999 | -0.0035 | about 5% |
| Tier 3 | \$50,000,000 - \$199,999,999 | -0.0070 | about 10% |
| Tier 4 | \$200,000,000+ | -0.0104 | about 15% |

#### Taker rebate tiers

Taker rebates are a percentage of the taker fees you paid, including combo taker fees, and are paid weekly. Your taker rebate tier is set by your qualifying taker volume. See [How tiers are set](#how-tiers-are-set) for the calculation window.

| Taker tier | Qualifying taker volume | % Taker fee rebate |
| - | - | - |
| Tier 1 | Below \$250,000 | No taker rebate |
| Tier 2 | \$250,000 - \$999,999 | 3% |
| Tier 3 | \$1,000,000 - \$9,999,999 | 5% |
| Tier 4 | \$10,000,000 - \$24,999,999 | 10% |
| Tier 5 | \$25,000,000 - \$49,999,999 | 15% |
| Tier 6 | \$50,000,000+ | 30% |

#### How tiers are set

* Rebate tiers use volume over the preceding 30 completed calendar days in Eastern Time (ET), excluding the current day. Tiers are set daily at 12:00 AM ET and apply until the next midnight.
* Maker rebate and taker rebate tiers are numbered and set independently. Your maker rebate tier (Tier 1 to Tier 4) uses maker volume only and your taker rebate tier (Tier 1 to Tier 6) uses taker volume only.
* All complementary programs, including accelerations and overrides, continue to apply. Taker rebates are paid weekly.
* **Accelerated Tier Placement**: A Participant may provide verifiable proof of their trailing-30-day notional trading volume on another prediction market and be assigned to the rebate tier corresponding to that volume.

For example, qualifying taker volume of \$9.8 million for Monday gives a 5% rebate on Monday's taker fees. Qualifying taker volume of \$10.2 million for Tuesday gives a 10% rebate on Tuesday's taker fees. Each day uses its own tier; rebates are still paid weekly.

#### How volume is measured

Dollar volume is the amount you paid for the position you received, not the face value of the contracts. For each fill, volume is the number of contracts multiplied by the price of the side you received.

```
Volume (buy)  = C × p
Volume (sell) = C × (1 - p)
```

| Fill | Volume | Face value (not used) |
| - | - | - |
| Buy 1,000 contracts at \$0.10 | \$100 | \$1,000 |
| Sell 1,000 contracts at \$0.90 | \$100 | \$1,000 |
| Sell 1,000 contracts at \$0.30 | \$700 | \$1,000 |

* **Taker volume** is your dollar volume on fills where your order removed liquidity from the book.
* **Maker volume** is your dollar volume on fills where your resting order was executed against.
* Combo fills count toward both maker and taker volume.
* Volume is measured in Eastern Time (ET), using the calculation window in [How tiers are set](#how-tiers-are-set). Voided fills are excluded.

> [!NOTE]
>
> **API integrators**
>
> Use decoded values in the fee formulas:
>
> * `C`: the number of contracts.
> * `p`: the execution price in decimal dollars.
>
> Execution reports encode prices, quantities and collected fees as scaled integers. The relevant fields are:
>
> * `price_scale` and `fractional_quantity_scale`: the price and quantity scaling factors.
> * `commission_notional_collected`: the collected fee in scaled notional units.
>
> See [Fees on execution reports](https://docs.polymarket.us/partners/orders/data-model.md#fees-on-execution-reports) for decoding instructions.

### Standard Fee Schedule by Price

This table shows a 100-contract example using **Taker Tier 4** (`0.0695`) and **Maker Tier 4** (`-0.0104`). Other tiers use their respective [taker fee](#taker-fee-tiers) and [maker rebate](#maker-rebate-tiers) coefficients. Maker Tier 1 receives no rebate.

| Price | Trade Value (100-lot) | Taker Pays (Tier 4) | Maker Receives (Tier 4) |
| - | - | - | - |
| \$0.01 | \$1 | \$0.07 | \$0.01 |
| \$0.02 | \$2 | \$0.14 | \$0.02 |
| \$0.03 | \$3 | \$0.20 | \$0.03 |
| \$0.04 | \$4 | \$0.27 | \$0.04 |
| \$0.05 | \$5 | \$0.33 | \$0.05 |
| \$0.06 | \$6 | \$0.39 | \$0.06 |
| \$0.07 | \$7 | \$0.45 | \$0.07 |
| \$0.08 | \$8 | \$0.51 | \$0.08 |
| \$0.09 | \$9 | \$0.57 | \$0.09 |
| \$0.1 | \$10 | \$0.63 | \$0.09 |
| \$0.11 | \$11 | \$0.68 | \$0.10 |
| \$0.12 | \$12 | \$0.73 | \$0.11 |
| \$0.13 | \$13 | \$0.79 | \$0.12 |
| \$0.14 | \$14 | \$0.84 | \$0.13 |
| \$0.15 | \$15 | \$0.89 | \$0.13 |
| \$0.16 | \$16 | \$0.93 | \$0.14 |
| \$0.17 | \$17 | \$0.98 | \$0.15 |
| \$0.18 | \$18 | \$1.03 | \$0.15 |
| \$0.19 | \$19 | \$1.07 | \$0.16 |
| \$0.2 | \$20 | \$1.11 | \$0.17 |
| \$0.21 | \$21 | \$1.15 | \$0.17 |
| \$0.22 | \$22 | \$1.19 | \$0.18 |
| \$0.23 | \$23 | \$1.23 | \$0.18 |
| \$0.24 | \$24 | \$1.27 | \$0.19 |
| \$0.25 | \$25 | \$1.30 | \$0.20 |
| \$0.26 | \$26 | \$1.34 | \$0.20 |
| \$0.27 | \$27 | \$1.37 | \$0.20 |
| \$0.28 | \$28 | \$1.40 | \$0.21 |
| \$0.29 | \$29 | \$1.43 | \$0.21 |
| \$0.3 | \$30 | \$1.46 | \$0.22 |
| \$0.31 | \$31 | \$1.49 | \$0.22 |
| \$0.32 | \$32 | \$1.51 | \$0.23 |
| \$0.33 | \$33 | \$1.54 | \$0.23 |
| \$0.34 | \$34 | \$1.56 | \$0.23 |
| \$0.35 | \$35 | \$1.58 | \$0.24 |
| \$0.36 | \$36 | \$1.60 | \$0.24 |
| \$0.37 | \$37 | \$1.62 | \$0.24 |
| \$0.38 | \$38 | \$1.64 | \$0.25 |
| \$0.39 | \$39 | \$1.65 | \$0.25 |
| \$0.4 | \$40 | \$1.67 | \$0.25 |
| \$0.41 | \$41 | \$1.68 | \$0.25 |
| \$0.42 | \$42 | \$1.69 | \$0.25 |
| \$0.43 | \$43 | \$1.70 | \$0.25 |
| \$0.44 | \$44 | \$1.71 | \$0.26 |
| \$0.45 | \$45 | \$1.72 | \$0.26 |
| \$0.46 | \$46 | \$1.73 | \$0.26 |
| \$0.47 | \$47 | \$1.73 | \$0.26 |
| \$0.48 | \$48 | \$1.73 | \$0.26 |
| \$0.49 | \$49 | \$1.74 | \$0.26 |
| \$0.5 | \$50 | \$1.74 | \$0.26 |
| \$0.51 | \$51 | \$1.74 | \$0.26 |
| \$0.52 | \$52 | \$1.73 | \$0.26 |
| \$0.53 | \$53 | \$1.73 | \$0.26 |
| \$0.54 | \$54 | \$1.73 | \$0.26 |
| \$0.55 | \$55 | \$1.72 | \$0.26 |
| \$0.56 | \$56 | \$1.71 | \$0.26 |
| \$0.57 | \$57 | \$1.70 | \$0.25 |
| \$0.58 | \$58 | \$1.69 | \$0.25 |
| \$0.59 | \$59 | \$1.68 | \$0.25 |
| \$0.6 | \$60 | \$1.67 | \$0.25 |
| \$0.61 | \$61 | \$1.65 | \$0.25 |
| \$0.62 | \$62 | \$1.64 | \$0.25 |
| \$0.63 | \$63 | \$1.62 | \$0.24 |
| \$0.64 | \$64 | \$1.60 | \$0.24 |
| \$0.65 | \$65 | \$1.58 | \$0.24 |
| \$0.66 | \$66 | \$1.56 | \$0.23 |
| \$0.67 | \$67 | \$1.54 | \$0.23 |
| \$0.68 | \$68 | \$1.51 | \$0.23 |
| \$0.69 | \$69 | \$1.49 | \$0.22 |
| \$0.7 | \$70 | \$1.46 | \$0.22 |
| \$0.71 | \$71 | \$1.43 | \$0.21 |
| \$0.72 | \$72 | \$1.40 | \$0.21 |
| \$0.73 | \$73 | \$1.37 | \$0.20 |
| \$0.74 | \$74 | \$1.34 | \$0.20 |
| \$0.75 | \$75 | \$1.30 | \$0.20 |
| \$0.76 | \$76 | \$1.27 | \$0.19 |
| \$0.77 | \$77 | \$1.23 | \$0.18 |
| \$0.78 | \$78 | \$1.19 | \$0.18 |
| \$0.79 | \$79 | \$1.15 | \$0.17 |
| \$0.8 | \$80 | \$1.11 | \$0.17 |
| \$0.81 | \$81 | \$1.07 | \$0.16 |
| \$0.82 | \$82 | \$1.03 | \$0.15 |
| \$0.83 | \$83 | \$0.98 | \$0.15 |
| \$0.84 | \$84 | \$0.93 | \$0.14 |
| \$0.85 | \$85 | \$0.89 | \$0.13 |
| \$0.86 | \$86 | \$0.84 | \$0.13 |
| \$0.87 | \$87 | \$0.79 | \$0.12 |
| \$0.88 | \$88 | \$0.73 | \$0.11 |
| \$0.89 | \$89 | \$0.68 | \$0.10 |
| \$0.9 | \$90 | \$0.63 | \$0.09 |
| \$0.91 | \$91 | \$0.57 | \$0.09 |
| \$0.92 | \$92 | \$0.51 | \$0.08 |
| \$0.93 | \$93 | \$0.45 | \$0.07 |
| \$0.94 | \$94 | \$0.39 | \$0.06 |
| \$0.95 | \$95 | \$0.33 | \$0.05 |
| \$0.96 | \$96 | \$0.27 | \$0.04 |
| \$0.97 | \$97 | \$0.20 | \$0.03 |
| \$0.98 | \$98 | \$0.14 | \$0.02 |
| \$0.99 | \$99 | \$0.07 | \$0.01 |

### Standard Fee Rules

* Standard taker fees and maker rebates are symmetric around p = 0.50 and lowest near the extremes (0 and 1).
* All fees and rebates are rounded to the nearest \$0.01 using banker's rounding (round half to even).
* When an aggressive order fills against multiple resting orders, each fill is charged its banker's-rounded fee, adjusted so that the total commission collected across the order's fills never exceeds the banker's rounding of the cumulative exact fee. The adjustment can only reduce a fill's charge, never increase it. Maker rebates are computed per fill, independently.

### Standard Fee Examples

Maker rebates in these examples use the Tier 4 coefficient (0.0104). At Tier 2 or Tier 3, use 0.0035 or 0.0070 instead. Tier 1 makers receive no rebate.

#### Example 1: Buy 100 contracts at \$0.10 — cheap contract

Buying a long shot. The fee scales with price uncertainty: p × (1 − p) = 0.10 × 0.90 = 0.09.

* **Buyer (taker):** 0.0695 × 100 × 0.10 × 0.90 = **−\$0.63**
* **Seller (maker, Tier 4):** 0.0104 × 100 × 0.10 × 0.90 = **+\$0.09**

***

#### Example 2: Buy 100 contracts at \$0.65 — expensive contract

Buying a likely outcome. Higher price but lower p × (1 − p) than midpoint.

* **Buyer (taker):** 0.0695 × 100 × 0.65 × 0.35 = **−\$1.58**
* **Seller (maker, Tier 4):** 0.0104 × 100 × 0.65 × 0.35 = **+\$0.24**

***

#### Example 3: Sell 100 contracts at \$0.30 — sell low probability

The seller is the aggressor. Both sides pay based on the same p × (1 − p) factor.

* **Seller (taker):** 0.0695 × 100 × 0.30 × 0.70 = **−\$1.46**
* **Buyer (maker, Tier 4):** 0.0104 × 100 × 0.30 × 0.70 = **+\$0.22**

***

#### Example 4: Sell 100 contracts at \$0.90 — sell high probability

When the price is close to \$1.00, p × (1 − p) is small and fees are minimal.

* **Seller (taker):** 0.0695 × 100 × 0.90 × 0.10 = **−\$0.63**
* **Buyer (maker, Tier 4):** 0.0104 × 100 × 0.90 × 0.10 = **+\$0.09**

***

#### Example 5: Buy 100 contracts at \$0.50 — coin flip market

A 50/50 market. This is where the fee is highest per contract because p × (1 − p) = 0.25.

* **Buyer (taker):** 0.0695 × 100 × 0.50 × 0.50 = **−\$1.74**
* **Seller (maker, Tier 4):** 0.0104 × 100 × 0.50 × 0.50 = **+\$0.26**

## Combo Trading

### Maker Fees

From **12:00 AM ET on Saturday, November 14, 2026**, the combo maker fee is:

```
Fee = 0.03 × C × p × (1 - p)
```

* `C` is the number of contracts.
* `p` is the combo execution price in decimal dollars.

For example, 100 contracts filled at \$0.10 incur a maker fee of `0.03 × 100 × 0.10 × 0.90` = **\$0.27**.

For fills before that time, combo maker fills carry no maker fee. Quoters should account for the fee in their pricing from the effective time.

### Taker Fees

The combo taker fee is:

```
Fee = C × p × [Θ × (1 - p) + 0.06 × (1 - p)^4]
```

Θ is your [taker fee coefficient](#taker-fee-tiers).

* **`C`** is the number of contracts.
* **`p`** is the combo execution price in decimal dollars.
* The curve applies to the combo execution as a whole, not separately to its component legs.

Combo fills carry no maker rebate. A participant's taker rebate percentage is applied to the actual combo taker fee collected. Taker rebate tiers and payout cadence are unchanged.

### Combo Fee Schedule by Price

Taker amounts apply to fills from **November 11, 2026 at 1:00 PM ET**. **Maker fees are currently zero; the maker column shows the upcoming fees from November 14, 2026 at 12:00 AM ET.** Taker fees use the Tier 4 coefficient Θ = `0.0695`; for other tiers, use your [taker fee coefficient](#taker-fee-tiers) in the formula above.

Each row assumes a single fill of 100 contracts. Fees are rounded to the nearest cent using banker's rounding. Taker amounts are before any taker rebate.

| Price | Trade Value (100-lot) | Taker Pays (Tier 4, 100-lot) | Maker Pays (100-lot, from Nov 14) |
| - | - | - | - |
| \$0.01 | \$1 | \$0.13 | \$0.03 |
| \$0.02 | \$2 | \$0.25 | \$0.06 |
| \$0.03 | \$3 | \$0.36 | \$0.09 |
| \$0.04 | \$4 | \$0.47 | \$0.12 |
| \$0.05 | \$5 | \$0.57 | \$0.14 |
| \$0.06 | \$6 | \$0.67 | \$0.17 |
| \$0.07 | \$7 | \$0.77 | \$0.20 |
| \$0.08 | \$8 | \$0.86 | \$0.22 |
| \$0.09 | \$9 | \$0.94 | \$0.25 |
| \$0.10 | \$10 | \$1.02 | \$0.27 |
| \$0.11 | \$11 | \$1.09 | \$0.29 |
| \$0.12 | \$12 | \$1.17 | \$0.32 |
| \$0.13 | \$13 | \$1.23 | \$0.34 |
| \$0.14 | \$14 | \$1.30 | \$0.36 |
| \$0.15 | \$15 | \$1.36 | \$0.38 |
| \$0.16 | \$16 | \$1.41 | \$0.40 |
| \$0.17 | \$17 | \$1.46 | \$0.42 |
| \$0.18 | \$18 | \$1.51 | \$0.44 |
| \$0.19 | \$19 | \$1.56 | \$0.46 |
| \$0.20 | \$20 | \$1.60 | \$0.48 |
| \$0.21 | \$21 | \$1.64 | \$0.50 |
| \$0.22 | \$22 | \$1.68 | \$0.51 |
| \$0.23 | \$23 | \$1.72 | \$0.53 |
| \$0.24 | \$24 | \$1.75 | \$0.55 |
| \$0.25 | \$25 | \$1.78 | \$0.56 |
| \$0.26 | \$26 | \$1.80 | \$0.58 |
| \$0.27 | \$27 | \$1.83 | \$0.59 |
| \$0.28 | \$28 | \$1.85 | \$0.60 |
| \$0.29 | \$29 | \$1.87 | \$0.62 |
| \$0.30 | \$30 | \$1.89 | \$0.63 |
| \$0.31 | \$31 | \$1.91 | \$0.64 |
| \$0.32 | \$32 | \$1.92 | \$0.65 |
| \$0.33 | \$33 | \$1.94 | \$0.66 |
| \$0.34 | \$34 | \$1.95 | \$0.67 |
| \$0.35 | \$35 | \$1.96 | \$0.68 |
| \$0.36 | \$36 | \$1.96 | \$0.69 |
| \$0.37 | \$37 | \$1.97 | \$0.70 |
| \$0.38 | \$38 | \$1.97 | \$0.71 |
| \$0.39 | \$39 | \$1.98 | \$0.71 |
| \$0.40 | \$40 | \$1.98 | \$0.72 |
| \$0.41 | \$41 | \$1.98 | \$0.73 |
| \$0.42 | \$42 | \$1.98 | \$0.73 |
| \$0.43 | \$43 | \$1.98 | \$0.74 |
| \$0.44 | \$44 | \$1.97 | \$0.74 |
| \$0.45 | \$45 | \$1.97 | \$0.74 |
| \$0.46 | \$46 | \$1.96 | \$0.75 |
| \$0.47 | \$47 | \$1.95 | \$0.75 |
| \$0.48 | \$48 | \$1.95 | \$0.75 |
| \$0.49 | \$49 | \$1.94 | \$0.75 |
| \$0.50 | \$50 | \$1.92 | \$0.75 |
| \$0.51 | \$51 | \$1.91 | \$0.75 |
| \$0.52 | \$52 | \$1.90 | \$0.75 |
| \$0.53 | \$53 | \$1.89 | \$0.75 |
| \$0.54 | \$54 | \$1.87 | \$0.75 |
| \$0.55 | \$55 | \$1.86 | \$0.74 |
| \$0.56 | \$56 | \$1.84 | \$0.74 |
| \$0.57 | \$57 | \$1.82 | \$0.74 |
| \$0.58 | \$58 | \$1.80 | \$0.73 |
| \$0.59 | \$59 | \$1.78 | \$0.73 |
| \$0.60 | \$60 | \$1.76 | \$0.72 |
| \$0.61 | \$61 | \$1.74 | \$0.71 |
| \$0.62 | \$62 | \$1.71 | \$0.71 |
| \$0.63 | \$63 | \$1.69 | \$0.70 |
| \$0.64 | \$64 | \$1.67 | \$0.69 |
| \$0.65 | \$65 | \$1.64 | \$0.68 |
| \$0.66 | \$66 | \$1.61 | \$0.67 |
| \$0.67 | \$67 | \$1.58 | \$0.66 |
| \$0.68 | \$68 | \$1.56 | \$0.65 |
| \$0.69 | \$69 | \$1.52 | \$0.64 |
| \$0.70 | \$70 | \$1.49 | \$0.63 |
| \$0.71 | \$71 | \$1.46 | \$0.62 |
| \$0.72 | \$72 | \$1.43 | \$0.60 |
| \$0.73 | \$73 | \$1.39 | \$0.59 |
| \$0.74 | \$74 | \$1.36 | \$0.58 |
| \$0.75 | \$75 | \$1.32 | \$0.56 |
| \$0.76 | \$76 | \$1.28 | \$0.55 |
| \$0.77 | \$77 | \$1.24 | \$0.53 |
| \$0.78 | \$78 | \$1.20 | \$0.51 |
| \$0.79 | \$79 | \$1.16 | \$0.50 |
| \$0.80 | \$80 | \$1.12 | \$0.48 |
| \$0.81 | \$81 | \$1.08 | \$0.46 |
| \$0.82 | \$82 | \$1.03 | \$0.44 |
| \$0.83 | \$83 | \$0.98 | \$0.42 |
| \$0.84 | \$84 | \$0.94 | \$0.40 |
| \$0.85 | \$85 | \$0.89 | \$0.38 |
| \$0.86 | \$86 | \$0.84 | \$0.36 |
| \$0.87 | \$87 | \$0.79 | \$0.34 |
| \$0.88 | \$88 | \$0.74 | \$0.32 |
| \$0.89 | \$89 | \$0.68 | \$0.29 |
| \$0.90 | \$90 | \$0.63 | \$0.27 |
| \$0.91 | \$91 | \$0.57 | \$0.25 |
| \$0.92 | \$92 | \$0.51 | \$0.22 |
| \$0.93 | \$93 | \$0.45 | \$0.20 |
| \$0.94 | \$94 | \$0.39 | \$0.17 |
| \$0.95 | \$95 | \$0.33 | \$0.14 |
| \$0.96 | \$96 | \$0.27 | \$0.12 |
| \$0.97 | \$97 | \$0.20 | \$0.09 |
| \$0.98 | \$98 | \$0.14 | \$0.06 |
| \$0.99 | \$99 | \$0.07 | \$0.03 |

### Combo Fee Examples

These examples apply to fills from **November 11, 2026 at 1:00 PM ET** and before **November 14, 2026 at 12:00 AM ET**, while combo maker fees remain zero. Both examples assume a single fill and a Tier 4 taker coefficient of `0.0695`. Taker amounts are before any taker rebate.

#### Example 1: Buy 100 combo contracts at \$0.10

* **Buyer (taker):** 100 × 0.10 × \[0.0695 × 0.90 + 0.06 × 0.90^4] = \$1.01916 → **−\$1.02**
* **Seller (maker):** **\$0.00**. From November 14, 2026 at 12:00 AM ET: 0.03 × 100 × 0.10 × 0.90 = **−\$0.27**.

#### Example 2: Buy 100 combo contracts at \$0.25

* **Buyer (taker):** 100 × 0.25 × \[0.0695 × 0.75 + 0.06 × 0.75^4] = \$1.777734375 → **−\$1.78**
* **Seller (maker):** **\$0.00**. From November 14, 2026 at 12:00 AM ET: 0.03 × 100 × 0.25 × 0.75 = \$0.5625 → **−\$0.56**.

## FAQ

### Are fees deducted from my balance automatically?

Yes. Taker fees are deducted from your balance at the time of the trade. Maker rebates are credited to your balance at the time of the fill.

### Can fees ever be zero?

Yes. Fees are rounded to the nearest cent. On small trades (low quantity or prices near \$0.00 or \$1.00), the fee can round down to \$0.00.

### Do I pay fees when my order is canceled or expires?

No. Fees are only charged when a trade executes. If your order is canceled, expires, or is rejected, no fee is charged.

### What is banker's rounding?

Fees are rounded to the nearest cent using banker's rounding (round half to even). For example, \$0.025 rounds to \$0.02 (down to even), while \$0.035 rounds to \$0.04 (up to even).


This documentation is built and hosted on [Mintlify](https://mintlify.com), a developer documentation platform.
