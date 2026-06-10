# Semantic layer

<!-- vale Google.Acronyms = NO -->

A business-level reference for this dataset. This document is deliberately decoupled from the physical schema: no column lists, no join syntax. It describes what the data *means*, the conventions and quirks that bite people, and the shared vocabulary a team should agree on before anyone writes a query. The goal is something maintainable, that survives a column rename or an engine migration.

## Dataset overview

TPC-H is a decision-support benchmark from the Transaction Processing Performance Council. It simulates the analytical workload of a global wholesale supplier: a company that buys parts from suppliers and sells them, in bulk, to business customers worldwide. A tool called `dbgen` generates the data synthetically to exercise complex SQL, not to reflect a real business.

This distinction matters more than anything else in this document. TPC-H draws every number from a deterministic pseudo-random distribution. There are no seasonal trends, no customer churn, no regional preferences, no product affinities. Anything that looks like a "business insight" derived from TPC-H is almost certainly an artifact of the data generator. Use it to validate query logic, pipelines, and performance, not to make business decisions.

The world it models has a clear shape: customers place orders, each order contains one or more line items, each line item refers to a part bought from a supplier. Customers and suppliers both sit in countries, and countries roll up into five regions. That is the entire universe.

## Core business concepts

**Order.** A commercial transaction initiated by a customer on a specific date. An order is a header: it has a status, a priority, a total value, and a date. It does not itself contain products. The products live in its line items. When someone says "number of orders," they mean distinct order headers.

**Line item.** A single product on an order, with its own quantity, price, discount, tax, and shipping detail. This is the finest grain in the dataset and where almost all real revenue analysis happens. An order with five products is one order and five line items. Most aggregation mistakes in TPC-H come from confusing the order grain with the line-item grain.

**Customer.** A business buyer. Each customer belongs to one country and one market segment, and carries an account balance. There is no notion of a customer changing over time, no account creation date, no lifecycle. A customer simply exists.

**Supplier.** The sell-side counterpart. A supplier provides parts and also belongs to a country. The same country hierarchy applies to suppliers as to customers, which is the source of a very common ambiguity, explained in the caveats section below.

**Part.** A product in the catalogue, with a brand, a manufacturer, a type, a size, and a container. Customers buy parts through line items, and suppliers provide them via the part-supplier relationship.

**Nation and Region.** The geography hierarchy. Twenty-five countries roll up into exactly five regions. These are Africa, America, Asia, Europe, and the Middle East. Both customers and suppliers belong to a country, so you can measure geography from either side.

## The metrics that matter

The single most important pattern in TPC-H is revenue calculation, because several defensible definitions exist and they do not agree.

**Order total value.** Each order carries a pre-computed total in O_TOTALPRICE. The specification defines it as the sum over the order's line items of extended price reduced by discount and grossed up by tax. In other words it is the net-revenue-after-tax of the order, materialized once at generation time. It decomposes exactly into its line items, with one caveat: the spec truncates each line to two decimals before summing, so recomputing from the lines without replicating that per-line rounding can drift by a few cents. Treat it as a fast, stored shortcut to the order's billed amount, not as a separate or coarser measure: it equals the aggregation of the after-tax figure below.

**Gross line revenue.** The pre-discount value of a line item: price times the implied quantity already baked into the extended price. This is revenue before any concession.

**Net revenue.** Gross line revenue with the discount applied. This is the canonical TPC-H revenue measure and the one used in the benchmark's own queries. When the specification talks about "revenue," this is almost always what it means: extended price reduced by the discount rate.

**Net revenue after tax.** Net revenue with tax added back on. This is the closest the dataset gets to a billed amount, and it is the figure used in the benchmark's pricing summary.

**Discount given.** The monetary value conceded through discounting, that is, the gap between gross and net. Useful as a pricing-discipline lens, though in TPC-H discount rates are uniformly distributed and carry no real signal.

The rule of thumb: for anything resembling a P&L or margin question, work at the line-item grain with net revenue. Reach for the order total only for coarse, order-level cuts where speed matters more than precision.

Beyond revenue, the meaningful measures are largely counts and ratios: order volume, average order value, quantity shipped, and rate-style metrics like fulfillment rate, on-time delivery rate, late-delivery rate, and return rate. The delivery and return metrics only exist at the line-item level, because the relevant dates, ship, commit, and receipt, and the return flag live there.

## Status, priority, and dates

**Order status** is a lifecycle flag: in-progress, fulfilled, or partial. The order header carries it. Note that a parallel line-status concept exists at the line-item level, and the two overlap but are not identical. If you are reasoning about "is this order done," be explicit about which one you mean.

**Order priority** is a business-urgency label assigned at order creation, with values urgent, high, medium, not-specified, or low. It is a static attribute. It does not change as the order progresses, and in TPC-H it is uniformly distributed, so it cannot serve as a signal for real demand pressure.

**Dates.** The order date is the main time anchor. At the line-item level there are three further dates that drive all delivery analysis: the ship date, the committed date (which acts as the delivery SLA), and the receipt date. On-time delivery is receipt-on-or-before-commit. Lateness is receipt-after-commit. The benchmark builds its shipping-priority and order-priority queries entirely on the interplay of these dates.

## The standard business questions

TPC-H ships with twenty-two canonical queries, each phrased as a business question. They are the best guide to what questions the dataset can answer, and they double as a catalogue of analytical patterns. The ones worth knowing by name:

- **Pricing summary.** Total billed, shipped, and returned business, broken down by return status and line status. The reference revenue-aggregation query.
- **Minimum cost supplier.** Which supplier to use for a given part in a given region. A supply side sourcing question.
- **Shipping priority.** The highest-value orders not yet shipped. Revenue at risk by backlog.
- **Order priority checking.** Whether the priority system is working, as a proxy for customer satisfaction.
- **Local supplier volume.** Revenue flowing through suppliers in the same region as the customer. A localization-of-supply question.
- **Forecasting revenue change.** The revenue impact of changing a discount policy.
- **Volume shipping.** Trade volume between specific nations.
- **National market share.** A nation's share of revenue within its region, over time.
- **Product type profit.** Profit by product type and nation.
- **Returned item reporting.** Customers with the most returns, for service follow-up.
- **Shipping modes and order priority.** Whether cheaper shipping modes hurt high-priority orders by arriving late.
- **Customer distribution.** The relationship between customers and how many orders they place.
- **Promotion effect.** The revenue lift attributable to promotional parts.
- **Large-volume customer.** Identifying the biggest customers by order size.

The recurring themes are revenue, supply localization, delivery performance, returns, and customer concentration. If a stakeholder asks a question that does not map onto one of these themes, it probably is not answerable from TPC-H without inventing semantics the data does not have.

## Caveats and gotchas

These are the things that cause wrong answers, in rough order of how often they bite.

**Order grain versus line-item grain.** The most frequent error. Counting line items when you mean orders inflates volume. Summing line revenue and comparing it to the order total gives mismatches. Decide your grain first, then aggregate.

**Customer geography versus supplier geography.** "Revenue by region" is ambiguous. It can mean the customer's region or the region the goods came from. The two produce genuinely different distributions because a customer in one region routinely buys from suppliers in another. Always state which side you mean. This is the single biggest source of disagreement in regional analysis.

**Order total reconciles to after-tax charge, not to net revenue.** O_TOTALPRICE is the sum over the order's line items of the after-tax amount (extended price, less discount, plus tax), so it does decompose into its lines, subject only to the spec's per-line two-decimal truncation. What it does not match is line-level net revenue, which is pre-tax: the difference is exactly the tax. So if you sum net revenue per order and compare it to the order total, you will see a gap, and that gap is tax, not an inconsistency. Pick one measure, be explicit about whether it is pre- or post-tax, and stay consistent within an analysis.

**Synthetic uniformity.** Priorities, statuses, market segments, discount rates, and regional volumes are all close to uniformly distributed by design. The five regions have nearly identical revenue and order counts. Do not interpret "Europe is slightly ahead" as a finding. It is noise from the generator.

**Date range and truncation.** Order dates span roughly 1992 through mid-1998. The final month in the data is partial, so any monthly trend will show a sharp cliff at the end that is an artifact of where the generator stopped, not a business event. Annotate or exclude it.

**Scale factor.** A scale factor governs the dataset size. Everything scales proportionally with it, so absolute totals are meaningless in isolation. Only relationships and ratios carry across scale factors. When comparing results, confirm you are at the same scale factor.

**Returns and line status.** Return flags and line statuses only describe shipped line items and only make sense at the line grain. Aggregating them at the order level requires a deliberate choice about how to roll up a multi-line order.

**No slowly changing dimensions.** Customers, parts, and suppliers have no history. There is no "as of" version of any dimension, no effective dating. Any time-based attribution flows entirely through the order and line-item dates.

## Vocabulary to standardize

Agreeing on these terms up front prevents most cross-team confusion:

- **Revenue** should default to net revenue at the line-item grain. If anyone means the order total, they should say "order total value" explicitly.
- **Order** always means a distinct order header, never a line item.
- **Region** always requires a qualifier: customer region or supplier region.
- **On-time** means received on or before the committed date.
- **Fulfilled** describes order header status. A line ships at line status. Keep these separate.
- **Priority** is the static order-creation label, not a derived urgency.
- **Customer** is a stable identity with no lifecycle; do not speak of acquisition, churn, or tenure.
