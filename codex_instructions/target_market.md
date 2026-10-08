# Target Market and Business Questions

## 1. Market position

The Pakistan Economy Data Warehouse is a macroeconomic intelligence product for organizations that need trustworthy, regularly refreshed public economic data in one analytical layer. It turns dispersed government releases, which arrive in different formats and frequencies, into analysis-ready indicators and traceable datasets.

It is a decision-support platform, not a trading or investment-advice service. Its purpose is to shorten the time needed to find, validate, combine, and interpret official Pakistan economic statistics.

## 2. Target market

### Primary market — investment and financial-services teams

- Asset managers, mutual funds, and portfolio-research teams assessing Pakistan macroeconomic conditions.
- Brokerage and securities-firm research departments preparing market and sector commentary.
- Commercial, investment, and Islamic banks monitoring currency, trade, inflation, and liquidity-related conditions.
- Insurance companies and treasury teams assessing interest-rate, inflation, currency, and economic-risk exposure.
- Private-equity, venture-capital, and corporate-finance teams conducting market due diligence.

These users need a consistent view of official indicators, reliable history, and the ability to explain where each number came from.

### Secondary market — corporate strategy and operating businesses

- Importers, exporters, and manufacturers monitoring exchange-rate and trade conditions.
- Consumer-goods, retail, logistics, and distribution businesses tracking CPI, SPI, and fuel-price pressure.
- Multinational companies evaluating country conditions, repatriation context, and foreign-investment trends.
- Strategic-planning, finance, procurement, and pricing teams building budgets and scenario analyses.

These users need early visibility into cost pressure, demand conditions, currency movement, and sector-level economic activity.

### Tertiary market — institutions and research users

- Economic consultancies and advisory firms.
- Development organizations, policy researchers, universities, and think tanks.
- Journalism, data-storytelling, and public-interest research teams.

These users benefit from documented, reproducible public-data pipelines and clear source lineage.

## 3. What we are catering to

The product addresses five practical needs:

1. **One reliable analytical view of public data.** Official data is dispersed across SBP, PBS, and OGRA portals, released as CSV, Excel, PDF, and scanned PDF. The pipeline collects these sources into one governed warehouse.
2. **Faster research cycles.** Analysts should spend time interpreting economic conditions rather than repeatedly downloading files, cleaning columns, aligning dates, and checking duplicates.
3. **Confidence and traceability.** Every curated record should be traceable to the original government file, source URL, retrieval time, and source snapshot hash.
4. **Revision-aware historical analysis.** Government data can be corrected after publication. The pipeline detects changed source files and observations instead of treating every load as append-only.
5. **Dashboard-ready data.** Curated facts and dimensions will make Power BI reporting consistent across teams, avoiding conflicting spreadsheet calculations.

## 4. Business questions the pipeline answers

### Inflation, household-cost, and pricing pressure

- Is headline CPI increasing or decreasing compared with the previous month and the same month last year?
- Is inflation different across national, urban, and rural coverage?
- Which CPI groups or detailed items are contributing most to cost pressure?
- How are weekly SPI prices moving for essential goods and across cities?
- Are recent fuel-price changes likely to add cost pressure for transport, logistics, retail, or manufacturing?
- How do CPI, SPI, and fuel-price trends move together over time?

### Currency and external-sector conditions

- Is the Pakistani rupee strengthening or weakening against major currencies, and at what daily/monthly pace?
- What are the latest available exchange rates and their short- and medium-term trends?
- Are export receipts rising or falling by published commodity category?
- Are import payments rising or falling, and which categories appear to drive the movement?
- How has the trade environment changed over time when imports and exports are compared at validated, consistent aggregation levels?
- Are remittance inflows improving, weakening, or changing by reported geography/series?

### Investment and sector activity

- What is the direction of total FDI: net flow, inflow, and outflow?
- Which published sectors are attracting or losing foreign investment?
- Are FDI movements broad-based or concentrated in a few sectors?
- How do sector FDI trends compare with broader currency, trade, and inflation conditions?

### Business planning and risk monitoring

- What macroeconomic indicators should be monitored when refreshing a business plan, budget, or investment memo?
- Which indicators have changed materially since the prior reporting period?
- Where could exchange-rate, fuel, imported-input, or consumer-price movements affect cost assumptions?
- Which source publication or revision caused a reported indicator to change?
- Is an apparent movement based on a new observation, a corrected historical observation, or a reporting-period difference?

### Data governance and operations

- When was each source file retrieved, and what official URL produced it?
- Which records failed parsing or validation and require review?
- Has an upstream file changed since the prior acquisition?
- Which historical values were revised, and what were the old and new values?
- Are the current dashboard figures supported by complete, non-duplicated source data?

## 5. Key user personas

| Persona | Typical job to be done | Value delivered |
| --- | --- | --- |
| Equity or macro research analyst | Build an evidence-based view of Pakistan’s economic backdrop | Fast access to consistent historical series, trends, and source provenance |
| Treasury / risk analyst | Monitor currency and macroeconomic exposure | Timely FX, trade, remittance, inflation, and revision alerts |
| Corporate planner | Refresh budgets, forecasts, and pricing assumptions | Integrated signals for cost, currency, demand, and logistics pressure |
| Investment due-diligence team | Assess country and sector conditions before an investment decision | Sector FDI, trade, inflation, and FX evidence in one governed workspace |
| Data / BI analyst | Build recurring Power BI reports | Clean, documented Silver/Gold tables instead of repeated manual file preparation |
| Research institution | Produce reproducible economic analysis | Public-source lineage, preserved source snapshots, and revision history |

## 6. Product boundaries

The platform provides official-data ingestion, standardization, historical tracking, and visualization-ready metrics. It does not:

- issue investment recommendations or trade signals;
- forecast macroeconomic indicators unless a separately governed forecasting capability is added;
- replace official SBP, PBS, or OGRA publications;
- infer missing data as zero or silently alter official source values;
- combine aggregate and component series where doing so would double count.

## 7. Customer value proposition

For customers tracking Pakistan’s economy, the platform offers a practical alternative to fragmented downloads and spreadsheet maintenance: a single, source-traceable, revision-aware foundation for macroeconomic research and business planning. It caters to the need for speed without sacrificing the provenance and validation required for credible analysis.
