# Rules

- All business data (transactions, trips, customers, orders, FX rates, events) lives in the S3 data lake. Answer
  data questions with the `hudi` MCP server's tools, following the `hudi-lake` skill.
- Convert order amounts to USD with the `fx_rates` table before summing across currencies.
