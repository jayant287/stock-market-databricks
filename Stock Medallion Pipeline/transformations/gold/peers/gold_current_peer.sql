-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_peers_current AS
SELECT
    symbol,
    peer
FROM stock_market.silver.silver_peers
WHERE __END_AT IS NULL;