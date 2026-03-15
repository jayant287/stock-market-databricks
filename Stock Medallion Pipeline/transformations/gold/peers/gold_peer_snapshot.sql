-- Please edit the sample below

CREATE MATERIALIZED VIEW stock_market.gold.gold_peers_snapshot AS
SELECT
    symbol,
    COUNT(DISTINCT peer) AS peer_count
FROM stock_market.gold.gold_peers_current
GROUP BY symbol;