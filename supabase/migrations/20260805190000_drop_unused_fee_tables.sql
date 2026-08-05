-- Logistics cleanup: drop fee tables/views no longer used by the app.
-- Sales now come from amazon_orders + amazon_order_items only.
--
-- KEEP (do not drop):
--   amazon_connections, amazon_orders, amazon_order_items,
--   amazon_inventory_daily, vw_inventory_latest_per_asin_max,
--   inventory_carton_specs, inventory_products, events, etl_runs
--
-- Run in Supabase SQL Editor (or `supabase db execute` if linked).

-- 1) Drop fee-joined view first (depends on amazon_fees / amazon_orders)
DROP VIEW IF EXISTS public.vw_amazon_fees_orders;

-- 2) Drop unused fee tables
DROP TABLE IF EXISTS public.amazon_fee_lines;
DROP TABLE IF EXISTS public.amazon_account_fees;
DROP TABLE IF EXISTS public.amazon_fees;

-- 3) Reset incomplete order_items so the next backfill starts clean
TRUNCATE TABLE public.amazon_order_items;
