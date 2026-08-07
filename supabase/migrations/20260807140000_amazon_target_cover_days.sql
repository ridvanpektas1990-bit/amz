-- Desired Amazon FBA cover (days) for local→Amazon ship sizing recommendations.
-- Universal Stammdaten defaults overwrite per SKU; each SKU can diverge afterwards.
alter table public.inventory_local_stock
  add column if not exists amazon_target_cover_days integer;

alter table public.inventory_local_stock
  alter column amazon_target_cover_days set default 30;

update public.inventory_local_stock
set amazon_target_cover_days = 30
where amazon_target_cover_days is null;

alter table public.inventory_local_stock
  alter column amazon_target_cover_days set not null;

alter table public.inventory_local_stock
  drop constraint if exists inventory_local_stock_amazon_target_cover_days_check;

alter table public.inventory_local_stock
  add constraint inventory_local_stock_amazon_target_cover_days_check
  check (amazon_target_cover_days >= 1);
