-- Order date for open supplier POs (ETA = ordered_at + supplier lead).
alter table public.inventory_local_stock
  add column if not exists on_order_ordered_at date null;
