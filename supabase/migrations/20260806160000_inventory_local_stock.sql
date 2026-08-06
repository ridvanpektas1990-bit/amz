-- Local / 3PL warehouse stock + open supplier POs (multi-echelon).
-- Table may already exist in production; use IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

create table if not exists public.inventory_local_stock (
  tenant_id text not null,
  seller_sku text not null,
  local_qty integer not null default 0,
  on_order_units integer not null default 0,
  transfer_lead_days integer not null default 7,
  last_inbound_seen integer null,
  updated_at timestamptz not null default now(),
  constraint inventory_local_stock_pkey primary key (tenant_id, seller_sku),
  constraint fk_inventory_local_stock_product
    foreign key (tenant_id, seller_sku)
    references public.inventory_products (tenant_id, seller_sku)
    on delete cascade
);

alter table public.inventory_local_stock
  add column if not exists transfer_lead_days integer not null default 7;

alter table public.inventory_local_stock
  add column if not exists last_inbound_seen integer null;

alter table public.inventory_local_stock
  add column if not exists on_order_ordered_at date null;

create index if not exists idx_inventory_local_stock_tenant
  on public.inventory_local_stock using btree (tenant_id);

create index if not exists idx_inventory_local_stock_sku
  on public.inventory_local_stock using btree (seller_sku);

alter table public.inventory_local_stock enable row level security;
