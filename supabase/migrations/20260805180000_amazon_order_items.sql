-- Line items for SP-API orders (SKU-level units without fee settlement lag).

create table if not exists public.amazon_order_items (
  tenant_id text not null,
  marketplace text not null,
  amazon_order_id text not null,
  order_item_id text not null,
  seller_sku text,
  asin text,
  title text,
  quantity_ordered integer not null default 0,
  quantity_shipped integer not null default 0,
  purchase_date_local timestamptz,
  purchase_date_berlin date,
  iso_year integer,
  iso_week integer,
  updated_at timestamptz not null default now(),
  primary key (tenant_id, marketplace, amazon_order_id, order_item_id)
);

create index if not exists amazon_order_items_sku_date_idx
  on public.amazon_order_items (tenant_id, marketplace, seller_sku, purchase_date_berlin);

create index if not exists amazon_order_items_order_idx
  on public.amazon_order_items (tenant_id, marketplace, amazon_order_id);

alter table public.amazon_order_items enable row level security;
