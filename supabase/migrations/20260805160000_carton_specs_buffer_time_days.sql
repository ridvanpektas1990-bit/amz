-- Optional safety/buffer days after lead time for shipment sizing.
-- Charge cover days = production + shipping + buffer.

alter table public.inventory_carton_specs
  add column if not exists buffer_time_days integer
  check (buffer_time_days is null or buffer_time_days >= 0);

comment on column public.inventory_carton_specs.buffer_time_days is
  'Extra cover days after lead time; shipment qty = forecast demand over (lead + buffer) from expected arrival.';
