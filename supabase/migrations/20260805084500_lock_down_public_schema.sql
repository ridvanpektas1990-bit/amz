-- Phase 1 security baseline
--
-- The application and ETL workflows access Supabase exclusively through the
-- server-side service_role. No browser client currently needs direct table
-- access. Lock down the public API surface before adding end-user auth/RLS
-- policies in a later migration.

begin;

do $$
declare
  relation_name text;
begin
  for relation_name in
    select tablename
    from pg_tables
    where schemaname = 'public'
  loop
    execute format('alter table public.%I enable row level security', relation_name);
  end loop;
end
$$;

-- Tables and views are still available to service_role, but no longer to an
-- anonymous or merely authenticated PostgREST client without explicit grants.
revoke all privileges on all tables in schema public from anon, authenticated;
revoke all privileges on all sequences in schema public from anon, authenticated;
revoke execute on all functions in schema public from public, anon, authenticated;

commit;
