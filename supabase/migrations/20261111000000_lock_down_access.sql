-- Lock down access to the public schema.
--
-- Fixes, in order:
--   1. Sign-up could create an admin: the role came straight from client-supplied metadata.
--   2. Users could edit their own role / is_verified, because profile updates had no column limits.
--   3. Anyone with the publishable key, signed in or not, could read every profile (emails, phone numbers).
--   4. Any signed-in user could rewrite any advisor's statistics.
--   5. Unverified advisors could create trades; admin approval now gates trade creation.
--   6. SECURITY DEFINER functions were callable over /rest/v1/rpc; stats functions had a mutable search_path.
--   7. Policies re-ran auth.uid() / get_role() per row; they now run once per query (Supabase lint auth_rls_initplan).
-- Safe to run more than once.

begin;

-- 1. Sign-up may only pick 'user' or 'advisor'. Admins are promoted by hand in the dashboard.
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  user_role text := coalesce(new.raw_user_meta_data->>'role', 'user');
begin
  if user_role not in ('user', 'advisor') then
    user_role := 'user';
  end if;

  insert into public.profiles (id, email, role) values (new.id, new.email, user_role);

  if user_role = 'advisor' then
    insert into public.advisor_details (profile_id) values (new.id);
  else
    insert into public.user_details (profile_id) values (new.id);
  end if;

  return new;
end;
$$;

-- 6. Stats functions run as their owner (the calling user may not be allowed to update platform_statistics)
--    with a fixed search_path.
alter function public.update_platform_statistics() security definer set search_path = public;
alter function public.handle_statistics_update() security definer set search_path = public;

-- Trigger and stats functions are internal: nobody calls them over the API.
-- (Triggers don't need EXECUTE at fire time.)
revoke execute on function public.handle_new_user() from public, anon, authenticated;
revoke execute on function public.handle_subscription_change() from public, anon, authenticated;
revoke execute on function public.handle_trade_card_closure() from public, anon, authenticated;
revoke execute on function public.handle_statistics_update() from public, anon, authenticated;
revoke execute on function public.update_platform_statistics() from public, anon, authenticated;
revoke execute on function public.update_advisor_stats(uuid) from public, anon, authenticated;
-- get_role() is used inside policies, so signed-in users keep it.
revoke execute on function public.get_role() from public, anon;
grant execute on function public.get_role() to authenticated;

-- 3. Signed-out clients need no table access at all (sign-up goes through Supabase Auth).
revoke all on all tables in schema public from anon;
alter default privileges in schema public revoke all on tables from anon;

-- 2 + 3. Profiles: signed-in users read only safe columns (no phone number or push token),
--        and can change only their name, phone number and push token.
revoke all on public.profiles from authenticated;
grant select (id, email, role, first_name, last_name, is_verified, created_at) on public.profiles to authenticated;
grant update (first_name, last_name, mobile_number, fcm_token) on public.profiles to authenticated;

-- 4. Advisor statistics are written only by update_advisor_stats() (runs as owner).
revoke insert, update, delete on public.advisor_statistics from authenticated;

-- Drop every existing policy on these tables, then recreate them.
do $$
declare p record;
begin
  for p in
    select policyname, tablename from pg_policies
    where schemaname = 'public'
      and tablename in ('profiles', 'advisor_details', 'user_details', 'advisor_statistics',
                        'platform_statistics', 'subscriptions', 'watchlists', 'trade_cards')
  loop
    execute format('drop policy %I on public.%I', p.policyname, p.tablename);
  end loop;
end $$;

-- profiles: your own row, plus advisors (shown to users browsing advisors and on trade cards)
create policy "Read own profile and advisor profiles" on public.profiles
  for select to authenticated
  using (id = (select auth.uid()) or role = 'advisor');
create policy "Update own profile" on public.profiles
  for update to authenticated
  using (id = (select auth.uid())) with check (id = (select auth.uid()));

-- advisor_details
create policy "Signed-in users read advisor details" on public.advisor_details
  for select to authenticated using (true);
create policy "Advisors create own details" on public.advisor_details
  for insert to authenticated with check (profile_id = (select auth.uid()));
create policy "Advisors update own details" on public.advisor_details
  for update to authenticated
  using (profile_id = (select auth.uid())) with check (profile_id = (select auth.uid()));

-- user_details
create policy "Users manage own details" on public.user_details
  for all to authenticated
  using (profile_id = (select auth.uid())) with check (profile_id = (select auth.uid()));

-- advisor_statistics (read-only for clients)
create policy "Signed-in users read advisor statistics" on public.advisor_statistics
  for select to authenticated using (true);

-- platform_statistics
create policy "Admins read platform statistics" on public.platform_statistics
  for select to authenticated using ((select public.get_role()) = 'admin');

-- subscriptions
create policy "Users read own subscriptions" on public.subscriptions
  for select to authenticated using (user_id = (select auth.uid()));
create policy "Users create own subscriptions" on public.subscriptions
  for insert to authenticated with check (user_id = (select auth.uid()));
create policy "Users delete own subscriptions" on public.subscriptions
  for delete to authenticated using (user_id = (select auth.uid()));

-- watchlists
create policy "Advisors read own watchlist" on public.watchlists
  for select to authenticated using (advisor_id = (select auth.uid()));
create policy "Advisors add to own watchlist" on public.watchlists
  for insert to authenticated with check (advisor_id = (select auth.uid()));
create policy "Advisors remove from own watchlist" on public.watchlists
  for delete to authenticated using (advisor_id = (select auth.uid()));

-- trade_cards: advisors see their own; users see trades of advisors they subscribe to
create policy "Read own or subscribed trade cards" on public.trade_cards
  for select to authenticated
  using (
    ((select public.get_role()) = 'advisor' and advisor_id = (select auth.uid()))
    or ((select public.get_role()) = 'user' and advisor_id in (
          select s.advisor_id from public.subscriptions s where s.user_id = (select auth.uid())))
  );
-- 5. Only advisors an admin has approved can create trades.
create policy "Verified advisors create own trade cards" on public.trade_cards
  for insert to authenticated
  with check (
    advisor_id = (select auth.uid())
    and exists (select 1 from public.profiles p
                where p.id = (select auth.uid()) and p.role = 'advisor' and p.is_verified)
  );
create policy "Advisors update own trade cards" on public.trade_cards
  for update to authenticated
  using ((select public.get_role()) = 'advisor' and advisor_id = (select auth.uid()))
  with check ((select public.get_role()) = 'advisor' and advisor_id = (select auth.uid()));
create policy "Advisors delete own trade cards" on public.trade_cards
  for delete to authenticated
  using ((select public.get_role()) = 'advisor' and advisor_id = (select auth.uid()));

-- Indexes for the foreign keys the policies and stats queries filter on
create index if not exists trade_cards_advisor_id_idx on public.trade_cards (advisor_id);
create index if not exists subscriptions_advisor_id_idx on public.subscriptions (advisor_id);

commit;
