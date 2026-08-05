# Phase 1: Daten- und Sicherheits-Audit

Stand: 05.08.2026

## Systemübersicht

- Next.js 15 auf Vercel
- Supabase als produktive PostgreSQL-Datenbank
- Amazon SP-API über LWA OAuth und signierte AWS-Anfragen
- GitHub Actions für Orders, Gebühren, Lagerbestand, Backfills und Wiederholungen
- Serverzugriff auf Supabase ausschließlich über `SUPABASE_SERVICE_ROLE_KEY`
- Tenant-Ermittlung aktuell über einen signierten `amz_tenant`-Cookie

## Vorhandenes Datenmodell

Das öffentliche Schema enthält unter anderem:

- Amazon-Verbindungen, Orders, Gebühren und Gebührenzeilen
- tägliche Lagerbestandssnapshots und Lagerkosten
- Import-Jobs und ETL-Läufe
- Produkt-, lokaler Lager- und Kartonstammdaten
- bereits angelegte `tenants` und `tenant_users`
- Analytics- und Status-Views für Orders, Importe und Bestand

Nahezu alle operativen Tabellen enthalten bereits `tenant_id`. Das ist eine gute
Grundlage für Mandantenfähigkeit. Ausnahmen sind globale Referenzdaten wie
`events` und `marketplace_dim`.

## Kritischer Befund: anonymer Direktzugriff

Ein read-only Funktionstest mit dem öffentlichen Supabase-`anon`-Schlüssel hat
ohne Benutzerlogin sichtbare Zeilen in mehreren produktiven Objekten geliefert:

- `amazon_storage_sessions`
- `import_jobs`
- `inventory_products`
- `inventory_local_stock`
- `inventory_carton_specs`
- `amazon_connections_public`
- `vw_amazon_fees_orders`
- `vw_inventory_latest_per_asin_max`
- `vw_orders_weekly`
- `etl_last_status`

Der sensible Refresh-Token in `amazon_connections` war dabei nicht anonym
lesbar. Trotzdem sind Verkaufs-, Bestands-, Seller- und Importdaten betroffen.

### Maßnahme

Migration `20260805084500_lock_down_public_schema.sql`:

- aktiviert RLS auf allen Tabellen im Schema `public`
- entzieht `anon` und `authenticated` sämtliche direkten Tabellen-, View- und
  Sequenzrechte
- entzieht öffentlichen Rollen die Ausführung von Funktionen im Schema `public`
- verändert die bestehenden Rechte von `service_role` nicht

Die Migration wurde am 05.08.2026 über den Supabase SQL Editor auf der
Produktionsdatenbank ausgeführt.

### Verifikation nach der Migration

- Alle zehn zuvor anonym lesbaren Objekte antworten jetzt mit HTTP `401`.
- Kein getestetes Objekt liefert dem `anon`-Schlüssel weiterhin eine Zeile aus.
- Das Dashboard antwortet serverseitig weiterhin mit HTTP `200`.
- SKU-Liste, Wochenmetriken und Lagerbestand funktionieren weiterhin über
  `service_role`.
- Kontrollfall `8Y-MKK8-1CHG`: 110 Einheiten in den letzten 30 Tagen und 179
  Einheiten Lagerbestand werden weiterhin korrekt geladen.

Vor der späteren Freigabe für echte SaaS-Benutzer werden gezielte RLS-Policies
für Supabase Auth und `tenant_users` als eigene Migration ergänzt.

## Datenpipeline

Positiv:

- `etl_runs` protokolliert bereits Laufstatus pro Tenant, Marketplace und Monat.
- `import_jobs` bildet Backfill-Aufträge und Status ab.
- Nightly-, Monthly-, Onboarding-, Inventory- und Retry-Workflows existieren.
- Die meisten operativen Tabellen besitzen eine Tenant-Spalte.

Offene Risiken:

- Workflow-Logik liegt teilweise als große Inline-Python-Blöcke in YAML-Dateien.
- Es gibt noch keine vollständige, versionierte Historie des bestehenden
  Supabase-Schemas im Repository.
- Datenfrische und letzter erfolgreicher Lauf werden im Dashboard nicht klar
  angezeigt.
- Die ETL-Laufstruktur unterscheidet nicht sauber zwischen Orders, Gebühren und
  Bestand und besitzt keine standardisierten Zähler oder Fehlercodes.

## KPI- und Forecast-Logik

Die OOS-Prognose liegt derzeit doppelt vor:

- tageweise in `/api/inventory/overview`
- wochenweise im Dashboard für Chart und Bestellplanung

Dadurch können gleiche SKUs unterschiedliche Ergebnisse anzeigen. Die nächste
Phase-1-Maßnahme ist deshalb eine gemeinsame, serverseitig getestete
Forecast-Bibliothek. Der Fall `8Y-MKK8-1CHG` wird als Regressionstest festgehalten:
Nullphasen im Referenzjahr müssen auf das reale 30-Tage-Tempo zurückfallen.

## Priorisierte Folgearbeiten

1. Sicherheitsmigration anwenden und anonymen Zugriff erneut testen.
2. Vollständiges Datenbankschema als versionierte Baseline erfassen.
3. Forecast-Engine zentralisieren und Regressionstests hinzufügen.
4. Einheitlichen Sync-Status mit Datenfrische und Fehlerdetails bereitstellen.
5. Sync-Status und Datenstand im Dashboard anzeigen.
6. Anschließend Supabase Auth und Tenant-RLS-Policies einführen.
