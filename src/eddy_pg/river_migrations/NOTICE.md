# River migrations

The `.up.sql` files in this directory are the database migrations of
[River](https://github.com/riverqueue/river), copied unmodified from
`riverdriver/riverpgxv5/migration/main` at version **v0.51.2**.

They are licensed under the Mozilla Public License 2.0; see `LICENSE` in this
directory. The rest of this package is MIT-licensed.

To update them, change the version in `scripts/sync-river-migrations.sh` and
run it. Do not edit the files by hand.
