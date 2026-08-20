-- Deliberately a no-op. On a database that ran the EDITED 000001 squash, the
-- table and column this migration caught up were created by 000001 — dropping
-- them here would have the down of one migration destroy the work of another.
-- There is nothing distinguishable to undo, so nothing is undone.
SELECT 1;
