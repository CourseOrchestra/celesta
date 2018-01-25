create schema after version '1.0';

EXECUTE NATIVE H2 AFTER --{{
  select 1;
--}};