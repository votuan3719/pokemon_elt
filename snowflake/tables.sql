CREATE OR REPLACE TABLE tcg_pokemon_api (
    json_data VARIANT
);

SET stage_path = CONCAT('@pokemon.public.cards/', TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD'), '/');
SET copy_into = CONCAT('COPY INTO pokemon.dbt_pokemon.tcg_pokemon_api FROM ', $stage_path, ' FILE_FORMAT = (TYPE = JSON)');
EXECUTE IMMEDIATE $copy_into;

CREATE OR REPLACE TABLE price_tracker_api (
    json_data VARIANT
);

SET stage_path = CONCAT('@pokemon.public.prices/', TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD'), '/');
SET copy_into = CONCAT('COPY INTO pokemon.dbt_pokemon.price_tracker_api FROM ', $stage_path, ' FILE_FORMAT = (TYPE = JSON)');
EXECUTE IMMEDIATE $copy_into;

CREATE OR REPLACE TABLE tcg_sets_api (
    json_data VARIANT
);

SET stage_path = CONCAT('@pokemon.public.sets/');
SET copy_into = CONCAT('COPY INTO pokemon.dbt_pokemon.tcg_sets_api FROM ', $stage_path, ' FILE_FORMAT = (TYPE = JSON)');
EXECUTE IMMEDIATE $copy_into;
