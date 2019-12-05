-- Postgres extensions, functions, and enums necessary for some views

CREATE EXTENSION IF NOT EXISTS intarray;


DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'total_obligation_bins') THEN
    CREATE TYPE public.total_obligation_bins AS ENUM ('<1M', '1M..25M', '25M..100M', '100M..500M', '>500M');
  ELSE
    RAISE NOTICE 'TYPE total_obligation_bins already exists, skipping creation...';
  END IF;
END$$;


CREATE OR REPLACE FUNCTION public.obligation_to_enum(award NUMERIC)
RETURNS public.total_obligation_bins
IMMUTABLE PARALLEL SAFE
AS $$
  DECLARE
    DECLARE result text;
  BEGIN
    IF award < 1000000.0 THEN result='<1M';              -- under $1 million
    ELSIF award < 25000000.0 THEN result='1M..25M';      -- under $25 million
    ELSIF award < 100000000.0 THEN result='25M..100M';   -- under $100 million
    ELSIF award < 500000000.0 THEN result='100M..500M';  -- under $500 million
    ELSE result='>500M';                                 --  over $500 million
    END IF;
  RETURN result::public.total_obligation_bins;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.urlencode(str_val TEXT)
RETURNS text
IMMUTABLE PARALLEL SAFE
AS $$
  DECLARE
    DECLARE result text;
  BEGIN
    result = REPLACE($1, '%', '%25');
    result = REPLACE(result, ' ', '%20');
    result = REPLACE(result, '!', '%21');
    result = REPLACE(result, '#', '%23');
    result = REPLACE(result, '$', '%24');
    result = REPLACE(result, '&', '%26');
    result = REPLACE(result, '''', '%27');
    result = REPLACE(result, '(', '%28');
    result = REPLACE(result, ')', '%29');
    result = REPLACE(result, '*', '%2A');
    result = REPLACE(result, '+', '%2B');
    result = REPLACE(result, ',', '%2C');
    result = REPLACE(result, '/', '%2F');
    result = REPLACE(result, ':', '%3A');
    result = REPLACE(result, ';', '%3B');
    result = REPLACE(result, '=', '%3D');
    result = REPLACE(result, '?', '%3F');
    result = REPLACE(result, '@', '%40');
    result = REPLACE(result, '[', '%5B');
    result = REPLACE(result, ']', '%5D');
  RETURN result;
  END;
$$ LANGUAGE plpgsql;
