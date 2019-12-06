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


CREATE OR REPLACE FUNCTION public.urlencode(INOUT str_val text)
IMMUTABLE PARALLEL SAFE
AS $$
  BEGIN
    -- Only percent-encode special characters, pass all unicode and other ascii characters
    -- IMPORTANT! handle % first, otherwise it can return incorrect results
    -- Appears to be inefficient, but it beat an "optimized" algorithm using regex and arrays
    str_val := REPLACE(str_val, '%', '%25');
    str_val := REPLACE(str_val, ' ', '%20');
    str_val := REPLACE(str_val, '!', '%21');
    str_val := REPLACE(str_val, '#', '%23');
    str_val := REPLACE(str_val, '$', '%24');
    str_val := REPLACE(str_val, '&', '%26');
    str_val := REPLACE(str_val, '''', '%27');
    str_val := REPLACE(str_val, '(', '%28');
    str_val := REPLACE(str_val, ')', '%29');
    str_val := REPLACE(str_val, '*', '%2A');
    str_val := REPLACE(str_val, '+', '%2B');
    str_val := REPLACE(str_val, ',', '%2C');
    str_val := REPLACE(str_val, '/', '%2F');
    str_val := REPLACE(str_val, ':', '%3A');
    str_val := REPLACE(str_val, ';', '%3B');
    str_val := REPLACE(str_val, '=', '%3D');
    str_val := REPLACE(str_val, '?', '%3F');
    str_val := REPLACE(str_val, '@', '%40');
    str_val := REPLACE(str_val, '[', '%5B');
    str_val := REPLACE(str_val, ']', '%5D');
  END;
$$ LANGUAGE plpgsql;
