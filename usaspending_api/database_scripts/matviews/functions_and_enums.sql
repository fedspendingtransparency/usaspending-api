-- Postgres extensions, functions, and enums necessary for some views

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'total_obligation_bins') THEN
    CREATE TYPE public.total_obligation_bins AS ENUM ('<1M', '1M', '1M..25M', '25M', '25M..100M', '100M', '100M..500M', '500M', '>500M');
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
    IF    award       IS NULL THEN result=NULL;
    ELSIF award <   1000000.0 THEN result='<1M';         -- under $1 million
    ELSIF award =   1000000.0 THEN result='1M';          -- $1 million
    ELSIF award <  25000000.0 THEN result='1M..25M';     -- under $25 million
    ELSIF award =  25000000.0 THEN result='25M';         -- $25 million
    ELSIF award < 100000000.0 THEN result='25M..100M';   -- under $100 million
    ELSIF award = 100000000.0 THEN result='100M';        -- $100 million
    ELSIF award < 500000000.0 THEN result='100M..500M';  -- under $500 million
    ELSIF award = 500000000.0 THEN result='500M';        -- $500 million
    ELSE  result='>500M';                                --  over $500 million
    END IF;
  RETURN result::public.total_obligation_bins;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.urlencode(INOUT str_val text)
IMMUTABLE PARALLEL SAFE
AS $$
  DECLARE
    DECLARE i text;
    DECLARE reassemble_array text[];
    DECLARE hex_code_array text[];
  BEGIN
    -- Percent-encode all characters which aren't considered the "unreserved list"
    --  of URL characters: A-Za-z0-9\-_.~
    --  (This is the most conservative approach when implementing URLencoding)
    -- The logic appears to be inefficient, but it beat more "optimized" algorithms

    -- IMPORTANT! handle % first, otherwise it can return incorrect results
    str_val := REPLACE(str_val, '%', '%25');

    FOREACH i IN ARRAY ARRAY(SELECT DISTINCT (regexp_matches (str_val, '([^A-Za-z0-9\-_.~%])', 'gi'))[1]) LOOP
      reassemble_array = '{}';
      hex_code_array = string_to_array(UPPER(encode(i::bytea, 'hex')), NULL);

      FOR i IN 0..array_length(hex_code_array, 1) - 1 LOOP
        IF i % 2 = 0 THEN
          reassemble_array := array_append(reassemble_array, '%');
        END IF;
        reassemble_array := array_append(reassemble_array, hex_code_array[i+1]);
      END LOOP;
      str_val := REPLACE(str_val, i, array_to_string(reassemble_array, ''));
    END LOOP;
  END;
$$ LANGUAGE plpgsql;
