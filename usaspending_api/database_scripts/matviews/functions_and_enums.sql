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


CREATE OR REPLACE FUNCTION public.obligation_to_enum(award NUMERIC) RETURNS public.total_obligation_bins AS $$
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


CREATE OR REPLACE FUNCTION public.recipient_normalization_pair(original_name TEXT, search_duns TEXT) RETURNS RECORD AS $$
  DECLARE
    DECLARE result text[];
  BEGIN
    IF original_name ILIKE 'multiple recipients' THEN result = ARRAY['MULTIPLE RECIPIENTS', '-1'];
    ELSIF original_name ILIKE 'redacted due to pii' THEN result = ARRAY['REDACTED DUE TO PII', '-2'];
    ELSIF original_name ILIKE 'multiple foreign recipients' THEN result = ARRAY['MULTIPLE FOREIGN RECIPIENTS', '-3'];
    ELSIF original_name ILIKE 'private individual' THEN result = ARRAY['PRIVATE INDIVIDUAL', '-4'];
    ELSIF original_name ILIKE 'individual recipient' THEN result = ARRAY['INDIVIDUAL RECIPIENT', '-5'];
    ELSE result = ARRAY[(SELECT legal_business_name FROM duns WHERE awardee_or_recipient_uniqu = search_duns), search_duns];
    END IF;
  RETURN (COALESCE(result[1], ' '), result[2])::RECORD;
  END;
$$ LANGUAGE plpgsql;