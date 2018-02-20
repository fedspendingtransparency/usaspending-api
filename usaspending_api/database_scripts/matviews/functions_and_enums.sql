-- The function(s) and enum(s) below were originally created for the materialized views

CREATE TYPE total_obligation_bins AS ENUM ('<1M', '1M..25M', '25M..100M', '100M..500M', '>500M');
CREATE TYPE award_type AS ENUM ('A', 'B', 'C', 'D', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11');
CREATE TYPE action_type AS ENUM ('b','H','S','C','L','J','W','V','N','Y','A','R',
                                 'a','X','D','B','M','G','F','c','K','P','T','E');
CREATE TYPE contract_pricing_type AS ENUM ('0','1','2','3','A','B','J','K','L','M',
                                           'NO','O','P','R','S','T','U','V','X','Y','Z');


CREATE OR REPLACE FUNCTION obligation_to_enum(award NUMERIC) RETURNS total_obligation_bins AS $$
  DECLARE
    DECLARE result text;
  BEGIN
    IF award < 1000000.0 THEN result='<1M';              -- under $1 million
    ELSIF award < 25000000.0 THEN result='1M..25M';      -- under $25 million
    ELSIF award < 100000000.0 THEN result='25M..100M';   -- under $100 million
    ELSIF award < 500000000.0 THEN result='100M..500M';  -- under $500 million
    ELSE result='>500M';                                 -- over $500 million
    END IF;
  RETURN result::total_obligation_bins;
  END;
$$ LANGUAGE plpgsql;
