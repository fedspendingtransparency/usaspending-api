--DROP TABLE IF EXISTS award_delta;
--CREATE TABLE award_delta (award_id int PRIMARY KEY, generated_unique_award_id text,operation text, update_date TIMESTAMP);

CREATE OR REPLACE FUNCTION log_award_changes() RETURNS TRIGGER AS $award_audit$
	BEGIN
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO award_delta SELECT OLD.id, OLD.generated_unique_award_id, 'D', now();
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO award_delta SELECT NEW.id, NEW.generated_unique_award_id,  'U', now();
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO award_delta SELECT NEW.id, NEW.generated_unique_award_id,  'I', now();
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$award_audit$ LANGUAGE plpgsql;

CREATE TRIGGER award_delta_changes
	AFTER UPDATE OR DELETE OR INSERT
	ON awards
	FOR EACH ROW
	EXECUTE PROCEDURE log_award_changes();