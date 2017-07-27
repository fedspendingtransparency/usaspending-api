
DROP SCHEMA IF EXISTS local_broker CASCADE


CREATE SCHEMA local_broker


CREATE TABLE local_broker.award_financial_assistance AS
  SELECT * FROM broker.award_financial_assistance
  WHERE submission_id = %(broker_submission_id)s


CREATE TABLE local_broker.award_procurement AS
    SELECT * FROM broker.award_procurement
    WHERE submission_id = %(broker_submission_id)s
