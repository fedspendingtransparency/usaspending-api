CREATE TABLE IF NOT EXISTS award_category
    (
        type_code text NOT NULL,
        type_name text NOT NULL
    );

TRUNCATE award_category;

INSERT INTO award_category
    (
        type_code,
        type_name
    )
VALUES
    ('A', 'contract'),
    ('B', 'contract'),
    ('C', 'contract'),
    ('D', 'contract'),
    ('02', 'grant'),
    ('03', 'grant'),
    ('04', 'grant'),
    ('05', 'grant'),
    ('06', 'direct payment'),
    ('10', 'direct payment'),
    ('07', 'loans'),
    ('08', 'loans'),
    ('09', 'insurance'),
    ('11', 'other');

CREATE INDEX IF NOT EXISTS award_category_type_code_idx ON award_category USING btree (type_code);
