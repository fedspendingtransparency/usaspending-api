create table if not exists award_category
    (
        type_code text not null,
        type_name text not null
    );

create index if not exists award_category_type_code_idx on award_category using btree (type_code);

truncate award_category;

insert into award_category
    (
        type_code,
        type_name
    )
values
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