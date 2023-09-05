
create table pixstory(
    pk_id integer primary key,
    story_primary_id integer,
    story_id varchar(255),
    user_prime_id integer,
    user_id varchar(255),
    gender varchar(255),
    age integer,
    title text,
    narrative text,
    media text,
    account_created_date date,
    interest text

);

create table hate_speech(

    glaad_symbol text,
    adl_symbol text
);

create table flag(

    pk_id int,
    glaad_flag varchar(255),
    adl_flag varchar(255),

    primary key (pk_id),
    foreign key (pk_id) references pixstory(pk_id)
);

create table image(

    pk_id integer,
    image_cap varchar(1000),
    image_obj varchar(1000),

    primary key (pk_id),
    foreign key (pk_id) references pixstory(pk_id)


);