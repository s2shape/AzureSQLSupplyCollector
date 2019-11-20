--- Test data
use TestDb;
go

create table test_data_types (
   id_field INT IDENTITY(1, 1) CONSTRAINT test_data_types_pk PRIMARY KEY,
   bigint_field bigint,
   numeric_field numeric,
   bit_field bit,
   smallint_field smallint,
   decimal_field decimal,
   smallmoney_field smallmoney,
   int_field int,
   tinyint_field tinyint,
   money_field money,
   float_field float,
   real_field real,
   date_field date,
   datetimeoffset_field datetimeoffset,
   datetime2_field datetime2,
   smalldatetime_field smalldatetime,
   datetime_field datetime,
   time_field time,
   char_field char(40),
   varchar_field varchar(100),
   text_field text,
   nchar_field nchar(40),
   nvarchar_field nvarchar(100),
   ntext_field ntext
);
go

insert into test_data_types(bigint_field, numeric_field, bit_field, smallint_field, decimal_field, smallmoney_field, int_field, tinyint_field, money_field, float_field, real_field, date_field, datetimeoffset_field, datetime2_field, smalldatetime_field, datetime_field, time_field, char_field, varchar_field, text_field, nchar_field, nvarchar_field, ntext_field)
values(1, 3.14, 'TRUE', 10000, 1.23, 100000.50, 1, 2, 1000000.45, 3.54, 4.51, '2019-08-20', '2019-08-20 14:00:00', '2019-08-20 14:00:00', '2019-08-20 14:00:00', '2019-08-20 14:00:00', '14:00:00', 'char', 'varchar', 'text', 'nchar', 'nvarchar', 'ntext');
go

create table test_field_names (
   id int identity(1, 1) CONSTRAINT test_field_names_pk PRIMARY KEY,
   low_case int,
   UPCASE int,
   CamelCase int,
   [Table] int,
   [array] int,
   [SELECT] int
);
go

insert into test_field_names(low_case, upcase, camelcase, [Table], [array], [SELECT])
values(0,0,0,0,0,0);
go

create table test_index (
   id int NOT NULL IDENTITY(1, 1) CONSTRAINT test_index_pk PRIMARY KEY,
   name varchar(100) NOT NULL UNIQUE
);
go

insert into test_index(name)
values('Sunday');
insert into test_index(name)
values('Monday');
insert into test_index(name)
values('Tuesday');
insert into test_index(name)
values('Wednesday');
insert into test_index(name)
values('Thursday');
insert into test_index(name)
values('Friday');
insert into test_index(name)
values('Saturday');
go

create table test_index_ref (
   id INT IDENTITY(1,1) CONSTRAINT test_index_ref_pk PRIMARY KEY,
   index_id integer CONSTRAINT test_index_ref_fk FOREIGN KEY REFERENCES test_index(id)
);
go

insert into test_index_ref(index_id)
values(1);
insert into test_index_ref(index_id)
values(5);
go

COMMIT;
go
