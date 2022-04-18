CREATE TABLE IF NOT EXISTS {{params.table_name}} (
   country VARCHAR(50),
   date VARCHAR(50),
   email VARCHAR(50),
   first_name VARCHAR(50),
   gender VARCHAR(50),
   id INT primary key,
   ip_address VARCHAR(50),
   last_name VARCHAR(50),
   ip_validity VARCHAR(50)
)