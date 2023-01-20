
```
mysql -u root -p

```

enter password `root`


```
SELECT User,Host FROM mysql.user;

CREATE USER 'team'@'localhost' IDENTIFIED BY 'team1234';
CREATE USER 'team'@'%' IDENTIFIED BY 'team1234';

CREATE DATABASE ecommerce; 
GRANT ALL ON *.* TO 'team'@'localhost';
GRANT ALL ON *.* TO 'team'@'%';
FLUSH PRIVILEGES;

exit;
```
 

 ```
 USE ecommerce;

-- detect insert/update changes using timestamp , but HARD delete

create table products (id int, 
                       name varchar(255), 
                       price int, 
                       create_ts timestamp DEFAULT CURRENT_TIMESTAMP, 
                       update_ts timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP );
                       
             
exit
 ```


```

insert into products (id, name,price) values(1, 'product1', 100);
insert into products (id, name,price) values(2, 'product2', 200);
insert into products (id, name,price) values(3, 'product3', 300);
insert into products (id, name,price) values(4, 'product4', 400);
```

```
select * from products;
```
