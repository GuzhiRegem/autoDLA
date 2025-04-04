AutoDLA behaivor is controlled using enviroment variables, here's a list of those

## General
- > ### `DATETIME_FORMAT`
> - *`DEFAULT_VALUE=`* `'%Y-%m-%d %H:%M:%S'`
> - *`TYPE=`* `STR`
>
> Controls the str format used for datetime conversion
- > ### `AUTODLA_SQL_VERBOSE`
> - *`DEFAULT_VALUE=`* `FALSE`
> - *`TYPE=`* `BOOL`
>
> Controls if AutoDLA logs the execution of SQL Querys

## AutoDLA WEB
- > ### `AUTODLAWEB_USER`
> - *`DEFAULT_VALUE=`* `'autodla'`
> - *`TYPE=`* `STR`
>
> User for AutoDLA WEB Admin Panel authentication
- > ### `AUTODLAWEB_PASSWORD`
> - *`DEFAULT_VALUE=`* `'password'`
> - *`TYPE=`* `STR`
>
> Password for AutoDLA WEB Admin Panel authentication
## PostgreSQL
Connection variables for PostgreSQL

- > ### `AUTODLA_POSTGRES_USER`
> - *`DEFAULT_VALUE=`* `'postgres'`
> - *`TYPE=`* `STR`
- > ### `AUTODLA_POSTGRES_PASSWORD`
> - *`DEFAULT_VALUE=`* `'password'`
> - *`TYPE=`* `STR`
- > ### `AUTODLA_POSTGRES_HOST`
> - *`DEFAULT_VALUE=`* `'localhost'`
> - *`TYPE=`* `STR`
- > ### `AUTODLA_POSTGRES_DB`
> - *`DEFAULT_VALUE=`* `'my_db'`
> - *`TYPE=`* `STR`