import sys
sys.path.append('..')
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from RocketStocks.utils import secrets
import logging

# Logging configuration
logger = logging.getLogger(__name__)

class Postgres():
    def __init__(self):
        self.user = secrets.db_user
        self.pwd = secrets.db_password
        self.db = secrets.db_name
        self.host = secrets.db_host
        self.port = secrets.db_port
        self.conn = None
        self.cur = None
        
    # Open connection to PostgreSQL database
    def open_connection(self):
        self.conn = psycopg2.connect(
            host =self.host,
            dbname = self.db,
            user = self.user,
            password = self.pwd,
            port = self.port if self.port else 5432)

        self.cur = self.conn.cursor()

    # Close connection to PostgreSQL database
    def close_connection(self):
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None
    
    # Create database tables
    def create_tables(self):
        self.open_connection()
        create_script = """ CREATE TABLE IF NOT EXISTS tickers (
                            ticker          varchar(8) PRIMARY KEY,
                            cik             char(10) NOT NULL,
                            name            varchar(255) NOT NULL,
                            country         varchar(40), 
                            ipoyear         char(4),
                            industry        varchar(64),
                            sector          varchar(64),
                            url             varchar(64)
                            );

                            CREATE TABLE IF NOT EXISTS upcoming_earnings (
                            date                    date NOT NULL,
                            ticker                  varchar(8) PRIMARY KEY, 
                            time                    varchar(32),
                            fiscal_quarter_ending   varchar(10),
                            eps_forecast            varchar(8),
                            no_of_ests              varchar(8),
                            last_year_eps           varchar(8),
                            last_year_rpt_dt        varchar(10)
                            );

                            CREATE TABLE IF NOT EXISTS watchlists (
                            ID                  varchar(255) PRIMARY KEY,
                            tickers             varchar(255),
                            systemgenerated     boolean
                            );
                            
                            CREATE TABLE IF NOT EXISTS popularity (
                            datetime            timestamp,
                            rank                int,
                            ticker              varchar(8),
                            name                varchar(255),
                            mentions            int,
                            upvotes             int,
                            rank_24h_ago        int,
                            mentions_24h_ago    int,
                            PRIMARY KEY (datetime, rank)
                            );

                            CREATE TABLE IF NOT EXISTS historical_earnings (
                            date                date,
                            ticker              varchar(8),
                            eps                 float,
                            surprise            float,
                            epsForecast         float,
                            fiscalQuarterEnding varchar(10),            
                            PRIMARY KEY (date, ticker)
                            );

                            CREATE TABLE IF NOT EXISTS reports(
                            type                varchar(64) PRIMARY KEY,
                            messageid           bigint
                            );

                            CREATE TABLE IF NOT EXISTS alerts(
                            date                date,
                            ticker              varchar(8),
                            alert_type          varchar(64),
                            messageid           bigint,
                            alert_data          json,
                            PRIMARY KEY (date, ticker, alert_type)
                            );

                            CREATE TABLE IF NOT EXISTS daily_price_history(
                            ticker              varchar(8),
                            open                float,
                            high                float,
                            low                 float,
                            close               float,
                            volume              bigint,
                            date                date,
                            PRIMARY KEY (ticker, date)
                            );

                            CREATE TABLE IF NOT EXISTS five_minute_price_history(
                            ticker              varchar(8),
                            open                float,
                            high                float,
                            low                 float,
                            close               float,
                            volume              bigint,
                            datetime            timestamp,
                            PRIMARY KEY (ticker, datetime)
                            );

                            CREATE TABLE IF NOT EXISTS ct_politicians(
                            politician_id       char(7) PRIMARY KEY,
                            name                varchar(64),
                            party               varchar(16),
                            state               varchar(32)
                            );
                            """
        logger.debug("Running script to create tables in database...")
        self.cur.execute(create_script)
        self.conn.commit()
        logger.debug("Create script completed successfully!")

        self.close_connection()
    
    def init_tables(self):
        logger.debug("Initlializing database tables")
        
    
    # Drop database tables
    def drop_all_tables(self):
        self.open_connection()
        drop_script = """DROP TABLE alerts;
                         DROP TABLE daily_price_history;
                         DROP TABLE five_minute_price_history;
                         DROP TABLE historical_earnings;
                         DROP TABLE popularity;
                         DROP TABLE reports;
                         DROP TABLE tickers;
                         DROP TABLE upcoming_earnings;
                         DROP TABLE watchlists;
                        """

        self.cur.execute(drop_script)
        self.conn.commit()
        self.close_connection()
        logger.debug("All database tables dropped")
    
    def drop_table(self, table:str):
        self.open_connection()
        drop_script = sql.SQL("DROP TABLE IF EXISTS {sql_table};").format(
                                                                sql_table = sql.Identifier(table)
        )
        self.cur.execute(drop_script)
        self.conn.commit()
        self.close_connection()
        logger.debug(f"Dropped table '{table}' from database")

  
    def insert(self, table:str, fields:list, values:list):
        self.open_connection()
        
        # Insert into
        insert_script =  sql.SQL("INSERT INTO {sql_table} ({sql_fields})").format(
                                    sql_table = sql.Identifier(table),
                                    sql_fields = sql.SQL(',').join([
                                        sql.Identifier(field) for field in fields
                                    ]))

        # Values
        insert_script += sql.SQL(f"VALUES %s")

        # On conflict, do nothing
        insert_script += sql.SQL("ON CONFLICT DO NOTHING;")
        #mog = []
        #values_str = ','.join(self.cur.mogrify(f"({",".join(["%s"]*len(fields))})", value) for value in values)

        execute_values(cur=self.cur,
                       sql=insert_script,
                       argslist=values)

        self.conn.commit()
        self.close_connection()

    # Select row(s) from database
    def select(self, table:str, fields:list, where_conditions:list = [], order_by:tuple = tuple(), fetchall:bool = True):
        self.open_connection()
        values = tuple()
        
        # Select
        select_script = sql.SQL("SELECT {sql_fields} FROM {sql_table} ").format(
                                                                    sql_fields = sql.SQL(",").join([
                                                                        sql.Identifier(field) for field in fields
                                                                    ]),
                                                                    sql_table = sql.Identifier(table)
        )

        # Where conditions
        if len(where_conditions) > 0:
            where_script, where_values = self.where_clauses(where_conditions=where_conditions)
           
            # Update script and values
            select_script += where_script
            values += where_values
        

        # Order by
        if len(order_by) > 0:
            select_script += sql.SQL("ORDER BY {sql_field} {sql_order}").format(
                                                                sql_field = sql.Identifier(order_by[0]),
                                                                sql_order = sql.SQL(order_by[1])
            )

        
        # End script
        select_script += sql.SQL(';')

        self.cur.execute(select_script, values)
        if fetchall:
            result = self.cur.fetchall()
        else:
            result = self.cur.fetchone()
        self.close_connection()
        return result
    
    # Update row(s) in database
    def update(self, table:str, set_fields:list, where_conditions:list = []):
        self.open_connection()

        values = tuple()
        # Update
        update_script = sql.SQL("UPDATE {sql_table} ").format(
                                                            sql_table = sql.Identifier(table)
        )

        # Set
        set_columns = [field for (field, value) in set_fields]
        set_values = tuple([value for (field, value) in set_fields])
        values += set_values
        

        update_script += sql.SQL("SET ")
        update_script += sql.SQL(',').join([
            sql.SQL("{sql_field} = %s").format(
                sql_field = sql.Identifier(field)
            ) for field in set_columns
        ])

        # Where conditions
        if len(where_conditions) > 0:
            where_script, where_values = self.where_clauses(where_conditions=where_conditions)
           
            # Update script and values
            update_script += where_script
            values += where_values
        
        
        # End script
        update_script += sql.SQL(';')

        self.cur.execute(update_script, values)
        
        self.conn.commit()
        self.close_connection()
    
    # Delete row(s) from database
    def delete(self, table:str, where_conditions:list):
        self.open_connection()

        values = tuple()
        # Delete
        delete_script = sql.SQL("DELETE FROM {sql_table} ").format(
                                                            sql_table = sql.Identifier(table)
        )

        # Where conditions
        if len(where_conditions) > 0:
            where_script, where_values = self.where_clauses(where_conditions=where_conditions)
           
            # Update script and values
            delete_script += where_script
            values += where_values
        
        # End script
        delete_script += sql.SQL(';')

        self.cur.execute(delete_script, values)
        self.conn.commit()
        self.close_connection()

    # Generate where clauses SQL
    def where_clauses(self, where_conditions:list):
        
        where_script = sql.SQL(" WHERE ")
        values = tuple()

        where_clauses = []
        for condition in where_conditions:
            if len(condition) == 2:
                # Only field and value; use = operator
                where_clauses.append(sql.SQL("{sql_field} = %s").format(
                                    sql_field = sql.Identifier(condition[0])))
                values += (condition[1],)

            elif len(condition) == 3:
                # Operator specified
                where_clauses.append(sql.SQL("{sql_field} {sql_operator} %s").format(
                                    sql_field = sql.Identifier(condition[0]),
                                    sql_operator = sql.SQL(condition[1])))
                values += (condition[2],)
        where_script += sql.SQL(" AND ").join(where_clauses)

        return where_script, values

    # Return list of columns from selected table
    def get_table_columns(self, table):
        self.open_connection()

        # Select 
        select_script = sql.SQL("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS")

        # Where
        select_script += sql.SQL(f" WHERE TABLE_NAME = '{table}'")
                                                                
        # Order by
        select_script += sql.SQL(" ORDER BY ordinal_position;")

        self.cur.execute(select_script)
        result = self.cur.fetchall()
        columns = [column[0] for column in result]
        self.close_connection()
        return columns
