#Import Packages
import psycopg2
from tabulate import tabulate

print("Beginning")

#Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    dbname="postgres",
    user="postgres",
    password="rootroot",
    port="5432"
)
print(conn)

# For Atomicity
conn.autocommit = False

# For isolation: SERIALIZABLE
conn.set_isolation_level(3)

try:
    # Create a cursor object to execute SQL queries
    cur = conn.cursor()
    
    # Delete d1 from dep_id in the stock table
    cur.execute("""
        DELETE FROM stock 
        WHERE stock.dep_id = 'd1'
    """)
    # Delete d1 from dep_id in the depot table
    cur.execute("""
        DELETE FROM depot
        WHERE  depot.dep_id = 'd1'
    """)
    
    # Insert the new dd1 to depot table
    cur.execute("""
        INSERT INTO depot(dep_id, addr, volume)
        VALUES ('dd1', 'New York', '9000')
    """)
    
    # Update the dep_id in depot table
    cur.execute("""
        UPDATE depot
        SET dep_id = 'dd1'
        WHERE dep_id = 'd1'
    """)
    
    # Insert the new dd1 to the stock table
    cur.execute("""
        INSERT INTO stock (prod_id, dep_id, quantity)
        VALUES ('p1', 'dd1', '1000'),
            ('p3', 'dd1', '3000'),
            ('p2', 'dd1', '-400')
        """)
    
    # Commit the transaction if everything is successful
    conn.commit()
    
except(Exception, psycopg2.DatabaseError) as err:
    # Roll back the transaction if any error occurs
    conn.rollback()
    print(f"Error: {err}")

finally:
    if conn:
        # Commit the transaction if everything is successful
        conn.commit()
        conn.close()
        print("PostgreSQL connection is now closed")

print("End")