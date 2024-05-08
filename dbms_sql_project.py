# Import Packages
import mysql.connector
from tabulate import tabulate

print("Beginning")

#Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="rootroot",
    database="test"
)
print(conn)

# For Atomicity
conn.autocommit = False

# For Isolation
conn.start_transaction(isolation_level = 'SERIALIZABLE')
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
    
except(Exception) as e:
    # Roll back the transaction if any error occurs
    conn.rollback()
    print(f"Error: {e}")

finally:
    if conn:
        # Commit the transaction if everything is successful
        conn.commit()
        conn.close()
        print("PostgreSQL connection is now closed")

print("End")