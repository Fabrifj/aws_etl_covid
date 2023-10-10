import pymysql

def lambda_handler(event, context):
    # Initialize connection to None
    connection = None
    
    try:
        connection = pymysql.connect(host='database-covid2.ci5us37i3fra.us-east-2.rds.amazonaws.com',
                                     port=3306,
                                     user='admin_db',
                                     password='p#%tG3*$8SU&YmAg',
                                     db='database-covid2')
        print("start")
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        
        return {
            'statusCode': 200,
            'body': str(record)
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }
    finally:
        # Closing database connection if it exists.
        if connection:
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")


print(lambda_handler("",""))