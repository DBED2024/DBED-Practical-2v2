import mysql.connector
import math

# DBED Assignment 2
# Student ID: a1877853
# Name: Ibunkun Adeoye

class DBEDAssign2():
    def __init__(self,name):
        self.name=name

    def disp(self):
        print(self.name)

    def setUp(self):
        """Set up the DB connection to the 'postal' database"""
        self.connection = mysql.connector.connect(database='postal')
        self.cursor = self.connection.cursor(buffered=True)

    def syncDB(self):
        """We need to commit after insertion to ensure that the changes stick.
        syncDB is a wrapper to the connection commit that takes no parameters and
        returns no result."""
        self.connection.commit()

    def tearDown(self):
        """tearDown destroys the cursor and the connection to clean up."""
        self.cursor.close()
        self.connection.close()

    def show_all(self):
        """Select all rows in the database's pcode table and return it as a list to the user"""
        query = "SELECT * from pcode;"
        self.cursor.execute(query,)
        return self.cursor.fetchall()


    def select_by_pcode(self,pcode):
        """Perform a SELECT * query using the pcode parameter for postcode. Returns the query
        result as a list object."""
        query = "SELECT * FROM pcode WHERE postcode=%s;"
        self.cursor.execute(query,(pcode,))
        return self.cursor.fetchall()


    def insert_data(self,id,pcode,locality,state):
        """Insert data into the database"""
        query = "INSERT INTO pcode (postcode, locality, state) VVALUES (%s, %s, %s)"
        self.cursor.execute(query, (pcode, locality, state))
        return self.select_by_pcode(pcode)


    def readData(self,fname):
        """Read in the data from the CSV datafile called fname and put it into the database
        Takes a single string parameter and does not return any values.
        IMPORTANT: you must call syncDB before exiting or your changes won't stick!"""
        with open('./'+fname,"r") as csv:
            # Skip the header
            csv.readline()

            # Your code here to insert the data
            for row in csv:
                data = row.strip().split(',')
                if len(data) == 4:
                    id, pcode, locality, state = data
                    query = "INSERT INTO pcode (id, postcode, locality, state) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE postcode=%s, locality=%s, state=%s;"
                    self.cursor.execute(query, (id, pcode, locality, state, pcode, locality, state))
                else:
                    print(f"Skipping line due to unexpected format: {row}")
            csv.close()
            self.syncDB()

    def entropyCalc(self):
        """Analyse the postcode data to determine the entropy of the fourth column
        Takes no parameters and returns a single floating point number that is the
        total entropy of the fourth column.
        """
        counts = [0] * 10

        # Scan the database for the counts
        for i in range(10):
            query = 'SELECT COUNT(*) FROM pcode WHERE postcode LIKE %s;'
            self.cursor.execute(query, ('%' + str(i),))
            counts[i] = self.cursor.fetchtone()[0]
        print("Counts:", counts)

        total = sum(counts)
        if total == 0:
            return 0.0


        # Calculate the frequencies and total entropy
        entropy = 0.0
        for counts in counts:
            if count > 0:
                probability = count / total
                entropy -= probability * math.log2(probability)

        # Return the total entropy
        print("Total Entropy:", entropy)
        return entropy



