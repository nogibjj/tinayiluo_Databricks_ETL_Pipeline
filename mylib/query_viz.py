from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = (
        "SELECT "
        "a.airline, "
        "a.incidents_85_99, "
        "b.incidents_00_14, "
        "a.fatal_accidents_85_99, "
        "b.fatal_accidents_00_14, "
        "b.fatalities_85_99, "
        "b.fatalities_00_14, "
        "(a.incidents_85_99 + b.incidents_00_14) AS total_incidents, "
        "a.fatal_accidents_85_99 + b.fatal_accidents_00_14 "
        "AS total_fatal_accidents, "
        "b.fatalities_85_99 + b.fatalities_00_14 "
        "AS total_fatalities "
        "FROM "
        "airline_safety1_delta AS a "
        "JOIN "
        "airline_safety2_delta AS b "
        "ON a.id = b.id "
        "ORDER BY total_incidents DESC "
        "LIMIT 10"
    )


    query_result = spark.sql(query)
    return query_result


# sample viz for project
def viz():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    # Convert the query_result DataFrame to Pandas for plotting
    query_result_pd = query.toPandas()

    # Bar Plot showing total Incidents vs Total Fatalities for all the Airlines (1985-2014)
    plt.figure(figsize=(15, 7))
    query_result_pd.plot(x='airline', y=['total_incidents', 'total_fatal_accidents', 
                                         'total_fatalities'], kind='bar')
    plt.title('Total Incidents vs. Fatal Accidents vs. '
              'Total Fatalities for Each Airline (1985-2014)')
    plt.ylabel('Counts')
    plt.xlabel('Airline')
    plt.xticks(rotation=45)
    plt.legend(title='Metrics')
    plt.tight_layout()
    plt.show()

    # Bar Plot showing total Incidents vs. Total Fatalities for selected Airline (1985-2014)
    # Filter for the specific airline
    selected_airline = 'Air France'  
    airline_data = query_result_pd[query_result_pd['airline'] == selected_airline]

    # Check if the filtered DataFrame is empty
    if airline_data.empty:
        print(f"No data available for {selected_airline}.")
        return

    # Bar Plot showing total Incidents vs. Total Fatalities for the selected Airline (1985-2014)
    plt.figure(figsize=(10, 6))
    airline_data.plot(x='airline', y=['total_incidents', 'total_fatal_accidents', 
                                      'total_fatalities'], kind='bar')
    plt.title(f'Total Incidents vs. Fatal Accidents vs. '
              f'Total Fatalities for {selected_airline} (1985-2014)')
    plt.ylabel('Counts')
    plt.xlabel('Airline')
    plt.xticks(rotation=0)  # No need to rotate for a single airline
    plt.legend(title='Metrics')
    plt.tight_layout()
    plt.show()
    
    # Prepare data for plotting
    periods = ['1985-1999', '2000-2014']

    # Initialize the figure
    plt.figure(figsize=(14, 8))

    # Plot trend lines for each airline
    for index, row in query_result_pd.iterrows():
        fatalities = [row['fatalities_85_99'], row['fatalities_00_14']]
        plt.plot(periods, fatalities, marker='o', label=row['airline'])

    # Customize the plot
    plt.title('Total Fatalities Change for Each Airline '
              '(1985-1999 vs 2000-2014)')
    plt.ylabel('Number of Fatalities')
    plt.xlabel('Time Period')
    plt.legend(title='Airlines', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()

    # Show the plot
    plt.show()
    
    # Select a specific airline 
    selected_airline = 'Air France'  
    airline_data = query_result_pd[query_result_pd['airline'] == selected_airline]

    # Prepare data for plotting
    periods = ['1985-1999', '2000-2014']
    fatalities_85_99 = airline_data['fatalities_85_99'].values[0]
    fatalities_00_14 = airline_data['fatalities_00_14'].values[0]
    fatalities = [fatalities_85_99, fatalities_00_14]

    # Plotting
    plt.figure(figsize=(8, 5))
    plt.plot(periods, fatalities, marker='o')
    plt.title(f'Total Fatalities Change for {selected_airline} '
              '(1985-1999 vs 2000-2014)')
    plt.ylabel('Number of Fatalities')
    plt.xlabel('Time Period')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    query_transform()
    viz()