# Databricks notebook source
"""
query and viz file
"""

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
        "a.fatal_accidents_85_99 + b.fatal_accidents_00_14 AS total_fatal_accidents, "
        "b.fatalities_85_99 + b.fatalities_00_14 AS total_fatalities "
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
    query_result_pd.plot(x='airline', y=['total_incidents', 'total_fatal_accidents', 'total_fatalities'], kind='bar')
    plt.title('Total Incidents vs. Fatal Accidents vs. Total Fatalities for Each Airline (1985-2014)')
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
    airline_data.plot(x='airline', y=['total_incidents', 'total_fatal_accidents', 'total_fatalities'], kind='bar')
    plt.title(f'Total Incidents vs. Fatal Accidents vs. Total Fatalities for {selected_airline} (1985-2014)')
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
    plt.title('Total Fatalities Change for Each Airline (1985-1999 vs 2000-2014)')
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
    fatalities = [airline_data['fatalities_85_99'].values[0], airline_data['fatalities_00_14'].values[0]]

    # Plotting
    plt.figure(figsize=(8, 5))
    plt.plot(periods, fatalities, marker='o')
    plt.title(f'Total Fatalities Change for {selected_airline} (1985-1999 vs 2000-2014)')
    plt.ylabel('Number of Fatalities')
    plt.xlabel('Time Period')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    query_transform()
    viz()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Analysis and Conclusion:
# MAGIC
# MAGIC - Visualization 1: Total Incidents vs. Fatal Accidents vs. Total Fatalities for Each Airline (1985-2014)
# MAGIC
# MAGIC Incidents vs. Fatalities Disparity: It is noteworthy that some airlines, such as Aeroflot, with a high number of total incidents do not necessarily have a proportionately high number of fatalities. This could suggest effective emergency response and safety procedures that mitigate the severity of incidents. 
# MAGIC
# MAGIC - Visualization 2: Total Fatalities Change for Each Airline (1985-1999 vs 2000-2014)
# MAGIC
# MAGIC Overall Improvement: Most airlines have shown a decrease in the number of fatalities over the two time periods, which is a positive trend. This suggests that overall, safety may have improved in the airline industry. 
# MAGIC
# MAGIC Specific Airline Performance: Some airlines, such as Delta/Northwest and Saudi Arabian, have shown a significant decrease in fatalities, indicating a substantial improvement in their safety record. These airlines could be examined as case studies to understand what specific actions contributed to this improvement. 
# MAGIC
# MAGIC Challenges for Some: Conversely, airlines like American and Air France have shown a significant increase in fatalities. This would be a point of concern, and it's recommended that management conduct an in-depth review of safety protocols, fleet maintenance, and pilot training programs. Understanding the reasons behind the increase is critical to reversing this trend.
# MAGIC
# MAGIC
