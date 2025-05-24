environment of this experiment is as follows:
Linux operating system: Ubuntu 24.04.2
Java operating environment: JDK 17
Hadoop version: 3.4.1
Maven version: 3.8.7
IDE: IntelliJ IDEA

The City-simple.tsv file contains data harvested from Wikipedia concerning cities around the world. The file contains one line per city, and seven tab-separated fields per line


These fields are the URI (unique identifier) of the city, its name, the name of the country it is in, the URI of the country, the city population, and its latitude and longitude. This experiment will use the small dataset to work out the urban population of each country in the dataset.
Before writing the code, we will answer the following questions and write a pseudocode detailing the map and reduce methods.


1. What part of the MapReduce system will do the grouping of records by country?
The Shuffle and Sort phase of MapReduce automatically handles the grouping. This is the intermediate phase between the Map and Reduce stages that:
i.	Collects all key-value pairs from the mappers.
ii.	Groups all values with the same key together.
iii.	Sorts the keys.
iv.	Sends each key and its associated list of values to a reducer.
2. What does the Mapper need to do to its inputs?
The Mapper needs to:
i.	Parse each input line (splitting by tabs).
ii.	Extract the country identifier (country name or URI) and the city population.
iii.	Emit a key-value pair where the country is the key and the population is the value.
iv.	Handle any data quality issues (missing values, invalid formats)
3. What should the keys and values be for the input to the Reducer?
Key: Country identifier (country name or country URI).
Values: An iterable list of population values from all cities in that country.
Example: ("United States", [8336817, 3971883, 2720546, ...])
4. What should the keys and values be for the output from the Mapper?
Key: Country identifier (country name or country URI).
Value: Individual city population (as an integer or long).
Example: ("United States", 8336817) for New York City
5. What part of that process does the reducer need to do?
The Reducer needs to:
i.	Receive a country key and all its associated city population values.
ii.	Sum up all the population values for that country.
iii.	Output the country and its total urban population.
