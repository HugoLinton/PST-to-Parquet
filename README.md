# PST-to-Parquet
This Project Takes the File Path of a PST File and converts it into a Scala case class and then to parquet. 

You can also query the data stored in Parquet to calculate the average email size and also the "Top Recipents".
 This is caluclated by a scoring system where 1 point is given to the account which has an email sent to and 0.5
 added for emails where the account is CC'ed.
 
 ##PST To Parquet Useage
 ```scala
 //Usage: ConvertPstToParquet <PST File Path> <Export Folder><Hadoop Home Directory>
 ```
 Simply run the ConvertPstToParquet class with the correct aruments and the parquet will
 be saved in the export folder.
 
 ##PST To Parquet Useage
  ```scala
  //Usage: ParquetQuery <Parquet Parent Folder>
  ```
  Simply run the ParquetQuery class with the path of the folder which is the parent to all the parquet folders.
  The code will go down one level looking for Parquet files, merge them all together using spark and then
  run the queries. The results will be outputted to the console.
 
 ##Notes
 The project will package all the dependencies into a "Uber" .jar file to avoid issues with missing
 dependencies.
 
 ##Issues
* Currently the CC field always stores and empty string when there is no data present
* The spark query for the average email length rounds up in the unit tests when the average
is 48.33. This could cause issues.

