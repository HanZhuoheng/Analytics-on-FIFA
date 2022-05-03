# Analytics-on-FIFA

This is the repository of 36-652 Statistical Computing II final project.

In order to complete Task I, I ingest the 6 FIFA player statistics dataset from 2015 to 2020 and populate the table with the cleaned final dateframe. Here is the table infrastructure from DbVisualizer. 

<img width="283" alt="Screen Shot 2022-05-03 at 1 33 21 AM" src="https://user-images.githubusercontent.com/89940463/166408670-5336af78-d2ba-4f0d-a2e4-df2c1671fe5c.png">

I conduct analytics on the cleaned dataset via Spark. All functions can be found in the Task II folder. For the first question, the input x is the number of players who achieved average highest improvement across all skillsets. All other questions have two inputs: the first one is year, which is year from 2015-2020, and the other one is x, which is the number of rows users want to receive (i.e. top x results).

In the Task III folder, I develop the unittest for all functions in Task II. For question 1, the pytest function tests the highest_improvement(5). For question 2, it tests the clubs_with_most_contracts_until_2021(2020,5). For question 3, it tests clubs_with_most_players(2020,5). For question 4, it tests most_popular_nation_positions(2020,5) and most_popular_team_positions(2020,5). For question 5, it tests most_popular_nationality(2020,5).

For Task IV, I submit screenshots that show evidences that I run the jupyter nootbook on Google Cloud Platform.

