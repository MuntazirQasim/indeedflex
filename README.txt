https://github.com/MuntazirQasim/indeedflex

## What is the complexity of your algorithm?

This algorithm has been broken down into separate functions. This helps reduce the complexity of the codebase by ensuring the code is structured well.

Both the first and last functions, `read_csv()` and `write_csv()`, simply take in the CSV file and a dataframe as arguments respectively.

In comparison, the most complex section of the algorithm is the `process_data()` function as this is where the majority of the algorithm's logic takes place. In this function a dataframe is passed in as an argument and the program then iterates through each worker in the dataset, processing each respective date.

Subsequently, using these dates, the program determines whether there are less than six days between each date. If this is the case, the program then checks if both the employer and role are equal.

If all of these conditions are met, worker continuity is then incremented for the respective worker.

Throughout the app - for example, within the for loops - comments have been added to ensure the code is readable and easy to follow.

## We provided a CSV file for convenience, but in a production environment, what format or database would you recommend instead for this use-case (and why)?

Since the data is currently utilised for reporting puposes, I would recommend using a _database_ because databases store data in a structured format. This means that it is easy for users to utilise the data without needing to posess an in-depth understanding of the data.

Onto which specific database to use, I would recommend **Oracle** for this usecase. This is because Oracle can:

- reliably store structured data securely
- be utilised for both **OLTP** and **OLAP** workloads\*

\*In this situation it would primarily be utilised for OLAP.

However, there are some drawbacks to Oracle such as it being very expensive - especially in comparison to alternatives such as **MySQL** which is open-sourced. It is of note though that MySQL has limited analytical processing capabilities which is the primary reason why I've recomended Oracle for this usecase.

## Is there anything else you would implement differently for a large-scale application in production?

### Iteration

Currently, this algorithm utilises a nested for loop. In a large-scale application, this would not be an ideal solution because it is inefficient for data processing. As a result, the program would run for a long time and use a lot of resources, incurring higher costs and reducing efficiency.

An effective alternative would be to use `DataFrame.itertuples` which is significantly faster but it would require manipulating the data into tuples beforehand. This method is preferable to another alternative Pandas method,  `DataFrame.iterrows` since the latter makes numerous function calls, is consequently slow at processing and is inefficient. 

### Tests

Moreover, the current tests could be expanded in a large-scale application. For example, writing additional tests of the output data as well as ensuring standardised linting tools are used across the team to improve code consistency.

In addition to unit testing, system integration and user acceptance testing should also be carried out which would involve liasing with the end-users.

Finally, whilst I am currently the sole contributor, in a large-scale application, pull requests should also be reviewed by multiple team members to ensure the code is monitored and improved effectively.