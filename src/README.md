# Indeed Flex Data Engineering Coding Challenge

This exercise consists of developing a simple tool to compute the continuity of work for the workers on the platform.


## Data Format

In `worker_activity.csv` (please use the file provided in this repository), you will find data structured as below (here ordered by Worker for convenience):

```
Worker, Employer, Role, Date
1435, 234, 86,  2021-01-01 17:00:00
1435, 234, 86,  2021-01-04 12:30:00
1435, 234, 86,  2021-01-08 07:00:00
135,  45,  696, 2021-01-25 18:00:00
135,  45,  95,  2021-01-27 18:00:00
135,  45,  95,  2021-01-29 22:15:00
456,  78,  576, 2021-02-02 05:00:00
456,  78,  576, 2021-11-29 14:30:00
```


## Continuity of Work

We want to generate a report describing the continuity of work for each user *as of 2021-12-01*. This date is important when considering the first requirement
(no activity for 6 days), as this should be generated as if we we're running the report on 1st Dec 2021.


We increment continuity for each day worked, and the counter is reset when one of the rules below applies:

* A worker had no activity for more than 6 days.
* A worker stayed active but switched to a different employer.
* A worker stayed active but switched to a different role.

In the dataset above, this gives:

* Worker `1435` worked three different days between January 1st and January 8, and never switched role nor employer. So continuity=3
* Worker `135` appears three times, but switched role after the first occurrence; thus we only count the last two positions, therefore continuity=2.
* Worker `456` worked a first time in February 2020, then paused for more than 6 days, then worked again in November 2020; so continuity=1.


## Results

We expect a `results.csv` file following this format, ordered by `Continuity` (descending order).
The field `Continuity` is expressed in days.

```
Worker,Continuity
1435,3
135,2
456,1
```


## Bonus Points

We don't expect your solution to use sophisticated paradigms or frameworks. However, you can spend as much time you want to demonstrate your skills. 
You could, for example, implement some tests to ensure your output is correct. 


## Open questions

As part of your submission, please answer the following questions in a `readme.txt` file.

1. What is the complexity of your algorithm?
2. We provided a CSV file for convenience, but in a production environment, what format or database would you recommend instead for this use-case (and why)?
3. Is there anything else you would implement differently for a large-scale application in production?


## Submission

Please write your application in either Python, Java, Scala or Kotlin, with a runnable shell script named `run-app.sh` to launch your application.
Please put your results in `result.csv` and submit all the files in a zip file named as follows: `indeedflex-dataeng-<firstname>-<lastname>.zip`.
It's recommended to spend approximately 1 to 2 hours on this challenge. However, you are free to elaborate as much as you want to show us your ability to deliver great software!
