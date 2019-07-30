

--bhushan@bhushan-VirtualBox:~/hadoop-2.7.3$ bin/hadoop fs -mkdir /Stackoverflow_data_analysis


--bhushan@bhushan-VirtualBox:~/hadoop-2.7.3$ bin/hadoop fs -put /home/bhushan/Stackoverflow_Data_Analysis_using_Apache_Pig/comments.csv /Stackoverflow_data_analysis 
--bhushan@bhushan-VirtualBox:~/hadoop-2.7.3$ bin/hadoop fs -put /home/bhushan/Stackoverflow_Data_Analysis_using_Apache_Pig/users.csv /Stackoverflow_data_analysis  
--bhushan@bhushan-VirtualBox:~/hadoop-2.7.3$ bin/hadoop fs -put /home/bhushan/Stackoverflow_Data_Analysis_using_Apache_Pig/posts.csv /Stackoverflow_data_analysis  
--bhushan@bhushan-VirtualBox:~/hadoop-2.7.3$ bin/hadoop fs -put /home/bhushan/Stackoverflow_Data_Analysis_using_Apache_Pig/posttypes.csv /Stackoverflow_data_analysis 

A = load '/Stackoverflow_data_analysis/comments.csv' USING PigStorage(',') as (id:int, user_id:int);

B = load '/Stackoverflow_data_analysis/users.csv' USING PigStorage(',') as (id:int, reputation:int, displayname:chararray, loc:chararray, age:int);

C = load '/Stackoverflow_data_analysis/posts.csv' USING PigStorage(',') as (id:int, post_type:int, creation_date:chararray, score:int, viewcount:float, owneruser_id:int, title:chararray, answercount:int, commentcount:int);


D = load '/Stackoverflow_data_analysis/posttypes.csv' USING PigStorage(',') as (id:int, name:chararray);



--A. Find the display name and no. of posts created by the user who has got maximum reputation.

--b = join B by id, C by id;
--c = order b by reputation DESC;
--d = limit c 1;
--e = foreach d generate displayname, post_type;
--dump e; 

--Answer === (Jon Skeet,2)


--B. Find the average age of users on the Stack Overflow site.

--b = group B by all;
--c = foreach b generate AVG(B.age);
--dump c;

--Answer === (35.263352397712275)


--C. Find the display name of user who posted the oldest post on Stack Overflow (in terms of date).

--b = join B by id, C by id;
--c = order b by creation_date ASC
--d = limit c 1;
--e = foreach d generate displayname, creation_date;
--dump e; 

--Answer === (Joel Spolsky,2008-07-31 21:42:52)


--D. Find the display name and no. of comments done by the user who has got maximum reputation.

--b = join B by id, C by id;
--c = order b by reputation DESC;
--d = limit c 1;
--e = foreach d generate displayname, commentcount;
--dump e;

-- Answer == (Jon Skeet,1)


--E. Find the display name of user who has created maximum no. of posts on Stack Overflow. 

--b = join B by id, C by id;
--c = order b by score DESC;
--d = limit c 1;
--e = foreach d generate displayname;
--dump e;

-- Answer == (Josh Kodroff)


--F. Find the owner name and id of user whose post has got maximum no. of view counts so far.

--b = join B by id, C by id;
--c = order b by viewcount DESC;
--d = limit c 1;
--e = foreach d generate displayname, owneruser_id;
--dump e;

-- Answer == (lost,244)


--G. find the title and owner name of post who has got maximum no. of Comment count.

--b = join B by id, C by id;
--c = order b by commentcount DESC;
--d = limit c 1;
--e = foreach d generate displayname, title;
--dump e;

-- Answer ==(accreativos,)



--H. Find the location which has maximum no of Stack Overflow users.

--b = join B by id, C by id;
--c = group b by loc;
--e = foreach c generate group, COUNT(b.owneruser_id);
--f = order e by $01 DESC;
--g = limit f 1;
--dump g;


--I. Find the total no. of answers, posts, comments created by Indian users.

--b = join B by id, C by id;
--c = filter b by loc == 'India';
--d = group c all;
--e = foreach d generate SUM(c.answercount), SUM(c.commentcount), SUM(c.score);
--dump e;

-- Answer == (110,51,674)

