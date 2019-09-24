# CS451 Assignment 1  
## Daiyang Wang(d268wang)  

### Question 1  
For pairs implementation:   
I used two map reduce job. I first use orginal file as input to map to emit the single line and each words as one. Then I used reduce to calculate the total number of lines and how many times each word appeared     
([\*,\*] num] as total number of lines, ([X,*], num) as how many time it appered.     
Then I saved it as side data into temp files. Then use second map with orginal file as input to calculate the pairs ([x,y] 1), then use reduce to load the side data, and calculate the final pmi value.     
For stripes implementatnion:  



### Question 2  
The application id is application_*1566180955559_0506*.  

### Question 3  
For the word cout job, there are 14 mapper runing in parallel.  

### Question 4  
Using threshold 10, I am able to extract *77198* pairs of distinct PMI.   

### Question 5
| X   | Frequency |
|:-----:|:------------:|
| game | 369            |
| for | 251             |        
| day | 187             |
| and | 176             |
| score | 151           |
| world | 137           |
| record | 133          |
| strangers | 123       |
| the | 116             |
| season | 113          |
