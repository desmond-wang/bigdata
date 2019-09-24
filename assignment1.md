# CS451 Assignment 1  
## Daiyang Wang(d268wang)  

### Question 1  
#### For pairs implementation:   
I used two map reduce job. I first use orginal file as input to map to emit the single line and each words as one. Then I used reduce to calculate the total number of lines and how many times each word appeared     
([\*,\*] num] as total number of lines, ([X,\*], num) as how many time it appered.     
Then I saved it as side data into temp files.   
Then use second map with orginal file as input to calculate the pairs ([x,y] 1), then use reduce to load the side data, and calculate the final PMI value *log(p(co-occur)/[p(x)p(y)]*.     
#### For stripes implementatnion:  
I used two map reduce job. I first use orginal file as input to map to emit the single line and each words as one. Then I used reduce to calculate the total number of lines and how many times each word appeared     
([\*,\*] num] as total number of lines, ([X,\*], num) as how many time it appered.     
Then I saved it as side data into temp files.   
Then use second map with original file as input to calculate the strips (x,{(y,1),(z,2),...}, then use reduce to load side data, and calculate the final PMI value *log(p(co-occur)/[p(x)p(y)]*.

### Question 2  
Under the student environment, the pair runtime is job1 **4.906**, job2 **21.488** second with 5 reducer and threshold of 10.  
the Stripes runtime is job1 **5.881**, job2 **11.463** second with 5 reducer and threshold of 10.
Student encironment: linux.student.cs.uwaterloo.ca

### Question 3  
Under the student environment and **without** combiner, the pair runtime is job1 **5.961**, job2 **25.561** second with 5 reducer and threshold of 10.  
the Stripes runtime is job1 **7.076**, job2 **13.504** second with 5 reducer and threshold of 10.
Student encironment: linux.student.cs.uwaterloo.ca

### Question 4  
Using threshold 10, I am able to extract *77198* pairs of distinct PMI.   

### Question 5   

#### Highest PMI    
(maine, anjou) 3.6331422, (anjou, maine) 3.6331422.
#### Lowerst PMI   
(you, thy) -1.5303967, (thy, you) -1.5303967

"maine" and "anjou" are most likely occure in the same line, maybe is is the name of main character.
'you' and 'thy' are lowest likely occure in the same line, since they are bascilly the same meaning.

### Question 6
**tears**  
(tears, shed) 2.1117902 (tears, salt) 2.0528123 (tears, eyes) 1.165167  
**death**
(death, father's) 1.120252 (death, die) 0.75415933 (death, life) 0.7381346  

### Question 7
(hockey, defenceman) 2.4180872 co-occur 153 times.  
(hockey, winger) 2.4700917 co-occur 188 times.
(hockey, sledge) 2.352185 co-occur 93 times.   
(hockey, goaltender) 2.2537384 co-occur 199 times.   
(hockey, ice) 2.2093477 co-occur 2160 times.

### Question 8
(data, cooling) 2.0979042 co-occur 74 times.  
(data, encryption) 2.0443723 co-occur 53 times.   
(data, array) 1.9926307 co-occur 50 times.
(data, storage) 1.9878386 co-occur 110 times.   
(data, database) 1.8893089 co-occur 99 times. 


