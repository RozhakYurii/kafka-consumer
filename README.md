# kafka-consumer-producer

To run this application you need to install zookeeper and kafka.

#### Here is a link where you can read about installing ang configuring kafka and zookeeper on Windows OS:
> https://dzone.com/articles/running-apache-kafka-on-windows-os

### This application uses 2 topics on kafka server: 
- test - topic where regular strings are stored.
- testJson - topic to store instances of Message class.

### To publish messages to kafka server there are 3 acceptable HTTP requests:
- POST on http://localhost:8080/message/publishAsJson with Message instance representation in request body it will be published onto "testJson" topic.
- GET on  http://localhost:8080/message/publish/{message} where message is regular string which will be published onto "test" topic.
- GET on  http://localhost:8080/message/publishAsJson/{message} where message - regular string which will be passed as text field to new Message instance to be published onto "testJson" topic.


