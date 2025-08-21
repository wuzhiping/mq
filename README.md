- docker build -t shawoo/mq:slim .
- docker run --rm -it -p 8888:8888 -v $(pwd)/data/:/data/ shawoo/mq:slim
```code
docker run --name=mq -it -d \            
        -p 15009:8888 -p 36379:6379 \
        -p 9800:8000 \
        -p 18233:8233 -p 17233:7233 \
        -e webhook="" \
        -v $PWD/MQ:/src/MQ shawoo/mq:temporalio
```
