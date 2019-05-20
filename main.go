package main

import (
    "fmt"
    "github.com/garyburd/redigo/redis"
    "strconv"
    "sync"
    "time"
)

//type user struct {
//    userId           int
//    hashedUserId     int
//    additionalRate   string
//}
//
//type userShop struct {
//    summaryOfClientUserPaymentShopId1 string
//    summaryOfClientUserPaymentShopId2 string
//    summaryOfClientUserPaymentShopId3 string
//    summaryOfClientUserPaymentShopId4 string
//    summaryOfClientUserPaymentShopId5 string
//}
//
//
//type shop struct{
//    shopId  int
//    name     string
//    url      string
//}
//
//type moleShop struct{
//    mallId        int
//    mallShopId    int
//    shopId        int
//    endpoint      string
//    rate          int
//}
//
//type shopLabel struct{
//    shopLabeltypeName1  string
//    shopLabeltypeName2  string
//    shopLabeltypeName3  string
//    shopLabeltypeName4  string
//    shopLabeltypeName5  string
//}


func InitSet(c redis.Conn,wg *sync.WaitGroup,ch chan int) {
    defer wg.Done()
    var num int = 0
    for {
        i, ok := <-ch
        if ok == false {
            fmt.Println("finish")
            break
        }

        if  i % 1000 == 0 && i != 0 {
            num++
            fmt.Printf("終了 %v \n",num)
        }

        for k := 0; k < 1000; k++ {
           c.Send("hmset", "user_"+strconv.Itoa(i)+"_"+strconv.Itoa(k), "user_id", strconv.Itoa(i),
               "hashed_user_id", strconv.Itoa(i), "additional_rate", strconv.Itoa(i))
           for j := 0; j < 5; j++ {
               c.Send("rpush", "user_shop_"+strconv.Itoa(i)+strconv.Itoa(k), strconv.Itoa(j))
           }
           c.Send("hmset", "shop_"+strconv.Itoa(i)+strconv.Itoa(k), "shop_id", strconv.Itoa(i),
               "name", strconv.Itoa(i), "url", strconv.Itoa(i))
           c.Send("hmset", "mall_shop_"+strconv.Itoa(i)+"_"+strconv.Itoa(k), "mall_id", strconv.Itoa(i),
               "mall_shop_id", strconv.Itoa(i), "shop_id", strconv.Itoa(i), "endpoint ", strconv.Itoa(i),
               "rate", strconv.Itoa(i))
           for j := 0; j < 5; j++ {
               c.Send("rpush", "shop_label_"+strconv.Itoa(i)+strconv.Itoa(k), strconv.Itoa(j))
           }
        }
        c.Flush()
    }
}


func redis_connection() redis.Conn {
    const IP_PORT = "laboon-redis-test.htbkgr.ng.0001.apne1.cache.amazonaws.com:6379"
    //redisに接続
    c, err := redis.Dial("tcp", IP_PORT)
    if err != nil {
        panic(err)
    }
    return c
}

func flashDB(dbindex int, c redis.Conn){
    c.Do("select",dbindex)
    c.Do("flushdb")

}

func main() {
    c := redis_connection()
    flashDB(0,c)
    start := time.Now()
    defer c.Close()
    defer flashDB(0,c)

    c.Do("SELECT",0)
    var wg sync.WaitGroup

    ch := make(chan int ,50)
    wg.Add(1)
    go InitSet(c,&wg,ch)

    fmt.Println("success")
    for i :=0; i < 10000; i++{
        ch <- i
    }

    close(ch)
    wg.Wait()
    end := time.Now()
    initSetTime := end.Sub(start).Seconds()
    fmt.Println(initSetTime)
}

