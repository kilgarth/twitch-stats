package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/config"
)

var conf *config.Config
var configFile = flag.String("c", "twitch_stats.conf", "specify config file")
var streamChannel string
var authToken string
var dsn string

type SubData struct {
	Total int `json:"_total"`
}

type FollowData struct {
	Total int `json:"_total"`
}

type StreamData struct {
	Game    string `json:"game,omitempty"`
	Channel struct {
		Status    string `json:"status"`
		Followers int    `json:"followers"`
	} `json:"channel"`
	StreamID int `json:"_id"`
	Viewers  int `json:"viewers"`
}

type TwitchData struct {
	Stream             StreamData `json:"stream"`
	InitialFollowers   int
	InitialSubscribers int
	FinalFollowers     int
	FinalSubscribers   int
	StartTime          time.Time
	EndTime            time.Time
	ViewersAggregate   map[string]int
	Started            bool
	Misses             int
	MaxViewers         int
}

func main() {
	flag.Parse()

	conf, err := config.ReadDefault(*configFile)
	if err != nil {
		conf = config.NewDefault()
		fmt.Printf("Error loading config file")
	}

	dsn, err = conf.String("DEFAULT", "DSN")
	if err != nil {
		log.Printf("Error reading DSN: %s", err)
	}

	log.SetFlags(log.Ldate | log.Ltime)
	logDir, _ := conf.String("DEFAULT", "log_dir")
	if logDir == "" {
		logDir = "."
	}

	streamChannel, _ = conf.String("DEFAULT", "StreamChannel")
	runTest, _ := conf.Bool("DEFAULT", "TestMode")
	authToken, _ = conf.String("DEFAULT", "AuthToken")
	monitorInterval, _ := conf.Int("DEFAULT", "MonitorInterval")
	monInt := time.Duration(monitorInterval)

	filePrefix := "twitch_stats-"
	fnTime := time.Now().UTC().Format("200601")

	logFile := fmt.Sprintf("%s/%s%s.log", logDir, filePrefix, fnTime)
	fp, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_SYNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file '%s': %s", logFile, err)
	}

	log.SetOutput(fp)

	log.Printf("Starting monitoring for '%s' with an interval of '%d' seconds.", streamChannel, monitorInterval)

	initDB()

	if runTest == true {
		test()
	} else {
		run(monInt)
	}
}

func run(i time.Duration) {
	tick := time.NewTicker(time.Second * i).C

	data := &TwitchData{}

	for {
		select {
		case <-tick:
			data = checkStatus(data)
		}
	}
}

func initDB() {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Database error: %s", err)
	}

	sql := `CREATE TABLE IF NOT EXISTS twitch_streams
	(
		id bigint not null primary key,
		status text,
		starttime timestamp DEFAULT '0000-00-00 00:00:00',
		endtime timestamp DEFAULT '0000-00-00 00:00:00',
		initialfollow int,
		initialsub int,
		endfollow int,
		endsub int,
		avgviewers int,
		maxviewers int,
		enterdate timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`

	_, err = db.Exec(sql)
	if err != nil {
		log.Printf("Error creating table: %s", err)
	}
}

func storeStream(id int, status string, startTime time.Time, endTime time.Time, initFollow int, initSub int, endFollow int, endSub int, avgView int, maxViewers int) {
	log.Printf("Inserting data: %d, %s, %s, %s, %d, %d, %d, %d, %d, %d, %s\n", id, status, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), initFollow, initSub, endFollow, endSub, avgView, maxViewers, time.Now().UTC().Format(time.RFC3339))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Database error: %s", err)
	}

	sql := `REPLACE INTO twitch_streams (id,status,starttime,endtime,initialfollow,initialsub,endfollow,endsub,avgviewers,maxviewers,enterdate) VALUES (?,?,?,?,?,?,?,?,?,?,?)`
	stmt, _ := db.Prepare(sql)
	res, err := stmt.Exec(id, status, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), initFollow, initSub, endFollow, endSub, avgView, maxViewers, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		log.Printf("Database insert error: %s", err)
	}

	rowsAff, _ := res.RowsAffected()
	if rowsAff != 1 {
		log.Printf("Rows affected was not 1, returned : %d", rowsAff)
	}

}

func test() {
	data := &TwitchData{}
	fmt.Println(getSubs())
	fmt.Println(getFollows())
	fmt.Println(checkStatus(data))
}

func checkStatus(i *TwitchData) *TwitchData {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitch.tv/kraken/streams/%s", streamChannel), nil)
	req.Header.Set("Accept", "application/vnd.twitchtv.v3+json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Printf("Error getting twitch API data: %s", err)
		return i
	}

	if res.StatusCode != 200 {
		log.Printf("Received non-200 response from twitch. %d", res.StatusCode)
		res.Body.Close()
		return i
	}

	d := json.NewDecoder(res.Body)
	decoded := &TwitchData{}
	err = d.Decode(&decoded)

	res.Body.Close()

	if err != nil {
		log.Printf("Decode error: %s", err)
		return i
	}

	if !i.Started && len(decoded.Stream.Channel.Status) > 0 {
		subs := getSubs()
		i.Stream.StreamID = decoded.Stream.StreamID
		i.Stream.Channel.Status = decoded.Stream.Channel.Status
		i.InitialSubscribers = subs
		i.InitialFollowers = decoded.Stream.Channel.Followers
		i.StartTime = time.Now().UTC()
		i.Started = true
		if i.ViewersAggregate == nil {
			i.ViewersAggregate = make(map[string]int)
		}
		i.ViewersAggregate[fmt.Sprint(time.Now())] = decoded.Stream.Viewers

		log.Printf("New stream detected. Title: %s, Current Viewers: %d \n", decoded.Stream.Channel.Status, decoded.Stream.Viewers)
		return i
	}

	if len(decoded.Stream.Channel.Status) <= 0 && i.Started {
		if i.Misses <= 2 {
			i.Misses = i.Misses + 1
			return i
		}
		subs := getSubs()
		finalFollow := getFollows()
		i.EndTime = time.Now().UTC()
		i.FinalFollowers = finalFollow
		i.FinalSubscribers = subs

		avgViewers := 0
		maxUsers := 0

		if len(i.ViewersAggregate) > 0 {
			v := 0
			for _, i := range i.ViewersAggregate {
				v = v + i
				if i > maxUsers {
					maxUsers = i
				}
			}
			numVStat := len(i.ViewersAggregate)
			avgViewers = v / numVStat
		}

		dFol := i.FinalFollowers - i.InitialFollowers
		dSub := i.FinalSubscribers - i.InitialSubscribers

		status := i.Stream.Channel.Status
		streamID := i.Stream.StreamID
		startTime := i.StartTime
		endTime := i.EndTime
		initFollow := i.InitialFollowers
		initSub := i.InitialSubscribers

		log.Printf("Detected stream end. Title: '%s' Started at: %s, Ended at %s. Average Viewers: %d (Max: %d) | Delta Followers: %d, Delta Subs: %d \n", status, startTime, endTime, avgViewers, maxUsers, dFol, dSub)

		storeStream(streamID, status, startTime, endTime, initFollow, initSub, finalFollow, subs, avgViewers, maxUsers)

		i = &TwitchData{}

		return i
	}

	if len(decoded.Stream.Channel.Status) > 0 && i.Started && decoded.Stream.Channel.Status == i.Stream.Channel.Status {
		if i.Misses > 0 {
			i.Misses = 0
		}
		i.Stream.Channel.Status = decoded.Stream.Channel.Status
		i.ViewersAggregate[fmt.Sprint(time.Now())] = decoded.Stream.Viewers
	} else if len(decoded.Stream.Channel.Status) > 0 && i.Started && (decoded.Stream.Channel.Status != i.Stream.Channel.Status) {
		//Stream ended, new one began.
		subs := getSubs()
		finalFollow := getFollows()
		i.EndTime = time.Now().UTC()
		i.FinalFollowers = finalFollow
		i.FinalSubscribers = subs

		avgViewers := 0
		maxUsers := 0

		if len(i.ViewersAggregate) > 0 {
			v := 0
			for _, i := range i.ViewersAggregate {
				v = v + i
				if i > maxUsers {
					maxUsers = i
				}
			}
			numVStat := len(i.ViewersAggregate)
			avgViewers = v / numVStat
		}

		dFol := i.FinalFollowers - i.InitialFollowers
		dSub := i.FinalSubscribers - i.InitialSubscribers
		status := i.Stream.Channel.Status
		streamID := i.Stream.StreamID
		startTime := i.StartTime
		endTime := i.EndTime
		initFollow := i.InitialFollowers
		initSub := i.InitialSubscribers

		log.Printf("Detected stream end. Title: '%s' Started at: %s, Ended at %s. Average Viewers: %d (Max: %d) | Delta Followers: %d, Delta Subs: %d \n", status, startTime, endTime, avgViewers, maxUsers, dFol, dSub)

		storeStream(streamID, status, startTime, endTime, initFollow, initSub, finalFollow, subs, avgViewers, maxUsers)

		i = &TwitchData{}

		return i
	}

	return i

}

func getSubs() int {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitch.tv/kraken/channels/%s/subscriptions", streamChannel), nil)
	req.Header.Set("Authorization", fmt.Sprintf("OAuth %s", authToken))
	req.Header.Set("Accept", "application/vnd.twitchtv.v3+json")

	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		log.Printf("Error getting subs: %s", err)
	}

	decoded := &SubData{}
	err = json.NewDecoder(res.Body).Decode(&decoded)

	if err != nil {
		log.Printf("Error decoding subs data: %s", err)
	}

	if res.StatusCode != 200 {
		log.Printf("Subs Error, API returned non 200 response: %d", res.StatusCode)
	}

	res.Body.Close()

	return decoded.Total
}

func getFollows() int {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitch.tv/kraken/channels/%s/follows", streamChannel), nil)
	req.Header.Set("Accept", "application/vnd.twitchtv.v3+json")

	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		log.Printf("Error getting follows: %s", err)
	}

	decoded := &FollowData{}
	err = json.NewDecoder(res.Body).Decode(&decoded)

	if err != nil {
		log.Printf("Error decoding followers data: %s", err)
	}

	if res.StatusCode != 200 {
		log.Printf("Followers Error, API returned non 200 response: %d", res.StatusCode)
	}

	res.Body.Close()

	return decoded.Total
}
