package mapreduce

import (
	"io/ioutil"
	"log"
	"strings"
	"sort"
	"os"
	"encoding/json"
	//"fmt"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	dic := make(map[string][]string)
	//reduceDic := make(map[string]string)
	for i:= 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		contents, err := ioutil.ReadFile(fileName)
		if err != nil {
			log.Fatal(err)
		}
		stringContents := string(contents)
		contentsList := strings.Split(stringContents,"\n")
		for _, each_contents := range contentsList {
			if len(each_contents) == 0 {
				break
			}
			line := strings.Split(each_contents, " ")
			dic[line[0]] = append(dic[line[0]], line[1])
		}
	}
	var keys []string
	for key, _ := range dic {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	//fmt.Println(dic)

	//for _, key := range keys {
	//	reduceDic[key] = reduceF(key, dic[key])
	//}

	writeFile, err := os.Create(outFile)
	if err != nil {
		//writeFile, _ = os.Create(outFile)
		//fmt.Println("Failed to open")
		log.Fatal(err)
	}

	enc := json.NewEncoder(writeFile)
	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, dic[key])})
	}

  writeFile.Close()
}
