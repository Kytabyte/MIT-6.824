package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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

	var inFile string
	var fp *os.File
	var dec *json.Decoder
	var err error
	var kv KeyValue

	kvmap := make(map[string][]string)

	// Read intermediate file and store in kvs
	for i := 0; i < nMap; i++ {
		inFile = reduceName(jobName, i, reduceTaskNumber)
		fp, err = os.Open(inFile)
		checkErr(err)
		dec = json.NewDecoder(fp)
		for {
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
		fp.Close()
	}

	// sort kvs by key
	var keys []string
	for key := range kvmap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// call reduceF() and write to file
	fp, err = os.Create(outFile)
	checkErr(err)
	enc := json.NewEncoder(fp)
	for _, key := range keys {
		err = enc.Encode(KeyValue{key, reduceF(key, kvmap[key])})
		checkErr(err)
	}
	fp.Close()
}
