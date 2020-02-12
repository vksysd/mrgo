package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

func main() {
	// fp, _ := os.Create("FileA")
	kva := []KeyValue{}
	// kva = append(kva, KeyValue{Key: "A", Value: "Apple"})
	// kva = append(kva, KeyValue{Key: "B", Value: "Ball"})
	// kva = append(kva, KeyValue{Key: "C", Value: "Cat"})
	// kva = append(kva, KeyValue{Key: "D", Value: "Dog"})
	// enc := json.NewEncoder(fp)
	// for _, kv := range kva {
	// 	enc.Encode(&kv)
	// }
	// fp.Close()

	fp2, err := os.OpenFile("FileA", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Println("Some Error!")
	}
	kva = append(kva, KeyValue{Key: "E", Value: "Elephant"})
	kva = append(kva, KeyValue{Key: "F", Value: "Fox"})
	enc1 := json.NewEncoder(fp2)
	for _, kv := range kva {
		err = enc1.Encode(&kv)
		if err != nil {
			fmt.Println("Error!")
			fmt.Println(err)
		}
	}

	fp2.Close()
}
