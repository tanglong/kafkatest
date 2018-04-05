package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func main() {

	arg := os.Args[1]

	files, err := ioutil.ReadDir(arg)
	if err != nil {
		log.Fatal(err)
	}

	fileW, err := os.Create("../filew")
	if err != nil {
		fmt.Printf("create filew fail!\n")
		return
	}
	defer fileW.Close()
	w := bufio.NewWriter(fileW)

	for _, fileinfo := range files {
		file, err := os.Open(arg + "/" + fileinfo.Name())
		reader := bufio.NewReader(file)
		var line string
		for {
			line, err = reader.ReadString('\n')
			line = strings.TrimSuffix(line, "\n")
			line = strings.TrimSuffix(line, "\r")
			line = strings.TrimSpace(line)

			if len(line) != 0 {
				fmt.Fprintln(w, line)
			}
			if err != nil {
				break
			}

		}
	}
}
