package brigades

import (
	"log"
)

var lastPrintOverwritable bool = false

func logPrint(s string, overwritable bool) {
	if lastPrintOverwritable {
		log.SetPrefix("\033[1A\033[K")
	}
	log.Println(s)
	log.SetPrefix("")
	lastPrintOverwritable = overwritable
}

func logPrintf(format string, overwritable bool, v ...interface{}) {
	if lastPrintOverwritable {
		log.SetPrefix("\033[1A\033[K")
	}
	log.Printf(format+"\n", v...)
	log.SetPrefix("")
	lastPrintOverwritable = overwritable
}
